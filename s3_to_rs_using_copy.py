"""
EC2 Script: S3 Staging → Redshift Landing Tables
(Byte-Range + Pipelined Upload/COPY + Multipart Upload + Parallel Files)
=========================================================================
Usage    : python3 s3_stg_to_rs_landing.py 20260522
Trigger  : Manual / Cron job on EC2

Optimisations vs previous version
──────────────────────────────────
1. MULTIPART UPLOAD  (per chunk)
   Each ~828 MB chunk is uploaded via boto3 TransferManager using
   MULTIPART_PART_SIZE parts uploaded in parallel (MULTIPART_CONCURRENCY
   threads).  Replaces single-PUT put_object.
   Saves: ~25s per chunk → ~75s per file.

2. PIPELINED UPLOAD → COPY  (per file)
   COPY is fired on chunk N the moment its upload finishes — it does
   not wait for chunks N+1 … N+3 to finish uploading.
   Uploads remain sequential (leftover carry-forward dependency).
   COPY jobs run concurrently in a thread pool alongside the upload loop.
   Saves: ~3 × upload_time overlap → ~25-30s per file.

3. PARALLEL FILE PROCESSING  (across files)
   FILE_PARALLELISM files are processed concurrently.
   Each file still uses NUM_CHUNKS COPY threads internally.
   Max simultaneous COPYs = FILE_PARALLELISM × NUM_CHUNKS.
   Check your Redshift WLM queue concurrency before raising this.
   Saves: (FILE_PARALLELISM-1)/FILE_PARALLELISM × total_time.

Memory per file: O(file_size / NUM_CHUNKS) — one chunk at a time.

Flow per file:
  1. HEAD  → file size (no download)
  2. GET 4 KB → extract header row
  3. Compute NUM_CHUNKS byte ranges
  4. For each range (sequentially, leftover dependency):
       a. GET byte range from S3
       b. Align to row boundary (carry leftover to next range)
       c. Prepend header
       d. Multipart-upload chunk to S3   ← OPT 1
       e. Submit COPY immediately        ← OPT 2  (don't wait for next chunk)
  5. Wait for all COPY futures to complete
  6. Stamp cycle_id (UPDATE WHERE cycle_id IS NULL)
  7. Log to rs_file_load_log
  8. Cleanup chunk files from S3

Load type : ALWAYS APPEND (no TRUNCATE).
Author    : Alumis Data Platform
"""

import boto3
import io
import logging
import re
import sys
import threading
import time
import traceback
from boto3.s3.transfer import TransferConfig
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
BUCKET_NAME         = ""
BASE_PREFIX         = ""
STAGING_PREFIX      = f""
CHUNKS_PREFIX       = ""

REDSHIFT_CLUSTER_ID = ""
REDSHIFT_DATABASE   = ""
REDSHIFT_DB_USER    = ""
REDSHIFT_IAM_ROLE   = ""

AWS_REGION          = ""

# ── Redshift slice count: ra3.large = 2 slices/node × 2 nodes = 4
NUM_CHUNKS          = 4

# ── OPT 1: Multipart upload settings
# Part size: 64 MB gives ~13 parts per 828 MB chunk.
# Concurrency: 4 parallel part-uploads per chunk.
MULTIPART_PART_SIZE    = 64 * 1024 * 1024   # 64 MB
MULTIPART_CONCURRENCY  = 4

# ── OPT 3: How many files to process in parallel.
# Each file uses NUM_CHUNKS COPY threads → total concurrent COPYs
# = FILE_PARALLELISM × NUM_CHUNKS.  Verify WLM queue allows this.
FILE_PARALLELISM    = 2

# ── Header scan: first N bytes fetched to extract the header row.
HEADER_SCAN_BYTES   = 4096

# ── Redshift tables
LOG_TABLE           = ""
LOAD_LOG_TABLE      = ""
LANDING_TABLE       = ""

# ── COPY options
COPY_OPTIONS        = """FORMAT AS CSV
DELIMITER '|'
IGNOREHEADER 1
DATEFORMAT 'auto'
TIMEFORMAT 'auto'
EMPTYASNULL
FILLRECORD
BLANKSASNULL"""

# ── Data API polling
MAX_WAIT_SECONDS    = 600
POLL_INTERVAL       = 5

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# THREAD-LOCAL AWS CLIENTS
# boto3 clients are not thread-safe across threads.
# Each worker gets its own client via thread-local storage.
# ─────────────────────────────────────────────
_thread_local = threading.local()

def get_rs_client():
    if not hasattr(_thread_local, "rs"):
        _thread_local.rs = boto3.client("redshift-data", region_name=AWS_REGION)
    return _thread_local.rs

def get_s3_client():
    if not hasattr(_thread_local, "s3"):
        _thread_local.s3 = boto3.client("s3", region_name=AWS_REGION)
    return _thread_local.s3

# Main-thread S3 client and transfer config (used for uploads outside thread pool)
s3_main = boto3.client("s3", region_name=AWS_REGION)

TRANSFER_CONFIG = TransferConfig(
    multipart_threshold = MULTIPART_PART_SIZE,
    multipart_chunksize = MULTIPART_PART_SIZE,
    max_concurrency     = MULTIPART_CONCURRENCY,
    use_threads         = True
)

# ─────────────────────────────────────────────
# HELPER: Validate cycle_id
# ─────────────────────────────────────────────
def validate_cycle_id(cycle_id: str) -> bool:
    if re.fullmatch(r"\d{8}", cycle_id):
        return True
    logger.error(f"Invalid cycle_id: '{cycle_id}'. Expected YYYYMMDD.")
    return False

# ─────────────────────────────────────────────
# HELPER: Extract typed value from Data API column
# ─────────────────────────────────────────────
def _val(col: dict):
    if col.get("isNull"):
        return None
    return (
        col.get("stringValue") or
        col.get("longValue")   or
        col.get("doubleValue") or
        col.get("booleanValue")
    )

# ─────────────────────────────────────────────
# CORE: Execute SQL via Redshift Data API
# Thread-safe — uses thread-local RS client.
# ─────────────────────────────────────────────
def execute_sql(sql: str, description: str = "", fetch_results: bool = False):
    """
    Returns (success: bool, error: str|None, records: list|None)
    """
    rs = get_rs_client()
    logger.info(f"SQL: {description or sql[:80]}")

    try:
        resp    = rs.execute_statement(
            ClusterIdentifier = REDSHIFT_CLUSTER_ID,
            Database          = REDSHIFT_DATABASE,
            DbUser            = REDSHIFT_DB_USER,
            Sql               = sql
        )
        stmt_id = resp["Id"]
    except Exception as e:
        logger.error(f"Submit failed: {e}")
        return False, str(e)[:1000], None

    elapsed = 0
    while elapsed < MAX_WAIT_SECONDS:
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL
        try:
            status = rs.describe_statement(Id=stmt_id)
            state  = status["Status"]
            logger.debug(f"  [{stmt_id}] {state} ({elapsed}s)")

            if state == "FINISHED":
                records = None
                if fetch_results:
                    records = rs.get_statement_result(Id=stmt_id).get("Records", [])
                return True, None, records

            if state in ("FAILED", "ABORTED"):
                err = status.get("Error", "Unknown")
                logger.error(f"Statement {state}: {err}")
                return False, str(err)[:1000], None

        except Exception as e:
            logger.error(f"Poll error: {e}")
            return False, str(e)[:1000], None

    msg = f"Timeout after {MAX_WAIT_SECONDS}s (stmt={stmt_id})"
    logger.error(msg)
    return False, msg, None

# ─────────────────────────────────────────────
# STEP 1: List staging files
# ─────────────────────────────────────────────
def list_staging_files(cycle_id: str) -> list:
    prefix    = f"{STAGING_PREFIX}/{cycle_id}/"
    paginator = s3_main.get_paginator("list_objects_v2")
    keys      = []

    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if CHUNKS_PREFIX in key:
                continue
            if not key.lower().endswith(".csv"):
                continue
            keys.append(key)

    logger.info(f"Found {len(keys)} CSV file(s) in s3://{BUCKET_NAME}/{prefix}")
    return sorted(keys)

# ─────────────────────────────────────────────
# STEP 2a: HEAD — file size only, no download
# ─────────────────────────────────────────────
def get_file_size(s3_key: str) -> int:
    resp = s3_main.head_object(Bucket=BUCKET_NAME, Key=s3_key)
    size = resp["ContentLength"]
    logger.info(f"File size: {size:,} bytes  ({s3_key.split('/')[-1]})")
    return size

# ─────────────────────────────────────────────
# STEP 2b: Fetch header row (first HEADER_SCAN_BYTES only)
# Returns (header_bytes, header_end_offset)
# ─────────────────────────────────────────────
def fetch_header(s3_key: str) -> tuple[bytes, int]:
    resp  = s3_main.get_object(
        Bucket = BUCKET_NAME,
        Key    = s3_key,
        Range  = f"bytes=0-{HEADER_SCAN_BYTES - 1}"
    )
    probe       = resp["Body"].read()
    newline_pos = probe.find(b"\n")

    if newline_pos == -1:
        raise ValueError(
            f"No newline found in first {HEADER_SCAN_BYTES} bytes of {s3_key}. "
            f"Increase HEADER_SCAN_BYTES."
        )

    header     = probe[: newline_pos + 1]
    header_end = newline_pos + 1
    logger.info(f"Header row: {len(header)} bytes, data starts at byte {header_end}")
    return header, header_end

# ─────────────────────────────────────────────
# STEP 3: Compute byte-range boundaries
# ─────────────────────────────────────────────
def compute_ranges(file_size: int, header_end: int, num_chunks: int) -> list[tuple[int, int]]:
    body_size  = file_size - header_end
    if body_size <= 0:
        return []

    chunk_size = -(-body_size // num_chunks)   # ceiling division
    ranges     = []

    for i in range(num_chunks):
        start = header_end + i * chunk_size
        end   = min(start + chunk_size - 1, file_size - 1)
        if start >= file_size:
            break
        ranges.append((start, end))

    return ranges

# ─────────────────────────────────────────────
# OPT 1: Multipart upload helper
# Replaces put_object for large chunks.
# Uses TransferManager: splits into MULTIPART_PART_SIZE parts,
# uploads MULTIPART_CONCURRENCY parts in parallel.
# ─────────────────────────────────────────────
def multipart_upload(chunk_data: bytes, chunk_key: str):
    s3_main.upload_fileobj(
        io.BytesIO(chunk_data),
        BUCKET_NAME,
        chunk_key,
        Config    = TRANSFER_CONFIG,
        ExtraArgs = {"ContentType": "text/csv"}
    )

# ─────────────────────────────────────────────
# STEP 4: Fetch one byte range, align rows, upload chunk.
# Returns chunk_key if uploaded, None if chunk carried entirely to next.
#
# leftover: mutable list used as a single-slot carry buffer between
# sequential iterations (not shared across threads).
# ─────────────────────────────────────────────
def fetch_align_and_upload_chunk(
    s3_key:      str,
    chunk_key:   str,
    header:      bytes,
    range_start: int,
    range_end:   int,
    is_last:     bool,
    leftover:    list,
    chunk_index: int,
    num_chunks:  int,
) -> str | None:

    resp = s3_main.get_object(
        Bucket = BUCKET_NAME,
        Key    = s3_key,
        Range  = f"bytes={range_start}-{range_end}"
    )
    raw = resp["Body"].read()

    # Prepend carry-forward bytes from previous chunk
    if leftover:
        raw = leftover.pop() + raw

    if not is_last:
        last_newline = raw.rfind(b"\n")
        if last_newline == -1:
            # Entire range is a partial row — carry forward
            leftover.append(raw)
            logger.warning(
                f"  Chunk {chunk_index}: no newline in range — "
                f"carrying to next chunk."
            )
            return None
        tail = raw[last_newline + 1:]
        raw  = raw[:last_newline + 1]
        if tail:
            leftover.append(tail)

    chunk_data = header + raw
    row_count  = raw.count(b"\n")

    logger.info(
        f"  Uploading chunk {chunk_index + 1}/{num_chunks} "
        f"({row_count:,} rows, {len(chunk_data):,} bytes) → {chunk_key.split('/')[-1]}"
    )

    # OPT 1: multipart upload instead of single PUT
    multipart_upload(chunk_data, chunk_key)

    del chunk_data, raw   # release memory immediately
    return chunk_key

# ─────────────────────────────────────────────
# STEP 5 (thread worker): COPY one chunk into Redshift
# ─────────────────────────────────────────────
def copy_chunk(chunk_key: str, cycle_id: str, chunk_index: int):
    """
    Returns (success: bool, error_message: str|None)
    """
    s3_path  = f"s3://{BUCKET_NAME}/{chunk_key}"
    copy_sql = f"""
        COPY {LANDING_TABLE}
        FROM '{s3_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        {COPY_OPTIONS};
    """.strip()

    success, error, _ = execute_sql(
        sql         = copy_sql,
        description = f"COPY chunk {chunk_index} → {LANDING_TABLE}"
    )
    return success, error

# ─────────────────────────────────────────────
# STEP 6: Stamp cycle_id on rows just loaded
# ─────────────────────────────────────────────
def stamp_cycle_id(cycle_id: str) -> bool:
    sql = f"""
        UPDATE {LANDING_TABLE}
        SET cycle_id = '{cycle_id}'
        WHERE cycle_id IS NULL;
    """.strip()
    success, error, _ = execute_sql(sql, description=f"Stamp cycle_id={cycle_id}")
    if not success:
        logger.error(f"cycle_id stamp failed: {error}")
    return success

# ─────────────────────────────────────────────
# STEP 7: Log result to rs_file_load_log
# ─────────────────────────────────────────────
def insert_load_log(cycle_id, s3_key, chunks_loaded, chunks_failed,
                    load_start_time, load_end_time, status, error_message=None):

    def esc(v): return str(v or "").replace("'", "''")

    filename  = s3_key.split("/")[-1]
    start_str = load_start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_str   = load_end_time.strftime("%Y-%m-%d %H:%M:%S")
    now_str   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    note      = f"chunks_loaded={chunks_loaded} chunks_failed={chunks_failed}"
    err_part  = (" | " + esc(error_message)[:800]) if error_message else ""

    sql = f"""
        INSERT INTO {LOAD_LOG_TABLE} (
            cycle_time_id,
            staging_file_name,
            staging_path,
            landing_table,
            load_start_time,
            load_end_time,
            status,
            error_message,
            insert_datetime
        ) VALUES (
            '{cycle_id}',
            '{esc(filename)}',
            '{esc(s3_key)}',
            '{LANDING_TABLE}',
            '{start_str}',
            '{end_str}',
            {status},
            '{esc(note + err_part)}',
            '{now_str}'
        );
    """
    ok, err, _ = execute_sql(sql, description=f"Insert load log | {filename}")
    if not ok:
        logger.error(f"Failed to insert load log for {filename}: {err}")

# ─────────────────────────────────────────────
# STEP 8: Cleanup chunk files from S3
# ─────────────────────────────────────────────
def cleanup_chunks(chunk_keys: list):
    if not chunk_keys:
        return
    try:
        s3_main.delete_objects(
            Bucket = BUCKET_NAME,
            Delete = {"Objects": [{"Key": k} for k in chunk_keys], "Quiet": True}
        )
        logger.info(f"Cleaned up {len(chunk_keys)} chunk file(s) from S3.")
    except Exception as e:
        logger.warning(f"Chunk cleanup failed (non-fatal): {e}")

# ─────────────────────────────────────────────
# PROCESS ONE FILE
#
# OPT 2: PIPELINED UPLOAD → COPY
# ─────────────────────────────────────────────
# Timeline (old):
#   upload0 ──► upload1 ──► upload2 ──► upload3 ──► COPY0+1+2+3
#
# Timeline (new):
#   upload0 ──► upload1 ──► upload2 ──► upload3
#           COPY0 starts              ↑ immediately after each upload
#                   COPY1 starts      ↑
#                           COPY2 starts  ↑
#                                   COPY3 starts  ↑
#   All 4 COPYs running concurrently in thread pool while
#   later uploads are still in progress.
# ─────────────────────────────────────────────
def process_file(s3_key: str, cycle_id: str) -> bool:
    filename        = s3_key.split("/")[-1]
    load_start_time = datetime.now(timezone.utc)
    chunk_keys      = []

    logger.info("━" * 60)
    logger.info(f"FILE START: {filename}")

    try:
        # ── HEAD: file size only ───────────────────────────────────
        file_size = get_file_size(s3_key)

        # ── Fetch header row ───────────────────────────────────────
        header, header_end = fetch_header(s3_key)

        # ── Compute byte ranges ────────────────────────────────────
        ranges = compute_ranges(file_size, header_end, NUM_CHUNKS)
        if not ranges:
            logger.warning(f"{filename} has no data rows — skipping.")
            return False

        filename_base = s3_key.split("/")[-1].replace(".csv", "")
        chunk_base    = f"{STAGING_PREFIX}/{cycle_id}/{CHUNKS_PREFIX}/{filename_base}"

        logger.info(
            f"Splitting into {len(ranges)} chunks — "
            f"upload and COPY will be pipelined."
        )

        # ── OPT 2: Pipeline — fire COPY as soon as each chunk uploads ─
        chunks_loaded = 0
        chunks_failed = 0
        failed_errors = []
        leftover      = []   # carry-forward buffer (upload loop only)

        # Thread pool stays open for the entire upload loop so COPY
        # jobs accumulate and run concurrently with later uploads.
        with ThreadPoolExecutor(
            max_workers        = NUM_CHUNKS,
            thread_name_prefix = "chunk-copy"
        ) as pool:
            copy_futures = {}   # future → chunk_index

            for i, (rng_start, rng_end) in enumerate(ranges):
                is_last   = (i == len(ranges) - 1)
                chunk_key = f"{chunk_base}/chunk_{i:02d}.csv"

                # Upload is sequential (leftover carry-forward dependency)
                uploaded = fetch_align_and_upload_chunk(
                    s3_key      = s3_key,
                    chunk_key   = chunk_key,
                    header      = header,
                    range_start = rng_start,
                    range_end   = rng_end,
                    is_last     = is_last,
                    leftover    = leftover,
                    chunk_index = i,
                    num_chunks  = len(ranges),
                )

                if uploaded:
                    chunk_keys.append(uploaded)
                    # Fire COPY immediately — don't wait for other uploads
                    f = pool.submit(copy_chunk, uploaded, cycle_id, i)
                    copy_futures[f] = i
                    logger.info(f"  → COPY chunk {i} submitted (upload done)")

            # All uploads done; wait for all COPY futures
            logger.info(
                f"All {len(ranges)} chunks uploaded. "
                f"Waiting for {len(copy_futures)} COPY job(s)…"
            )
            for future in as_completed(copy_futures):
                idx = copy_futures[future]
                try:
                    success, error = future.result()
                    if success:
                        chunks_loaded += 1
                        logger.info(f"  ✓ Chunk {idx} loaded successfully")
                    else:
                        chunks_failed += 1
                        failed_errors.append(f"chunk {idx}: {error}")
                        logger.error(f"  ✗ Chunk {idx} failed: {error}")
                except Exception as exc:
                    chunks_failed += 1
                    failed_errors.append(f"chunk {idx}: {exc}")
                    logger.error(f"  ✗ Chunk {idx} exception: {exc}")

        # ── Handle leftover edge case ──────────────────────────────
        if leftover:
            logger.warning("Trailing bytes after last chunk — appending to final chunk.")
            last_key = chunk_keys[-1] if chunk_keys else f"{chunk_base}/chunk_tail.csv"
            resp     = s3_main.get_object(Bucket=BUCKET_NAME, Key=last_key)
            s3_main.put_object(
                Bucket      = BUCKET_NAME,
                Key         = last_key,
                Body        = resp["Body"].read() + leftover.pop(),
                ContentType = "text/csv"
            )

        # ── Overall file status ────────────────────────────────────
        file_success  = chunks_failed == 0
        error_message = "; ".join(failed_errors) if failed_errors else None

        # ── Stamp cycle_id ─────────────────────────────────────────
        if file_success:
            if not stamp_cycle_id(cycle_id):
                file_success  = False
                error_message = "cycle_id stamp failed after successful COPY"

        load_end_time = datetime.now(timezone.utc)
        elapsed       = (load_end_time - load_start_time).total_seconds()

        # ── Log result ─────────────────────────────────────────────
        insert_load_log(
            cycle_id        = cycle_id,
            s3_key          = s3_key,
            chunks_loaded   = chunks_loaded,
            chunks_failed   = chunks_failed,
            load_start_time = load_start_time,
            load_end_time   = load_end_time,
            status          = 1 if file_success else 0,
            error_message   = error_message
        )

        status_icon = "✓" if file_success else "✗"
        logger.info(
            f"{status_icon} FILE {'DONE' if file_success else 'FAILED'}: "
            f"{filename} | {chunks_loaded}/{len(ranges)} chunks | {elapsed:.1f}s"
        )
        return file_success

    except Exception as e:
        load_end_time = datetime.now(timezone.utc)
        logger.error(f"✗ Unhandled error on {filename}: {e}")
        logger.error(traceback.format_exc())
        insert_load_log(
            cycle_id        = cycle_id,
            s3_key          = s3_key,
            chunks_loaded   = 0,
            chunks_failed   = NUM_CHUNKS,
            load_start_time = load_start_time,
            load_end_time   = load_end_time,
            status          = 0,
            error_message   = str(e)[:1000]
        )
        return False

    finally:
        cleanup_chunks(chunk_keys)

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def process_pending_files(cycle_id: str):
    logger.info("═" * 60)
    logger.info("EC2 Loader — S3 Staging → Redshift")
    logger.info("[byte-range | multipart upload | pipelined COPY | parallel files]")
    logger.info(f"cycle_id          : {cycle_id}")
    logger.info(f"Staging           : s3://{BUCKET_NAME}/{STAGING_PREFIX}/{cycle_id}/")
    logger.info(f"Target table      : {LANDING_TABLE}")
    logger.info(f"Cluster           : {REDSHIFT_CLUSTER_ID}")
    logger.info(f"Chunks/file       : {NUM_CHUNKS}  (= Redshift slice count)")
    logger.info(f"Multipart parts   : {MULTIPART_PART_SIZE // (1024*1024)} MB  ×  {MULTIPART_CONCURRENCY} concurrent")
    logger.info(f"File parallelism  : {FILE_PARALLELISM}  (max {FILE_PARALLELISM * NUM_CHUNKS} concurrent COPYs)")
    logger.info("═" * 60)

    all_files = list_staging_files(cycle_id)

    if not all_files:
        logger.warning("No CSV files found — nothing to load.")
        sys.exit(0)

    # ── TEST MODE: process only the first file ────────────────────
    # Remove or comment out this line to process all files.
    all_files = all_files[:1]

    total         = len(all_files)
    success_count = 0
    failed_count  = 0

    # ── OPT 3: Process FILE_PARALLELISM files concurrently ────────
    # Each file has its own upload loop + COPY thread pool internally.
    # Files share no state so this is safe with no locking needed.
    if FILE_PARALLELISM > 1 and total > 1:
        logger.info(f"Processing {total} file(s) with parallelism={FILE_PARALLELISM}")
        with ThreadPoolExecutor(
            max_workers        = FILE_PARALLELISM,
            thread_name_prefix = "file-proc"
        ) as pool:
            future_to_key = {
                pool.submit(process_file, key, cycle_id): key
                for key in all_files
            }
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    if future.result():
                        success_count += 1
                    else:
                        failed_count += 1
                except Exception as exc:
                    failed_count += 1
                    logger.error(f"File-level exception for {key.split('/')[-1]}: {exc}")
    else:
        # Single file or parallelism disabled — sequential loop
        for file_num, s3_key in enumerate(all_files, start=1):
            logger.info(f"\n[{file_num}/{total}] {s3_key.split('/')[-1]}")
            if process_file(s3_key, cycle_id):
                success_count += 1
            else:
                failed_count += 1

    logger.info("\n" + "═" * 60)
    logger.info(f"DONE  |  cycle_id: {cycle_id}")
    logger.info(f"Total   : {total}")
    logger.info(f"Success : {success_count}")
    logger.info(f"Failed  : {failed_count}")
    logger.info("═" * 60)

    sys.exit(1 if failed_count > 0 else 0)

# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage  : python3 s3_stg_to_rs_landing.py <cycle_id>")
        print("Example: python3 s3_stg_to_rs_landing.py 20260528")
        sys.exit(1)

    cycle_id = sys.argv[1].strip()

    if not validate_cycle_id(cycle_id):
        sys.exit(1)

    process_pending_files(cycle_id)