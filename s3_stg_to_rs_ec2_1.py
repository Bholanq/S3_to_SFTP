"""
EC2 Script: S3 Staging → Redshift Landing Tables  (Chunked + Multi-threaded)
=============================================================================
Usage    : python3 s3_stg_to_rs_landing.py 20260522
Trigger  : Manual / Cron job on EC2

Flow per file:
  1. LIST all CSV files in s3://.../Staging/{cycle_id}/
  2. For each file (sequentially):
       a. Download CSV from S3 into memory
       b. Split into NUM_CHUNKS equal parts (each with header row)
       c. Upload chunks to S3 under .../Staging/{cycle_id}/_chunks/{filename}/
       d. ThreadPoolExecutor (NUM_CHUNKS workers) runs COPY on each chunk
       e. Stamp cycle_id on newly loaded rows (UPDATE WHERE cycle_id IS NULL)
       f. Log result to rs_file_load_log
       g. Delete chunk files from S3 (cleanup)
  3. Move to next file

Why NUM_CHUNKS = 4:
  ra3.large has 2 slices/node x 2 nodes = 4 total slices.
  Splitting each CSV into exactly 4 chunks means every Redshift slice
  gets one chunk — maximum parallelism, no idle slices.

Load type: ALWAYS APPEND (no TRUNCATE at any point).

Author : Alumis Data Platform
"""

import boto3
import logging
import re
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
BUCKET_NAME         = ""
BASE_PREFIX         = ""
STAGING_PREFIX      = f""       # + /{cycle_id}/
CHUNKS_PREFIX       = ""                      # sub-folder inside cycle_id/

REDSHIFT_CLUSTER_ID = ""
REDSHIFT_DATABASE   = ""
REDSHIFT_DB_USER    = ""
REDSHIFT_IAM_ROLE   = ""                             # update

AWS_REGION          = ""

# ── ra3.large: 2 slices/node x 2 nodes = 4 slices total
# One chunk per slice = optimal COPY parallelism.
NUM_CHUNKS          = 4

# ── Redshift control/log tables
LOG_TABLE           = ""
LOAD_LOG_TABLE      = ""
LANDING_TABLE       = ""

# ── COPY options matching your original command
COPY_OPTIONS        = """FORMAT AS CSV
DELIMITER '|'
IGNOREHEADER 1
DATEFORMAT 'auto'
TIMEFORMAT 'auto'
EMPTYASNULL
FILLRECORD
BLANKSASNULL"""

# ── Data API polling
MAX_WAIT_SECONDS    = 600      # raised for larger chunk COPYs
POLL_INTERVAL       = 5

# ─────────────────────────────────────────────
# LOGGING  (includes thread name for clarity)
# ─────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# THREAD-LOCAL AWS CLIENTS
# boto3 clients are not thread-safe for concurrent calls.
# Each worker thread gets its own redshift-data and s3 client.
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

# ── Main-thread S3 client (used outside the pool)
s3_main = boto3.client("s3", region_name=AWS_REGION)

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
# Uses thread-local client — safe from any thread.
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
# STEP 1: List all CSV files in the staging folder
# ─────────────────────────────────────────────
def list_staging_files(cycle_id: str) -> list:
    """
    Returns sorted list of S3 keys for all .csv files under
    s3://{BUCKET_NAME}/{STAGING_PREFIX}/{cycle_id}/
    Excludes the _chunks sub-folder to avoid reprocessing.
    """
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
# STEP 2: Download a CSV from S3 into memory
# Returns (header_bytes, body_bytes)
# ─────────────────────────────────────────────
def download_csv(s3_key: str):
    logger.info(f"Downloading s3://{BUCKET_NAME}/{s3_key}")
    resp    = s3_main.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    content = resp["Body"].read()

    newline_pos = content.index(b"\n")
    header      = content[: newline_pos + 1]   # first line including \n
    body        = content[newline_pos + 1 :]    # data rows only

    return header, body

# ─────────────────────────────────────────────
# STEP 3: Split body into N chunks and upload to S3
# Returns list of S3 keys for the uploaded chunk files.
# ─────────────────────────────────────────────
def split_and_upload_chunks(header, body, s3_key, cycle_id, num_chunks=NUM_CHUNKS):
    """
    Splits body rows into num_chunks equal parts, prepends the header
    to each chunk, and uploads them under:
      {STAGING_PREFIX}/{cycle_id}/_chunks/{filename}/chunk_NN.csv
    """
    filename   = s3_key.split("/")[-1].replace(".csv", "")
    chunk_base = f"{STAGING_PREFIX}/{cycle_id}/{CHUNKS_PREFIX}/{filename}"

    rows       = body.splitlines(keepends=True)
    total_rows = len(rows)

    if total_rows == 0:
        logger.warning(f"File {filename} has no data rows — skipping.")
        return []

    # Ceiling division so all rows are covered
    chunk_size = max(1, -(-total_rows // num_chunks))
    chunk_keys = []

    for i in range(num_chunks):
        start      = i * chunk_size
        end        = min(start + chunk_size, total_rows)
        chunk_rows = rows[start:end]

        if not chunk_rows:
            logger.debug(f"Chunk {i} is empty — skipping.")
            continue

        chunk_data = header + b"".join(chunk_rows)
        chunk_key  = f"{chunk_base}/chunk_{i:02d}.csv"

        logger.info(f"  Uploading chunk {i+1}/{num_chunks} "
                    f"({len(chunk_rows):,} rows) → {chunk_key.split('/')[-1]}")
        s3_main.put_object(
            Bucket      = BUCKET_NAME,
            Key         = chunk_key,
            Body        = chunk_data,
            ContentType = "text/csv"
        )
        chunk_keys.append(chunk_key)

    return chunk_keys

# ─────────────────────────────────────────────
# STEP 4 (worker): COPY one chunk into Redshift
# Runs inside a thread-pool worker — uses thread-local RS client.
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
# STEP 5: Stamp cycle_id on rows just loaded
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
# STEP 6: Log result to rs_file_load_log
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
# STEP 7: Delete chunk files from S3 (cleanup)
# ─────────────────────────────────────────────
def cleanup_chunks(chunk_keys: list):
    if not chunk_keys:
        return
    objects = [{"Key": k} for k in chunk_keys]
    try:
        s3_main.delete_objects(
            Bucket = BUCKET_NAME,
            Delete = {"Objects": objects, "Quiet": True}
        )
        logger.info(f"Cleaned up {len(chunk_keys)} chunk file(s) from S3.")
    except Exception as e:
        logger.warning(f"Chunk cleanup failed (non-fatal): {e}")

# ─────────────────────────────────────────────
# PROCESS ONE FILE
# Download → split → parallel COPY → stamp → log → cleanup
# ─────────────────────────────────────────────
def process_file(s3_key: str, cycle_id: str) -> bool:
    """
    Full pipeline for a single CSV file.
    Returns True if all chunks loaded successfully.
    """
    filename        = s3_key.split("/")[-1]
    load_start_time = datetime.now(timezone.utc)
    chunk_keys      = []

    logger.info("━" * 60)
    logger.info(f"FILE START: {filename}")

    try:
        # ── Download ──────────────────────────────────────────────
        header, body = download_csv(s3_key)
        logger.info(f"Downloaded {filename} ({len(body):,} data bytes)")

        # ── Split and upload chunks ───────────────────────────────
        chunk_keys = split_and_upload_chunks(header, body, s3_key, cycle_id)

        if not chunk_keys:
            logger.warning(f"{filename} produced no chunks — skipping.")
            return False

        logger.info(f"Launching {len(chunk_keys)} worker thread(s) for COPY…")

        # ── Thread pool: COPY all chunks of this file in parallel ─
        # max_workers = number of chunks = number of Redshift slices
        # so every slice gets exactly one COPY at the same time.
        chunks_loaded = 0
        chunks_failed = 0
        failed_errors = []

        with ThreadPoolExecutor(
            max_workers        = len(chunk_keys),
            thread_name_prefix = "chunk-copy"
        ) as pool:
            future_to_idx = {
                pool.submit(copy_chunk, key, cycle_id, idx): (idx, key)
                for idx, key in enumerate(chunk_keys)
            }

            for future in as_completed(future_to_idx):
                idx, key = future_to_idx[future]
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

        # ── Overall file status ───────────────────────────────────
        file_success  = chunks_failed == 0
        error_message = "; ".join(failed_errors) if failed_errors else None

        # ── Stamp cycle_id on rows loaded by this file ────────────
        if file_success:
            if not stamp_cycle_id(cycle_id):
                file_success  = False
                error_message = "cycle_id stamp failed after successful COPY"

        load_end_time = datetime.now(timezone.utc)
        elapsed       = (load_end_time - load_start_time).total_seconds()

        # ── Log result ────────────────────────────────────────────
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
        logger.info(f"{status_icon} FILE {'DONE' if file_success else 'FAILED'}: "
                    f"{filename} | {chunks_loaded}/{len(chunk_keys)} chunks | {elapsed:.1f}s")

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
        # Always clean up chunks — even on failure
        cleanup_chunks(chunk_keys)

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def process_pending_files(cycle_id: str):
    logger.info("═" * 60)
    logger.info("EC2 Loader — S3 Staging → Redshift  [chunked + multi-threaded]")
    logger.info(f"cycle_id      : {cycle_id}")
    logger.info(f"Staging       : s3://{BUCKET_NAME}/{STAGING_PREFIX}/{cycle_id}/")
    logger.info(f"Target table  : {LANDING_TABLE}")
    logger.info(f"Cluster       : {REDSHIFT_CLUSTER_ID}")
    logger.info(f"Chunks/file   : {NUM_CHUNKS}  (matches Redshift slice count)")
    logger.info(f"Workers/file  : {NUM_CHUNKS}  (one thread per chunk)")
    logger.info("═" * 60)

    # ── Discover all CSV files ────────────────────────────────────
    all_files = list_staging_files(cycle_id)

    if not all_files:
        logger.warning(f"No CSV files found — nothing to load.")
        sys.exit(0)

    # ── TEST MODE: process only the first file ────────────────────
    # Remove or comment out this line to process all files.
    all_files = all_files[:1]

    total         = len(all_files)
    success_count = 0
    failed_count  = 0

    # ── Process each file one at a time ──────────────────────────
    # Parallelism lives INSIDE each file (4 concurrent chunk COPYs).
    # Processing files sequentially keeps Redshift WLM load constant:
    # always exactly NUM_CHUNKS concurrent COPYs, never more.
    for file_num, s3_key in enumerate(all_files, start=1):
        logger.info(f"\n[{file_num}/{total}] {s3_key.split('/')[-1]}")

        if process_file(s3_key, cycle_id):
            success_count += 1
        else:
            failed_count += 1
            # Log and continue — don't abort the whole run for one bad file

    # ── Summary ──────────────────────────────────────────────────
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