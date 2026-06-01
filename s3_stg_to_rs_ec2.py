"""
EC2 Script: S3 Staging → Redshift Landing Tables
=================================================
Usage    : python3 s3_stg_to_rs_landing.py 20260522
Trigger  : Manual / Cron job on EC2
Flow     : Read file_transfer_log (load_flag=0) from Redshift
           → lookup config_file_master by file_id
           → TRUNCATE landing table if load_type = O
           → run COPY command into Redshift landing table (with cycle_id column)
           → log result to rs_file_load_log
           → update load_flag=1 in file_transfer_log
Author   : Alumis Data Platform

Redshift paths:
  Control tables :
  Landing tables :

S3 paths:
  Staging        :
"""

import boto3
import logging
import sys
import time
import re
import traceback
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# CONFIGURATION — update these values
# ─────────────────────────────────────────────
BUCKET_NAME         = ""
BASE_PREFIX         = ""
STAGING_PREFIX      = f""          # + /{cycle_id}/

REDSHIFT_CLUSTER_ID = ""                        # update
REDSHIFT_DATABASE   = ""                                              # confirmed
REDSHIFT_DB_USER    = ""                              # update
REDSHIFT_IAM_ROLE   = "" #  update

AWS_REGION          = "eu-north-1"                                      #update to your region

# ── Redshift table references (database.schema.table)
LOG_TABLE           = ""
CONFIG_TABLE        = ""
LOAD_LOG_TABLE      = ""

# ── Data API polling settings
MAX_WAIT_SECONDS    = 
POLL_INTERVAL       = 

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# AWS CLIENT  (credentials from EC2 IAM role)
# ─────────────────────────────────────────────
rs = boto3.client("redshift-data", region_name=AWS_REGION)


# ─────────────────────────────────────────────
# HELPER: Validate cycle_id format
# ─────────────────────────────────────────────
def validate_cycle_id(cycle_id: str) -> bool:
    """Expects YYYYMMDD format e.g. 20260522"""
    if re.fullmatch(r"\d{8}", cycle_id):
        return True
    logger.error(f"Invalid cycle_id format: '{cycle_id}'. Expected YYYYMMDD (e.g. 20260522).")
    return False


# ─────────────────────────────────────────────
# CORE: Execute SQL on Redshift via Data API
# ─────────────────────────────────────────────
def execute_sql(sql: str, description: str = "", fetch_results: bool = False):
    """
    Submits SQL to Redshift Data API and polls until complete.
    Returns: (success: bool, error_message: str|None, records: list|None)
    """
    logger.info(f"Executing: {description or sql[:80]}")

    try:
        response = rs.execute_statement(
            ClusterIdentifier = REDSHIFT_CLUSTER_ID,
            Database          = REDSHIFT_DATABASE,
            DbUser            = REDSHIFT_DB_USER,
            Sql               = sql
        )
        stmt_id = response["Id"]
        logger.info(f"Statement ID: {stmt_id}")
    except Exception as e:
        logger.error(f"Failed to submit statement: {e}")
        return False, str(e)[:1000], None

    # ── Poll until complete ──────────────────────
    elapsed = 0
    while elapsed < MAX_WAIT_SECONDS:
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

        try:
            status = rs.describe_statement(Id=stmt_id)
            state  = status["Status"]
            logger.info(f"  State: {state} ({elapsed}s)")

            if state == "FINISHED":
                records = None
                if fetch_results:
                    result  = rs.get_statement_result(Id=stmt_id)
                    records = result.get("Records", [])
                return True, None, records

            elif state in ("FAILED", "ABORTED"):
                error = status.get("Error", "Unknown error")
                logger.error(f"Statement {state}: {error}")
                return False, str(error)[:1000], None

        except Exception as e:
            logger.error(f"Error polling statement: {e}")
            return False, str(e)[:1000], None

    msg = f"Timed out after {MAX_WAIT_SECONDS}s — stmt_id={stmt_id}"
    logger.error(msg)
    return False, msg, None


# ─────────────────────────────────────────────
# UTILITY: Extract typed value from Data API column
# ─────────────────────────────────────────────
def _val(col: dict):
    """
    Redshift Data API returns each column as:
      {"stringValue": "..."} | {"longValue": 123} | {"isNull": True}
    """
    if col.get("isNull"):
        return None
    return (
        col.get("stringValue") or
        col.get("longValue")   or
        col.get("doubleValue") or
        col.get("booleanValue")
    )


# ─────────────────────────────────────────────
# HELPER: Fetch pending records from file_transfer_log
# ─────────────────────────────────────────────
def fetch_pending_records(cycle_id: str):
    """
    Returns file_transfer_log rows where:
      - load_flag      = 0  (not yet loaded into Redshift)
      - transfer_status = 1  (successfully copied to staging)
      - staging_path contains the cycle_id folder
    """
    sql = f"""
        SELECT
            log_id,
            file_id,
            landing_file_name,
            staging_path,
            staging_file_name
        FROM {LOG_TABLE}
        WHERE load_flag       = 0
          AND transfer_status = 1
          AND staging_path LIKE '%/{cycle_id}/%'
        ORDER BY insert_datetime ASC;
    """
    success, error, records = execute_sql(
        sql           = sql,
        description   = f"Fetch pending records for cycle_id={cycle_id}",
        fetch_results = True
    )

    if not success:
        logger.error(f"Failed to fetch pending records: {error}")
        return []

    rows = []
    for rec in (records or []):
        rows.append({
            "log_id"            : _val(rec[0]),
            "file_id"           : _val(rec[1]),
            "landing_file_name" : _val(rec[2]),
            "staging_path"      : _val(rec[3]),
            "staging_file_name" : _val(rec[4]),
        })

    logger.info(f"Pending records for cycle_id={cycle_id}: {len(rows)}")
    return rows


# ─────────────────────────────────────────────
# HELPER: Fetch config from config_file_master
# ─────────────────────────────────────────────
def fetch_config(file_id: int):
    """
    Returns config dict for given file_id, or None if not found/inactive.
    """
    sql = f"""
        SELECT
            file_id,
            dataset_name,
            misc_copy_cmd,
            target_schema,
            landing_table,
            load_type
        FROM {CONFIG_TABLE}
        WHERE file_id    = {file_id}
          AND active_flag = '1'
        LIMIT 1;
    """
    success, error, records = execute_sql(
        sql           = sql,
        description   = f"Fetch config for file_id={file_id}",
        fetch_results = True
    )

    if not success or not records:
        logger.warning(f"No active config for file_id={file_id}: {error}")
        return None

    rec = records[0]
    return {
        "file_id"       : _val(rec[0]),
        "dataset_name"  : _val(rec[1]),
        "misc_copy_cmd" : _val(rec[2]),
        "target_schema" : _val(rec[3]),   # e.g. "sandbox"
        "landing_table" : _val(rec[4]),   # e.g. "lndg_compass_data"
        "load_type"     : _val(rec[5]),   # "O" = override (truncate+load), "A" = append
    }


# ─────────────────────────────────────────────
# HELPER: Build Redshift COPY SQL with cycle_id
# ─────────────────────────────────────────────
def build_copy_sql(staging_path: str, target_schema: str,
                   landing_table: str, misc_copy_cmd: str,
                   cycle_id: str) -> str:
    """
    Loads the staging CSV into the landing table and appends cycle_id
    as a literal column using Redshift COPY with column mapping.

    The landing table must have a cycle_id VARCHAR column defined.

    Strategy:
      1. COPY the file columns normally into a staging temp approach
         OR use a two-step: COPY raw then UPDATE cycle_id.

    Since Redshift COPY cannot inject a literal — we use a two-step:
      Step A : COPY file data into landing table (cycle_id will be NULL/default)
      Step B : UPDATE cycle_id = '{cycle_id}' WHERE cycle_id IS NULL

    Both SQLs are returned as a tuple so main() can run them in sequence.
    """
    full_table = f"dev.{target_schema}.{landing_table}"

    copy_sql = f"""
        COPY {full_table}
        FROM '{staging_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        {misc_copy_cmd};
    """.strip()

    # After COPY, stamp the cycle_id on rows that were just loaded (cycle_id is NULL)
    update_sql = f"""
        UPDATE {full_table}
        SET cycle_id = '{cycle_id}'
        WHERE cycle_id IS NULL;
    """.strip()

    return copy_sql, update_sql


# ─────────────────────────────────────────────
# HELPER: Insert into rs_file_load_log
# ─────────────────────────────────────────────
def insert_load_log(cycle_id, file_id, landing_file_name, staging_file_name,
                    staging_path, target_schema, landing_table, copy_sql,
                    load_start_time, load_end_time, status, error_message=None):

    def esc(v): return str(v or "").replace("'", "''")

    start_str = load_start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_str   = load_end_time.strftime("%Y-%m-%d %H:%M:%S")
    now_str   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    err_str   = esc(error_message)[:1000] if error_message else ""

    sql = f"""
        INSERT INTO {LOAD_LOG_TABLE} (
            cycle_time_id,
            file_id,
            landing_file_name,
            staging_file_name,
            staging_path,
            target_schema,
            landing_table,
            copy_sql,
            load_start_time,
            load_end_time,
            status,
            error_message,
            insert_datetime
        ) VALUES (
            '{cycle_id}',
            {file_id},
            '{esc(landing_file_name)}',
            '{esc(staging_file_name)}',
            '{esc(staging_path)}',
            '{esc(target_schema)}',
            '{esc(landing_table)}',
            '{esc(copy_sql)[:2000]}',
            '{start_str}',
            '{end_str}',
            {status},
            '{err_str}',
            '{now_str}'
        );
    """
    success, error, _ = execute_sql(sql, description=f"Insert load log | {staging_file_name}")
    if not success:
        logger.error(f"Failed to insert load log: {error}")


# ─────────────────────────────────────────────
# HELPER: Mark load_flag=1 in file_transfer_log
# ─────────────────────────────────────────────
def mark_as_loaded(log_id):
    sql = f"UPDATE {LOG_TABLE} SET load_flag = 1 WHERE log_id = {log_id};"
    success, error, _ = execute_sql(sql, description=f"Mark load_flag=1 for log_id={log_id}")
    if not success:
        logger.error(f"Failed to update load_flag: {error}")
    else:
        logger.info(f"load_flag → 1 for log_id={log_id}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def process_pending_files(cycle_id: str):
    logger.info("════════════════════════════════════════════════════════════")
    logger.info("EC2 Loader — S3 Staging → Redshift Landing")
    logger.info(f"cycle_id    : {cycle_id}")
    logger.info(f"Staging     : s3://{BUCKET_NAME}/{STAGING_PREFIX}/{cycle_id}/")
    logger.info(f"Cluster     : {REDSHIFT_CLUSTER_ID}")
    logger.info(f"Database    : {REDSHIFT_DATABASE}")
    logger.info("════════════════════════════════════════════════════════════")

    # ── Step 1: Fetch pending records for this cycle_id ──
    pending = fetch_pending_records(cycle_id)
    if not pending:
        logger.warning(
            f"\n{'='*60}\n"
            f"    NO PENDING FILES TO LOAD\n"
            f"  cycle_id : {cycle_id}\n"
            f"  Either all files are already loaded (load_flag=1)\n"
            f"  or the S3→Landing transfer has not run yet for this cycle.\n"
            f"{'='*60}"
        )
        return

    success_count = 0
    failed_count  = 0

    for record in pending:
        log_id            = record["log_id"]
        file_id           = record["file_id"]
        staging_path      = record["staging_path"]
        staging_file_name = record["staging_file_name"]
        landing_file_name = record["landing_file_name"]

        logger.info("────────────────────────────────────────────────────────────")
        logger.info(f"log_id={log_id} | file_id={file_id} | {staging_file_name}")

        # ── Step 2: Fetch config from config_file_master ──
        config = fetch_config(file_id)
        if not config:
            logger.error(f"Skipping — no active config for file_id={file_id}")
            failed_count += 1
            continue

        target_schema = config["target_schema"]   # e.g. sandbox
        landing_table = config["landing_table"]   # e.g. lndg_compass_data
        load_type     = config["load_type"]        # O=override, A=append
        misc_copy_cmd = config["misc_copy_cmd"]    # e.g. FORMAT AS CSV IGNOREHEADER 1
        full_table    = f"dev.{target_schema}.{landing_table}"

        copy_sql, update_sql = build_copy_sql(
            staging_path  = staging_path,
            target_schema = target_schema,
            landing_table = landing_table,
            misc_copy_cmd = misc_copy_cmd,
            cycle_id      = cycle_id
        )

        load_start_time = datetime.now(timezone.utc)
        status          = 0
        error_message   = None

        try:
            # ── Step 3: TRUNCATE if load_type = O (Override) ──
            if str(load_type).upper() == "O":
                logger.info(f"Load type OVERRIDE — truncating {full_table}")
                trunc_ok, trunc_err, _ = execute_sql(
                    sql         = f"TRUNCATE TABLE {full_table};",
                    description = f"TRUNCATE {full_table}"
                )
                if not trunc_ok:
                    raise Exception(f"TRUNCATE failed: {trunc_err}")

            # ── Step 4A: COPY file from S3 → Redshift ─────────
            copy_ok, copy_err, _ = execute_sql(
                sql         = copy_sql,
                description = f"COPY → {full_table}"
            )
            if not copy_ok:
                raise Exception(f"COPY failed: {copy_err}")

            # ── Step 4B: Stamp cycle_id on newly loaded rows ──
            logger.info(f"Stamping cycle_id='{cycle_id}' on loaded rows in {full_table}")
            upd_ok, upd_err, _ = execute_sql(
                sql         = update_sql,
                description = f"UPDATE cycle_id → {full_table}"
            )
            if not upd_ok:
                raise Exception(f"UPDATE cycle_id failed: {upd_err}")

            load_end_time  = datetime.now(timezone.utc)
            status         = 1
            success_count += 1
            logger.info(f"✓ Loaded {staging_file_name} → {full_table} | cycle_id={cycle_id}")

        except Exception as e:
            load_end_time = datetime.now(timezone.utc)
            error_message = str(e)[:1000]
            logger.error(f"✗ Failed log_id={log_id}: {e}")
            logger.error(traceback.format_exc())
            failed_count += 1

        # ── Step 5: Log result to rs_file_load_log ────────────
        try:
            insert_load_log(
                cycle_id          = cycle_id,
                file_id           = file_id,
                landing_file_name = landing_file_name,
                staging_file_name = staging_file_name,
                staging_path      = staging_path,
                target_schema     = target_schema,
                landing_table     = landing_table,
                copy_sql          = copy_sql,
                load_start_time   = load_start_time,
                load_end_time     = load_end_time,
                status            = status,
                error_message     = error_message
            )
        except Exception as e:
            logger.error(f"Failed to insert load log for log_id={log_id}: {e}")

        # ── Step 6: Mark load_flag=1 on success only ─────────
        if status == 1:
            try:
                mark_as_loaded(log_id)
            except Exception as e:
                logger.error(f"Failed to mark load_flag for log_id={log_id}: {e}")

    # ── Final summary ─────────────────────────────────────────
    total = len(pending)
    logger.info("════════════════════════════════════════════════════════════")
    logger.info(f"DONE  |  cycle_id: {cycle_id}")
    logger.info(f"Total: {total}  |  Success: {success_count}  |  Failed: {failed_count}")
    logger.info("════════════════════════════════════════════════════════════")

    if failed_count > 0:
        sys.exit(1)   # non-zero so cron/orchestrator detects partial failure
    sys.exit(0)


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage  : python3 s3_stg_to_rs_landing.py <cycle_id>")
        print("Example: python3 s3_stg_to_rs_landing.py 20260522")
        sys.exit(1)

    cycle_id = sys.argv[1].strip()

    if not validate_cycle_id(cycle_id):
        sys.exit(1)

    process_pending_files(cycle_id)