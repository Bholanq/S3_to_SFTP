import os
import tempfile
import boto3
import psycopg2
import csv
from dotenv import load_dotenv
from datetime import datetime

# Import custom logging framework
from logging_framework import LoggerManager

# =========================================================
# LOAD ENVIRONMENT VARIABLES
# =========================================================

load_dotenv()

# =========================================================
# INITIALIZE LOGGER
# =========================================================

SCRIPT_NAME = "redshift_to_s3_transfer"

# Example cycle ID:
# 20260528_153000
CYCLE_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

logger_manager = LoggerManager(SCRIPT_NAME)

logger_manager.write_log(
    CYCLE_ID,
    "Redshift to S3 transfer process started."
)

# =========================================================
# REDSHIFT CONFIGURATION
# =========================================================

REDSHIFT_HOST       = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT       = int(os.getenv("REDSHIFT_PORT", 5439))
REDSHIFT_DB         = os.getenv("REDSHIFT_DB")
REDSHIFT_USER       = os.getenv("REDSHIFT_USER")
REDSHIFT_CLUSTER_ID = os.getenv("REDSHIFT_CLUSTER_ID")
REDSHIFT_QUERY      = os.getenv("REDSHIFT_QUERY")

# =========================================================
# S3 CONFIGURATION
# =========================================================

S3_BUCKET     = os.getenv("S3_BUCKET")
S3_PREFIX     = os.getenv("S3_PREFIX")
S3_FILENAME   = os.getenv("S3_FILENAME", f"redshift_export_{CYCLE_ID}.csv")

# =========================================================
# FETCH IAM TEMPORARY CREDENTIALS
# =========================================================

logger_manager.write_log(
    CYCLE_ID,
    "Fetching IAM temporary credentials for Redshift."
)

try:

    redshift_client = boto3.client(
        "redshift",
        region_name=os.getenv("AWS_REGION", "us-east-1")
    )

    iam_creds = redshift_client.get_cluster_credentials(
        DbUser=REDSHIFT_USER,
        DbName=REDSHIFT_DB,
        ClusterIdentifier=REDSHIFT_CLUSTER_ID,
        AutoCreate=False
    )

    logger_manager.write_log(
        CYCLE_ID,
        "IAM temporary credentials fetched successfully."
    )

except Exception as e:

    logger_manager.write_log(
        CYCLE_ID,
        f"Failed to fetch IAM credentials: {str(e)}",
        "error"
    )

    raise

# =========================================================
# CONNECT TO REDSHIFT
# =========================================================

logger_manager.write_log(
    CYCLE_ID,
    f"Connecting to Redshift: {REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}"
)

try:

    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=iam_creds["DbUser"],
        password=iam_creds["DbPassword"],
        sslmode="require",
        connect_timeout=30
    )

    cursor = conn.cursor()

    logger_manager.write_log(
        CYCLE_ID,
        "Redshift connection established successfully."
    )

except Exception as e:

    logger_manager.write_log(
        CYCLE_ID,
        f"Failed to connect to Redshift: {str(e)}",
        "error"
    )

    raise

# =========================================================
# EXECUTE QUERY
# =========================================================

logger_manager.write_log(
    CYCLE_ID,
    "Executing Redshift query."
)

try:

    cursor.execute(REDSHIFT_QUERY)

    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]

    logger_manager.write_log(
        CYCLE_ID,
        f"Query executed successfully. Rows fetched: {len(rows)}"
    )

    if not rows:

        logger_manager.write_log(
            CYCLE_ID,
            "Query returned no results. Nothing to transfer.",
            "warning"
        )

        cursor.close()
        conn.close()

        exit()

except Exception as e:

    logger_manager.write_log(
        CYCLE_ID,
        f"Failed to execute Redshift query: {str(e)}",
        "error"
    )

    cursor.close()
    conn.close()

    raise

# =========================================================
# CLOSE REDSHIFT CONNECTION
# =========================================================

try:

    cursor.close()
    conn.close()

    logger_manager.write_log(
        CYCLE_ID,
        "Redshift connection closed."
    )

except Exception as e:

    logger_manager.write_log(
        CYCLE_ID,
        f"Error closing Redshift connection: {str(e)}",
        "warning"
    )

# =========================================================
# CREATE AWS S3 CLIENT
# =========================================================

logger_manager.write_log(
    CYCLE_ID,
    "Creating AWS S3 client."
)

try:

    s3 = boto3.client("s3")

    logger_manager.write_log(
        CYCLE_ID,
        "AWS S3 client created successfully."
    )

except Exception as e:

    logger_manager.write_log(
        CYCLE_ID,
        f"Failed to create S3 client: {str(e)}",
        "error"
    )

    raise

# =========================================================
# WRITE RESULTS TO TEMP FILE AND UPLOAD TO S3
# =========================================================

local_path = None

try:

    # =====================================================
    # CREATE TEMP FILE
    # =====================================================

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".csv",
        delete=False,
        newline=""
    ) as tmp_file:

        local_path = tmp_file.name

        writer = csv.writer(tmp_file)
        writer.writerow(column_names)
        writer.writerows(rows)

    logger_manager.write_log(
        CYCLE_ID,
        f"Temporary file created and written: {local_path}"
    )

    # =====================================================
    # BUILD S3 KEY
    # =====================================================

    s3_key = f"{S3_PREFIX.rstrip('/')}/{S3_FILENAME}"

    logger_manager.write_log(
        CYCLE_ID,
        f"Uploading file to s3://{S3_BUCKET}/{s3_key}"
    )

    # =====================================================
    # UPLOAD TO S3
    # =====================================================

    s3.upload_file(
        local_path,
        S3_BUCKET,
        s3_key
    )

    logger_manager.write_log(
        CYCLE_ID,
        f"S3 upload successful: {s3_key}"
    )

except Exception as e:

    logger_manager.write_log(
        CYCLE_ID,
        f"File transfer failed: {str(e)}",
        "error"
    )

    raise

finally:

    # =====================================================
    # DELETE TEMP FILE
    # =====================================================

    if local_path and os.path.exists(local_path):

        os.remove(local_path)

        logger_manager.write_log(
            CYCLE_ID,
            f"Temporary file deleted: {local_path}"
        )

logger_manager.write_log(
    CYCLE_ID,
    "Redshift to S3 transfer process completed successfully."
)

print("Transfer process completed.")