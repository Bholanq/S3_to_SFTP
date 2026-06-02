import os
import argparse
import tempfile
from datetime import datetime

import boto3
import psycopg2
import paramiko

from dotenv import load_dotenv
from logging_framework import LoggerManager

# =========================================================
# LOAD ENVIRONMENT VARIABLES
# =========================================================

load_dotenv()

# =========================================================
# COMMAND LINE ARGUMENTS
# =========================================================

parser = argparse.ArgumentParser(
    description="Redshift -> S3 Backup -> SFTP Transfer"
)

parser.add_argument(
    "--table_name",
    required=True,
    help="Redshift table name (schema.table)"
)

parser.add_argument(
    "--s3_folder",
    required=True,
    help="Full S3 destination folder. Example: s3://bucket/folder/path"
)

args = parser.parse_args()

TABLE_NAME = args.table_name
S3_FOLDER = args.s3_folder.rstrip("/")

if not S3_FOLDER.startswith("s3://"):
    raise ValueError(
        "--s3_folder must be a full S3 path. Example: s3://bucket/folder"
    )

# =========================================================
# PARSE S3 PATH
# =========================================================

s3_without_scheme = S3_FOLDER[5:]

parts = s3_without_scheme.split("/", 1)

S3_BUCKET = parts[0]

BASE_PREFIX = ""

if len(parts) > 1:
    BASE_PREFIX = parts[1]

# =========================================================
# LOGGER
# =========================================================

SCRIPT_NAME = "redshift_to_sftp_transfer"

CYCLE_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

logger_manager = LoggerManager(
    SCRIPT_NAME
)

logger_manager.write_log(
    CYCLE_ID,
    "Redshift to SFTP transfer process started."
)

# =========================================================
# CONFIGURATION
# =========================================================

REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = int(
    os.getenv("REDSHIFT_PORT", 5439)
)
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_CLUSTER_ID = os.getenv(
    "REDSHIFT_CLUSTER_ID"
)
REDSHIFT_IAM_ROLE = os.getenv(
    "REDSHIFT_IAM_ROLE"
)

AWS_REGION = os.getenv(
    "AWS_REGION",
    "us-east-1"
)

SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(
    os.getenv("SFTP_PORT", 22)
)
SFTP_USERNAME = os.getenv(
    "SFTP_USERNAME"
)
SFTP_PASSWORD = os.getenv(
    "SFTP_PASSWORD"
)
SFTP_REMOTE_DIR = os.getenv(
    "SFTP_REMOTE_DIR"
)

# =========================================================
# AWS CLIENTS
# =========================================================

logger_manager.write_log(
    CYCLE_ID,
    "Creating AWS clients."
)

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION
)

redshift_client = boto3.client(
    "redshift",
    region_name=AWS_REGION
)

# =========================================================
# GET TEMP REDSHIFT CREDENTIALS
# =========================================================

logger_manager.write_log(
    CYCLE_ID,
    "Fetching Redshift temporary credentials."
)

creds = redshift_client.get_cluster_credentials(
    DbUser=REDSHIFT_USER,
    DbName=REDSHIFT_DB,
    ClusterIdentifier=REDSHIFT_CLUSTER_ID,
    AutoCreate=False
)

# =========================================================
# CONNECT TO REDSHIFT
# =========================================================

logger_manager.write_log(
    CYCLE_ID,
    "Connecting to Redshift."
)

conn = psycopg2.connect(
    host=REDSHIFT_HOST,
    port=REDSHIFT_PORT,
    dbname=REDSHIFT_DB,
    user=creds["DbUser"],
    password=creds["DbPassword"],
    sslmode="require"
)

cursor = conn.cursor()

# =========================================================
# BUILD EXPORT LOCATION
# =========================================================

table_safe_name = TABLE_NAME.replace(
    ".",
    "_"
)

s3_prefix = (
    f"{BASE_PREFIX}/"
    f"{table_safe_name}_"
    f"{CYCLE_ID}"
).strip("/")

logger_manager.write_log(
    CYCLE_ID,
    f"Export path: s3://{S3_BUCKET}/{s3_prefix}"
)

# =========================================================
# UNLOAD TO S3
# =========================================================

unload_sql = f"""
UNLOAD ('SELECT * FROM {TABLE_NAME}')
TO 's3://{S3_BUCKET}/{s3_prefix}'
IAM_ROLE '{REDSHIFT_IAM_ROLE}'
FORMAT CSV
HEADER
PARALLEL ON
ALLOWOVERWRITE;
"""

logger_manager.write_log(
    CYCLE_ID,
    f"Starting UNLOAD for {TABLE_NAME}"
)

cursor.execute(
    unload_sql
)

conn.commit()

logger_manager.write_log(
    CYCLE_ID,
    "UNLOAD completed successfully."
)

cursor.close()
conn.close()

# =========================================================
# FIND EXPORTED FILE
# =========================================================

logger_manager.write_log(
    CYCLE_ID,
    "Searching exported file in S3."
)

response = s3.list_objects_v2(
    Bucket=S3_BUCKET,
    Prefix=s3_prefix
)

export_file = None

for obj in response.get(
    "Contents",
    []
):

    key = obj["Key"]

    if key.endswith("/"):
        continue

    if "manifest" in key.lower():
        continue

    export_file = key
    break

if not export_file:

    raise RuntimeError(
        f"No export file found in s3://{S3_BUCKET}/{s3_prefix}"
    )

logger_manager.write_log(
    CYCLE_ID,
    f"Export file found: {export_file}"
)

# =========================================================
# DOWNLOAD FILE
# =========================================================

local_path = tempfile.NamedTemporaryFile(
    delete=False,
    suffix=".csv"
).name

logger_manager.write_log(
    CYCLE_ID,
    f"Downloading file to {local_path}"
)

s3.download_file(
    S3_BUCKET,
    export_file,
    local_path
)

# =========================================================
# CONNECT TO SFTP
# =========================================================

logger_manager.write_log(
    CYCLE_ID,
    "Connecting to SFTP."
)

transport = paramiko.Transport(
    (
        SFTP_HOST,
        SFTP_PORT
    )
)

transport.connect(
    username=SFTP_USERNAME,
    password=SFTP_PASSWORD
)

sftp = paramiko.SFTPClient.from_transport(
    transport
)

sftp.chdir(
    SFTP_REMOTE_DIR
)

# =========================================================
# UPLOAD TO SFTP
# =========================================================

output_filename = (
    f"{table_safe_name}_{CYCLE_ID}.csv"
)

logger_manager.write_log(
    CYCLE_ID,
    f"Uploading {output_filename} to SFTP."
)

sftp.put(
    local_path,
    output_filename
)

logger_manager.write_log(
    CYCLE_ID,
    "SFTP upload completed."
)

# =========================================================
# CLEANUP
# =========================================================

sftp.close()
transport.close()

if os.path.exists(
    local_path
):
    os.remove(
        local_path
    )

logger_manager.write_log(
    CYCLE_ID,
    "Temporary file removed."
)

logger_manager.write_log(
    CYCLE_ID,
    "Process completed successfully."
)

print(
    f"Success. Backup stored at "
    f"s3://{S3_BUCKET}/{s3_prefix}"
)