```python
import os
import tempfile
import boto3
import paramiko
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

SCRIPT_NAME = "s3_to_sftp_transfer"

# Example cycle ID:
# 20260528_153000
CYCLE_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

logger_manager = LoggerManager(SCRIPT_NAME)

logger_manager.write_log(
    CYCLE_ID,
    "S3 to SFTP transfer process started."
)

# =========================================================
# S3 CONFIGURATION
# =========================================================

S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX")

# =========================================================
# SFTP CONFIGURATION
# =========================================================

SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT", 22))
SFTP_USERNAME = os.getenv("SFTP_USERNAME")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD")
SFTP_REMOTE_DIR = os.getenv("SFTP_REMOTE_DIR")

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
# LIST FILES FROM S3 PREFIX
# =========================================================

logger_manager.write_log(
    CYCLE_ID,
    f"Reading files from s3://{S3_BUCKET}/{S3_PREFIX}"
)

try:

    response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=S3_PREFIX
    )

    if "Contents" not in response:

        logger_manager.write_log(
            CYCLE_ID,
            "No files found in S3 location.",
            "warning"
        )

        exit()

    logger_manager.write_log(
        CYCLE_ID,
        "Successfully retrieved S3 object list."
    )

except Exception as e:

    logger_manager.write_log(
        CYCLE_ID,
        f"Failed to list S3 objects: {str(e)}",
        "error"
    )

    raise

# =========================================================
# CONNECT TO SFTP
# =========================================================

logger_manager.write_log(
    CYCLE_ID,
    f"Connecting to SFTP server: {SFTP_HOST}:{SFTP_PORT}"
)

try:

    transport = paramiko.Transport(
        (SFTP_HOST, SFTP_PORT)
    )

    transport.connect(
        username=SFTP_USERNAME,
        password=SFTP_PASSWORD
    )

    logger_manager.write_log(
        CYCLE_ID,
        "SFTP authentication successful."
    )

    sftp = paramiko.SFTPClient.from_transport(
        transport
    )

    logger_manager.write_log(
        CYCLE_ID,
        "SFTP client created successfully."
    )

except Exception as e:

    logger_manager.write_log(
        CYCLE_ID,
        f"SFTP connection failed: {str(e)}",
        "error"
    )

    raise

# =========================================================
# VERIFY REMOTE DIRECTORY EXISTS
# =========================================================

try:

    sftp.chdir(SFTP_REMOTE_DIR)

    logger_manager.write_log(
        CYCLE_ID,
        f"Verified remote directory exists: {SFTP_REMOTE_DIR}"
    )

except IOError as e:

    logger_manager.write_log(
        CYCLE_ID,
        f"Remote directory validation failed: {str(e)}",
        "error"
    )

    sftp.close()
    transport.close()

    raise

# =========================================================
# PROCESS FILES
# =========================================================

for obj in response["Contents"]:

    s3_key = obj["Key"]

    # Skip folder placeholders
    if s3_key.endswith("/"):
        continue

    filename = os.path.basename(s3_key)

    logger_manager.write_log(
        CYCLE_ID,
        f"Starting processing for file: {filename}"
    )

    local_path = None

    try:

        # =====================================================
        # CREATE TEMP FILE
        # =====================================================

        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:

            local_path = tmp_file.name

        logger_manager.write_log(
            CYCLE_ID,
            f"Temporary file created: {local_path}"
        )

        # =====================================================
        # DOWNLOAD FILE FROM S3
        # =====================================================

        logger_manager.write_log(
            CYCLE_ID,
            f"Downloading file from S3: {s3_key}"
        )

        s3.download_file(
            S3_BUCKET,
            s3_key,
            local_path
        )

        logger_manager.write_log(
            CYCLE_ID,
            f"S3 download completed: {filename}"
        )

        # =====================================================
        # BUILD REMOTE PATH
        # =====================================================

        remote_path = f"{SFTP_REMOTE_DIR}/{filename}"

        logger_manager.write_log(
            CYCLE_ID,
            f"Uploading file to SFTP: {remote_path}"
        )

        # =====================================================
        # UPLOAD TO SFTP
        # =====================================================

        sftp.put(
            local_path,
            remote_path
        )

        logger_manager.write_log(
            CYCLE_ID,
            f"SFTP upload successful: {filename}"
        )

    except Exception as e:

        logger_manager.write_log(
            CYCLE_ID,
            f"File transfer failed for {filename}: {str(e)}",
            "error"
        )

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

# =========================================================
# CLOSE CONNECTIONS
# =========================================================

try:

    sftp.close()

    logger_manager.write_log(
        CYCLE_ID,
        "SFTP client connection closed."
    )

except Exception as e:

    logger_manager.write_log(
        CYCLE_ID,
        f"Error closing SFTP client: {str(e)}",
        "warning"
    )

try:

    transport.close()

    logger_manager.write_log(
        CYCLE_ID,
        "SSH transport connection closed."
    )

except Exception as e:

    logger_manager.write_log(
        CYCLE_ID,
        f"Error closing transport connection: {str(e)}",
        "warning"
    )

logger_manager.write_log(
    CYCLE_ID,
    "S3 to SFTP transfer process completed successfully."
)

print("Transfer process completed.")
```
