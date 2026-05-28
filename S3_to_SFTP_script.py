import os
import tempfile
import boto3
import paramiko
from dotenv import load_dotenv

# =========================================================
# LOAD ENVIRONMENT VARIABLES
# =========================================================

load_dotenv()

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

print("Creating S3 client...")

s3 = boto3.client("s3")

# =========================================================
# LIST FILES FROM S3 PREFIX
# =========================================================

print(f"Reading files from:")
print(f"s3://{S3_BUCKET}/{S3_PREFIX}")

response = s3.list_objects_v2(
    Bucket=S3_BUCKET,
    Prefix=S3_PREFIX
)

# Check if files exist
if "Contents" not in response:
    print("No files found in S3 location.")
    exit()

# =========================================================
# CREATE SSH TRANSPORT CONNECTION
# =========================================================

print("\nConnecting to SFTP server...")

transport = paramiko.Transport(
    (SFTP_HOST, SFTP_PORT)
)

# =========================================================
# AUTHENTICATE WITH SFTP SERVER
# =========================================================

transport.connect(
    username=SFTP_USERNAME,
    password=SFTP_PASSWORD
)

print("SFTP authentication successful.")

# =========================================================
# CREATE SFTP CLIENT
# =========================================================

sftp = paramiko.SFTPClient.from_transport(
    transport
)

# =========================================================
# VERIFY REMOTE DIRECTORY EXISTS
# =========================================================

try:
    sftp.chdir(SFTP_REMOTE_DIR)

    print(f"Remote folder exists:")
    print(SFTP_REMOTE_DIR)

except IOError:

    print("Remote folder does not exist or access denied.")
    
    sftp.close()
    transport.close()

    exit()

# =========================================================
# PROCESS FILES FROM S3
# =========================================================

for obj in response["Contents"]:

    s3_key = obj["Key"]

    # Skip folder placeholders
    if s3_key.endswith("/"):
        continue

    # Extract filename only
    filename = os.path.basename(s3_key)

    print("\n-----------------------------------")
    print(f"Processing file: {filename}")

    # =====================================================
    # CREATE TEMP LOCAL FILE
    # =====================================================

    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:

        local_path = tmp_file.name

    # =====================================================
    # DOWNLOAD FILE FROM S3
    # =====================================================

    print("Downloading from S3...")

    s3.download_file(
        S3_BUCKET,
        s3_key,
        local_path
    )

    print(f"Downloaded to temp location:")
    print(local_path)

    # =====================================================
    # BUILD REMOTE SFTP PATH
    # =====================================================

    remote_path = f"{SFTP_REMOTE_DIR}/{filename}"

    print(f"Uploading to SFTP:")
    print(remote_path)

    # =====================================================
    # UPLOAD FILE TO SFTP
    # =====================================================

    sftp.put(
        local_path,
        remote_path
    )

    print("Upload successful.")

    # =====================================================
    # DELETE TEMP FILE
    # =====================================================

    os.remove(local_path)

    print("Temporary local file deleted.")

# =========================================================
# CLOSE CONNECTIONS
# =========================================================

sftp.close()

transport.close()

print("\n===================================")
print("All files transferred successfully.")
print("SFTP connection closed.")
print("===================================")