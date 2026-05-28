import os
import paramiko
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

# Read SFTP config
SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT", 22))
SFTP_USERNAME = os.getenv("SFTP_USERNAME")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD")
SFTP_REMOTE_DIR = os.getenv("SFTP_REMOTE_DIR")

print("Connecting to SFTP server...")

try:
    # Create transport/ connection object
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))

    # Authenticate
    transport.connect(
        username=SFTP_USERNAME,
        password=SFTP_PASSWORD
    )

    # Create SFTP client
    sftp = paramiko.SFTPClient.from_transport(transport)

    print("SFTP connection successful.")

    # Verify remote folder exists
    print(f"Checking remote folder: {SFTP_REMOTE_DIR}")

    try:
        sftp.chdir(SFTP_REMOTE_DIR)

        current_dir = sftp.getcwd()

        print("Folder exists.")
        print(f"Current remote directory: {current_dir}")

        # Optional: list files
        print("\nFiles in directory:")

        files = sftp.listdir()

        if not files:
            print("(empty directory)")
        else:
            for file in files:
                print(f"- {file}")

    except IOError:
        print("Folder does NOT exist or permission denied.")

    # Close connection
    sftp.close()
    transport.close()

    print("\nConnection closed.")

except Exception as e:
    print(f"Connection failed: {e}")