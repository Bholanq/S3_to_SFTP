import os
import sys
import paramiko
from dotenv import load_dotenv
from logging_framework import LoggerManager
import time
# ==============================================================================
# INITIALIZATION
# ==============================================================================

SCRIPT_NAME = "sftp_to_sftp"

load_dotenv()

cycle_id = os.getenv("CYCLE_ID", "manual_run")

logger = LoggerManager(SCRIPT_NAME)

CHUNK_SIZE = 16 * 1024 * 1024  # 16 MB

files_transferred = 0
bytes_transferred = 0

job_start = time.time()

try:

    logger.write_log(
        cycle_id,
        "Starting SFTP to SFTP transfer process"
    )

    # ==========================================================================
    # SOURCE CONNECTION
    # ==========================================================================

    logger.write_log(
        cycle_id,
        f"Connecting to source SFTP: {os.getenv('SFTP_HOST')}"
    )

    src_transport = paramiko.Transport(
        (
            os.getenv("SFTP_HOST"),
            int(os.getenv("SFTP_PORT", 22))
        )
    )

    src_transport.connect(
        username=os.getenv("SFTP_USER"),
        password=os.getenv("SFTP_PASSWORD")
    )

    src_sftp = paramiko.SFTPClient.from_transport(src_transport)

    logger.write_log(
        cycle_id,
        "Source SFTP connection established"
    )

    # ==========================================================================
    # DESTINATION CONNECTION
    # ==========================================================================

    logger.write_log(
        cycle_id,
        f"Connecting to destination SFTP: {os.getenv('DST_SFTP_HOST')}"
    )

    dst_transport = paramiko.Transport(
        (
            os.getenv("DEST_SFTP_HOST"),
            int(os.getenv("DEST_SFTP_PORT", 22))
        )
    )

    dst_transport.connect(
        username=os.getenv("DEST_SFTP_USER"),
        password=os.getenv("DEST_SFTP_PASSWORD")
    )

    dst_sftp = paramiko.SFTPClient.from_transport(dst_transport)

    logger.write_log(
        cycle_id,
        "Destination SFTP connection established"
    )

    # ==========================================================================
    # DIRECTORY CONFIGURATION
    # ==========================================================================

    source_dir = os.getenv("SFTP_REMOTE_DIR")
    dest_dir = os.getenv("DEST_SFTP_REMOTE_DIR")

    logger.write_log(
        cycle_id,
        f"Source Directory: {source_dir}"
    )

    logger.write_log(
        cycle_id,
        f"Destination Directory: {dest_dir}"
    )

    # ==========================================================================
    # FILE TRANSFERS
    # ==========================================================================

    files = src_sftp.listdir(source_dir)

    logger.write_log(
        cycle_id,
        f"Found {len(files)} files for transfer"
    )

    for filename in files:

        source_file = f"{source_dir}/{filename}"
        dest_file = f"{dest_dir}/{filename}"

        try:

            file_size = src_sftp.stat(source_file).st_size

            logger.write_log(
                cycle_id,
                f"Starting transfer: {filename} "
                f"({file_size:,} bytes)"
            )

            transferred = 0

            with src_sftp.open(source_file, "rb") as src_file:
                with dst_sftp.open(dest_file, "wb") as dst_file:

                    while True:

                        chunk = src_file.read(CHUNK_SIZE)

                        if not chunk:
                            break

                        dst_file.write(chunk)

                        transferred += len(chunk)

            files_transferred += 1
            bytes_transferred += transferred

            logger.write_log(
                cycle_id,
                f"Completed transfer: {filename} "
                f"({transferred:,} bytes)"
            )

        except Exception as file_error:

            logger.write_log(
                cycle_id,
                f"Failed transfer for {filename}: {str(file_error)}",
                "error"
            )

    # ==========================================================================
    # SUMMARY
    # ==========================================================================

    logger.write_log(
        cycle_id,
        f"Transfer complete. "
        f"Files transferred={files_transferred}, "
        f"Bytes transferred={bytes_transferred:,}"
    )

except Exception as e:

    logger.write_log(
        cycle_id,
        f"Fatal error: {str(e)}",
        "error"
    )

    sys.exit(1)

finally:

    try:
        src_sftp.close()
        src_transport.close()
    except:
        pass

    try:
        dst_sftp.close()
        dst_transport.close()
    except:
        pass

    logger.write_log(
        cycle_id,
        "SFTP connections closed"
    )

job_elapsed = time.time() - job_start

overall_speed = (
    bytes_transferred / 1024 / 1024
) / job_elapsed

logger.write_log(
    cycle_id,
    f"overall thorughput = {overall_speed:.2f} MB/s"
)