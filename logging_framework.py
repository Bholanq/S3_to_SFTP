################################################################################
#
#   Script Name - logging_framework.py
#   Description - This is a general script used for logging in Python.
#                 It can be imported and used in other scripts to provide a consistent logging mechanism.
#
################################################################################

# ==============================================================================
# Import Packages
# ==============================================================================
import logging
import os
from datetime import datetime


# ==========================================================================================
# Declare Variables
# ==========================================================================================

SCRIPT_NAME = None


# ==========================================================================================
# Main Classes and Functions
# ==========================================================================================
class LoggerManager:
    BASE_LOG_FOLDER = os.path.join(
    os.getcwd(),
    "logs"
    )
    def __init__(self, script_name):

        self.script_name= script_name
        self.logger={}

    def write_log(self, cycle_id, message, log_type="info"):

        # Create the folder for each cycle in log directory if it doesn't exist
        cycle_log_folder = os.path.join(self.BASE_LOG_FOLDER,cycle_id)
        os.makedirs(cycle_log_folder, exist_ok=True)
        logger_key = f"{cycle_id}_{self.script_name}"

        # Check if logger for this cycle and script already exists, if not create it
        if logger_key not in self.logger:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = os.path.join(cycle_log_folder,f"{self.script_name}_{timestamp}.log")
            logger = logging.getLogger(logger_key)
            logger.setLevel(logging.INFO)
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

            # File handler
            # file_handler = logging.FileHandler(log_file)
            # file_handler.setFormatter(formatter)
            # logger.addHandler(file_handler)
            # =========================================================
            # FILE HANDLER
            # Writes logs to log file
            # =========================================================

            file_handler = logging.FileHandler(log_file)

            file_handler.setFormatter(formatter)

            # =========================================================
            # CONSOLE HANDLER
            # Displays logs live in terminal
            # =========================================================

            console_handler = logging.StreamHandler()

            console_handler.setFormatter(formatter)

            # =========================================================
            # ADD HANDLERS
            # =========================================================

            if not logger.handlers:

                logger.addHandler(file_handler)

                logger.addHandler(console_handler)
            

            # Store the logger in the dictionary
            self.logger[logger_key] = logger
        
        # Get the logger and write the message
        logger = self.logger[logger_key]
        
        # Log the message based on the log type
        if log_type.lower() == "info":
            logger.info(message)

        elif log_type.lower() == "error":
            logger.error(message)

        elif log_type.lower() == "warning":
            logger.warning(message)

        else:
            logger.info(message)