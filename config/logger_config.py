import logging
import os
from datetime import datetime
from colorama import Fore, Style, init

# Initialize colorama for Windows compatibility
init(autoreset=True)

create_file: bool = True


def configure_logger(base_log_file_name: str):
    """
    Configures and returns a logger instance with optional file logging and colored console output.

    Parameters:
    - base_log_file_name (str): Base name of the log file.

    Returns:
    - logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Generate a timestamped log file name
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file_name = f"{os.path.splitext(base_log_file_name)[0]}_{timestamp}.log"

    # Prevent adding multiple handlers if the logger is reused
    if not logger.handlers:
        log_dir = os.path.dirname(log_file_name)

        if create_file and log_dir:  # Check if log_dir is not empty
            os.makedirs(log_dir, exist_ok=True)

        # File handler (only if create_file is True)
        if create_file:
            file_handler = logging.FileHandler(log_file_name)
            file_handler.setLevel(logging.INFO)

            # Updated formatter with line number
            file_formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)

        # Stream handler with color-coded log levels and messages
        class ColoredFormatter(logging.Formatter):
            LEVEL_COLORS = {
                "DEBUG": Fore.BLUE,
                "INFO": Fore.GREEN,
                "WARNING": Fore.YELLOW,
                "ERROR": Fore.RED,
                "CRITICAL": Fore.MAGENTA,
            }

            def format(self, record):
                level_color = self.LEVEL_COLORS.get(record.levelname, "")
                record.levelname = f"{level_color}{record.levelname}{Style.RESET_ALL}"
                record.msg = f"{level_color}{record.msg}{Style.RESET_ALL}"
                return super().format(record)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)

        # Updated formatter with line number for console logs
        stream_formatter = ColoredFormatter(
            "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
        )
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)

    # Add a new function to the logger object to create a separator
    def separator():
        """Logs a white separator line for readability."""
        logger.info(
            f"{Fore.WHITE}--------------------------------------------------------------------------------{Style.RESET_ALL}")

    # Attach the function to the logger instance
    logger.separator = separator

    return logger


# Create a logger with a timestamped file
logger = configure_logger("logs/field_bronze_table.log")

# Example log with line number
logger.info("Logger initialized with timestamped file.")
