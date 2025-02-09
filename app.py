import pandas as pd

from config.logger_config import logger
from crawler import start_polling_thread, poll_table
from file_processor.file_processor_registry import FileProcessorRegistry
from models.files import insert_data, fetch_files_to_process, update_file_status
from utils.db_util import get_session


def insert_fields_data_in_db(filepath):
    """
    Insert field data into the database.

    :param filepath: Path to the file to insert data from.
    """
    with get_session() as session:  # Establishing a database session
        try:
            logger.info(f"Inserting data from file: {filepath}")  # Logging start of insertion
            insert_data(session, str(filepath), 'FIELD', '')  # Inserting file data into database
            logger.info("Data insertion completed successfully.")  # Logging success
        except Exception as e:
            logger.error(f"Error inserting data from file {filepath}: {e}")  # Logging errors if any


def read_fields_data_in_db():
    """
    Read data from the database, validate it, and update file statuses.
    """
    with get_session() as session:  # Establishing a database session
        # Fetch files that need processing
        results = fetch_files_to_process(session)

        if not results:  # Check if results is None or an empty list
            logger.info("No files available for processing at this time.")
            return

        logger.separator()
        logger.info(f"Processing file: {results.filepath} with file_status {results.file_status}")

        # Read the file into a Pandas DataFrame
        df = pd.read_csv(results.filepath)

        # Get the appropriate file processor based on file metadata
        file_processor = FileProcessorRegistry.get_processor(results.id, results.filename, results.datatype)

        # If file status is 'BRONZE_PROCESSED', move to silver validation
        if results.file_status == 'BRONZE_PROCESSED':
            logger.info(f"Updating file '{results.filename}' status to 'SILVER_PROCESSING'.")
            update_file_status(session, 'SILVER_PROCESSING', results.id)  # Updating status
            file_processor.process()  # Processing file
            logger.info(f"Field silver processing completed successfully. Updating file '{results.filename}' status to 'SILVER_PROCESSED'.")
            update_file_status(session, 'SILVER_PROCESSED', results.id)  # Updating to final processed status
            return

        # Validate columns before further processing
        if file_processor.validate_columns(df):
            logger.error(f"Column validation failed. Updating file '{results.filename}' status to error.")
            update_file_status(session, 'ERROR', results.id, "Error: Columns do not match")  # Log error
            return
        else:
            logger.info(f"Column validation passed. Updating file '{results.filename}' status to 'BRONZE_PROCESSING'.")
            update_file_status(session, 'BRONZE_PROCESSING', results.id)  # Proceed with further processing

            # Perform field-level validation
            file_processor.validate(df, results)
            logger.info(f"Field validation completed successfully. Updating file '{results.filename}' status to 'BRONZE_PROCESSED'.")
            update_file_status(session, 'BRONZE_PROCESSED', results.id)  # Mark processing as completed
            return


def start_app():
    """
    Main entry point for executing the database initialization script.
    """
    logger.info("Starting polling thread for data insertion.")
    start_polling_thread(insert_fields_data_in_db)  # Start polling thread for inserting data
    logger.info("Polling thread started successfully.")

    # Start polling for processing files
    poll_table(read_fields_data_in_db)
