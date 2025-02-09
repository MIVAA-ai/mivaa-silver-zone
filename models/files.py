import os
import logging

from config.logger_config import logger
from utils.checksum_util import calculate_checksum
from utils.generate_sqlalchemy_model import generate_model_for_table

FileModelClass = None
# Generate the SQLAlchemy model class dynamically for the 'files' table
try:
    if FileModelClass is None:
        FileModelClass = generate_model_for_table('files')
        logger.info(f"Generated model class for table: {FileModelClass.__tablename__}")
except Exception as e:
    logger.error(f"Error generating model class for table 'files': {e}")
    # Ensure FileModelClass is defined as None if generation fails

def insert_data(session, filepath, datatype, remarks):
    """
    Inserts a new record into the `files` table using the FileModelClass.
    """
    if FileModelClass is None:
        logger.error("FileModelClass is not defined. Cannot insert data.")
        return None

    logger.info("Insert data called")

    # Extract filename from the provided file path
    filename = os.path.basename(filepath)

    # Calculate checksum for the file
    checksum = calculate_checksum(filepath)

    # Fetch the last ID and increment it
    try:
        last_id = session.query(FileModelClass).order_by(FileModelClass.id.desc()).first()
        new_id = (last_id.id + 1) if last_id else 1
    except Exception as e:
        logger.error(f"Error fetching last ID: {e}")
        new_id = 1

    # Create a new instance of the model
    new_file = FileModelClass(
        id=new_id,
        filename=filename,
        filepath=filepath,
        datatype=datatype,
        checksum=checksum,
        remarks=remarks,
        file_status='PICKED'  # Default status
    )

    try:
        # Add and commit the new record
        session.add(new_file)
        session.commit()
        logger.info(f"Inserted: {filename} with checksum {checksum}")
        return new_file.id
    except Exception as e:
        logger.error(f"Error inserting data into the `files` table: {e}")
        session.rollback()
        return None

def fetch_files_to_process(session):
    """
    Fetches files with status 1 or 2 from the `files` table for processing.
    """
    if FileModelClass is None:
        logger.error("FileModelClass is not defined. Cannot fetch files.")
        return None

    try:
        files_to_process = (
            session.query(FileModelClass)
            .filter(FileModelClass.file_status.in_(['PICKED', 'BRONZE_PROCESSED']))
            # .filter(FileModelClass.file_status.in_(['BRONZE_PROCESSED']))
            .order_by(FileModelClass.file_status.asc())
            .first()
        )
        return files_to_process
    except Exception as e:
        logger.error(f"Error fetching files from table: {e}")
        return None

def update_file_status(session, status, id, remarks=None):
    """
    Updates the status of a file in the `files` table using the FileModelClass.

    :param session: SQLAlchemy session
    :param status: New status to set
    :param id: ID of the file to update
    :param remarks: Optional remarks to add
    """
    if FileModelClass is None:
        logger.error("FileModelClass is not defined. Cannot update file status.")
        return

    try:
        # Query the file record by ID
        file_record = session.query(FileModelClass).filter_by(id=id).first()
        if not file_record:
            logger.warning(f"No file found with ID {id}")
            return

        # Update fields
        file_record.file_status = status
        if remarks is not None:
            file_record.remarks = remarks

        # Commit the changes
        session.commit()
        logger.info(f"Updated file with ID {id} to file_status {status}")
    except Exception as e:
        logger.error(f"Error updating file file_status: {e}")
        session.rollback()
