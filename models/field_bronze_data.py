import logging

from sqlalchemy import func, case
from sqlalchemy.orm import aliased

from config.logger_config import logger
from config.project_config import PROJECT_CONFIG
from models.error_messages import ErrorMessagesModel
from models.validation_errors import ValidationErrorsModel
from utils.db_util import get_session
from datetime import datetime
import pandas as pd
from utils.generate_sqlalchemy_model import generate_model_for_table

FieldBronzeTableModel = None
# Generate the SQLAlchemy model class dynamically for the 'field_bronze_table' table
try:
    if FieldBronzeTableModel is None:
        FieldBronzeTableModel = generate_model_for_table(PROJECT_CONFIG["SQL_TABLES"]["FIELD"]["BRONZE_TABLE"])
        logger.info(f"Generated model class for table: {FieldBronzeTableModel.__tablename__}")
except Exception as e:
    logger.error(f"Error generating model class for table 'field_bronze_table': {e}")
    # Ensure FieldBronzeTableModel is defined as None if generation fails

def log_field_bronze_table(df: pd.DataFrame, file_id: int):
    """
    Logs validation status for each row in the database into the 'field_bronze_table'.

    Parameters:
    - df (pd.DataFrame): DataFrame containing the data to log.
    - file_id (int): ID of the file being processed.
    - error_index_set (set): Set of indices that failed validation.
    """
    if FieldBronzeTableModel is None:
        logger.error("FieldBronzeTableModel is not defined. Cannot log data.")
        return

    with get_session() as session:
        try:
            # Determine the starting ID for the new rows
            max_id = session.query(FieldBronzeTableModel.id).order_by(FieldBronzeTableModel.id.desc()).first()
            max_id = max_id[0] if max_id else 0

            # Add required columns to the DataFrame
            df["id"] = range(max_id + 1, max_id + 1 + len(df))
            df["row_index"] = df.index
            df["file_id"] = file_id
            df["validation_timestamp"] = datetime.now()

            # Convert NaN values to None explicitly
            df = df.astype(object).where(pd.notna(df), None)

            # Convert the DataFrame to a list of dictionaries
            data_to_insert = df.to_dict(orient="records")

            # Use bulk_insert_mappings for efficient insertion
            session.bulk_insert_mappings(FieldBronzeTableModel, data_to_insert)
            session.commit()
            logger.info("Validation results logged successfully.")
        except Exception as e:
            logger.error(f"Error logging validation results: {e}")
            session.rollback()

def fetch_bronze_results_by_file_id(file_id):
    """
    Fetches records from the 'field_bronze_table' table for a specific file ID and groups them by 'FieldName'.

    Parameters:
    - file_id (int): ID of the file to fetch data for.

    Returns:
    - pd.DataFrame: A DataFrame containing the grouped records, or an empty DataFrame if no records are found.
    """
    if FieldBronzeTableModel is None:
        logger.error("FieldBronzeTableModel is not defined. Cannot fetch data.")
        return pd.DataFrame()

    with get_session() as session:
        try:
            # Query the table for the specified file_id
            # Build SQLAlchemy query to fetch results
            ValidationErrorsAlias = aliased(ValidationErrorsModel)
            ErrorMessagesAlias = aliased(ErrorMessagesModel)
            query = (
                session.query(
                    FieldBronzeTableModel.id,
                    FieldBronzeTableModel.row_index,
                    FieldBronzeTableModel.file_id,
                    FieldBronzeTableModel.FieldName,
                    FieldBronzeTableModel.FieldType,
                    FieldBronzeTableModel.DiscoveryDate,
                    FieldBronzeTableModel.X,
                    FieldBronzeTableModel.Y,
                    FieldBronzeTableModel.CRS,
                    FieldBronzeTableModel.Source,
                    FieldBronzeTableModel.ParentFieldName,
                    FieldBronzeTableModel.validation_timestamp,
                    func.group_concat(ErrorMessagesAlias.error_message, ', ').label("error_message"),
                    # Determine error_severity: show "ERROR" if any error exists, otherwise "WARNING"
                    case(
                        (func.sum(case((ErrorMessagesAlias.error_severity == 'ERROR', 1), else_=0)) > 0, 'ERROR'),
                        (func.sum(case((ErrorMessagesAlias.error_severity == 'WARNING', 1), else_=0)) > 0, 'WARNING'),
                        else_=''  # Return empty string when there are no warnings or errors
                    ).label("error_severity")
                )
                .outerjoin(
                    ValidationErrorsAlias,
                    (ValidationErrorsAlias.zone == "BRONZE") &
                    (FieldBronzeTableModel.row_index == ValidationErrorsAlias.row_index) &
                    (FieldBronzeTableModel.file_id == ValidationErrorsAlias.file_id)

                )
                .outerjoin(
                    ErrorMessagesAlias,
                    ValidationErrorsAlias.error_code == ErrorMessagesAlias.error_code
                )
                .filter(FieldBronzeTableModel.file_id == file_id)
                .group_by(
                    FieldBronzeTableModel.id,
                    FieldBronzeTableModel.row_index,
                    FieldBronzeTableModel.file_id,
                    FieldBronzeTableModel.FieldName,
                    FieldBronzeTableModel.FieldType,
                    FieldBronzeTableModel.DiscoveryDate,
                    FieldBronzeTableModel.X,
                    FieldBronzeTableModel.Y,
                    FieldBronzeTableModel.CRS,
                    FieldBronzeTableModel.Source,
                    FieldBronzeTableModel.ParentFieldName,
                    FieldBronzeTableModel.validation_timestamp,
                )
                .order_by(FieldBronzeTableModel.id)

            )

            query = query.filter(FieldBronzeTableModel.file_id == file_id)
            return pd.DataFrame(query.all(), columns=[col["name"] for col in query.column_descriptions])

        except Exception as e:
            logger.error(f"Error fetching records for file_id {file_id}: {e}")
            return pd.DataFrame()
