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

FieldSilverTableModel = None
# Generate the SQLAlchemy model class dynamically for the 'field_bronze_table' table
try:
    if FieldSilverTableModel is None:
        FieldSilverTableModel = generate_model_for_table(PROJECT_CONFIG["SQL_TABLES"]["FIELD"]["SILVER_TABLE"])
        logger.info(f"Generated model class for table: {FieldSilverTableModel.__tablename__}")
except Exception as e:
    logger.error(f"Error generating model class for table 'field_silver_data': {e}")
    # Ensure FieldSilverTableModel is defined as None if generation fails


def log_field_silver_table(df):
    if not FieldSilverTableModel:
        logger.error("FieldSilverTableModel is not defined. Cannot log data.")
        return

    if df.empty:
        logger.warning("DataFrame is empty. Nothing to log.")
        return

    with get_session() as session:
        try:
            # Determine the starting ID for new rows
            max_id = session.query(FieldSilverTableModel.id).order_by(FieldSilverTableModel.id.desc()).first()
            max_id = max_id[0] if max_id else 0

            # Ensure primary key starts from 1
            df["id"] = range(max_id + 1, max_id + 1 + len(df))
            df["validation_timestamp"] = datetime.now()

            # Convert NaN values to None explicitly
            df = df.astype(object).where(pd.notna(df), None)

            # Convert DataFrame to list of dictionaries
            data_to_insert = df.to_dict(orient="records")

            if data_to_insert:
                # Efficient bulk insert
                session.bulk_insert_mappings(FieldSilverTableModel, data_to_insert)
                session.commit()
                logger.info("Results for Silver Zone logged successfully.")
            else:
                logger.warning("No valid data to insert.")

        except Exception as e:
            logger.error(f"Error logging validation results: {e}")
            session.rollback()


def fetch_silver_results_by_file_id(file_id):
    """
    Fetches records from the 'field_bronze_table' table for a specific file ID and groups them by 'FieldName'.

    Parameters:
    - file_id (int): ID of the file to fetch data for.

    Returns:
    - pd.DataFrame: A DataFrame containing the grouped records, or an empty DataFrame if no records are found.
    """
    if FieldSilverTableModel is None:
        logger.error("FieldSilverTableModel is not defined. Cannot fetch data.")
        return pd.DataFrame()

    with get_session() as session:
        try:
            # Query the table for the specified file_id
            # Build SQLAlchemy query to fetch results
            ValidationErrorsAlias = aliased(ValidationErrorsModel)
            ErrorMessagesAlias = aliased(ErrorMessagesModel)
            query = (
                session.query(
                    FieldSilverTableModel.id,
                    FieldSilverTableModel.row_index,
                    FieldSilverTableModel.file_id,
                    FieldSilverTableModel.FieldName,
                    FieldSilverTableModel.FieldType,
                    FieldSilverTableModel.Source,
                    FieldSilverTableModel.DiscoveryDate,
                    FieldSilverTableModel.ParentFieldName,
                    FieldSilverTableModel.ParentFieldOSDUId,
                    FieldSilverTableModel.AsIngestedCoordinates,
                    FieldSilverTableModel.Wgs84Coordinates,
                    FieldSilverTableModel.CRS,
                    FieldSilverTableModel.validation_timestamp,
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
                    (ValidationErrorsAlias.zone == "SILVER") &
                    (FieldSilverTableModel.row_index == ValidationErrorsAlias.row_index) &
                    (FieldSilverTableModel.file_id == ValidationErrorsAlias.file_id)

                )
                .outerjoin(
                    ErrorMessagesAlias,
                    ValidationErrorsAlias.error_code == ErrorMessagesAlias.error_code
                )
                .filter(FieldSilverTableModel.file_id == file_id)
                .group_by(
                    FieldSilverTableModel.id,
                    FieldSilverTableModel.row_index,
                    FieldSilverTableModel.file_id,
                    FieldSilverTableModel.FieldName,
                    FieldSilverTableModel.FieldType,
                    FieldSilverTableModel.Source,
                    FieldSilverTableModel.DiscoveryDate,
                    FieldSilverTableModel.ParentFieldName,
                    FieldSilverTableModel.ParentFieldOSDUId,
                    FieldSilverTableModel.AsIngestedCoordinates,
                    FieldSilverTableModel.Wgs84Coordinates,
                    FieldSilverTableModel.CRS,
                    FieldSilverTableModel.validation_timestamp,
                )
                .order_by(FieldSilverTableModel.id)

            )

            query = query.filter(FieldSilverTableModel.file_id == file_id)
            return pd.DataFrame(query.all(), columns=[col["name"] for col in query.column_descriptions])

        except Exception as e:
            logger.error(f"Error fetching records for file_id {file_id}: {e}")
            return pd.DataFrame()