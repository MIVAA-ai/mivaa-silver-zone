import os
import traceback
from datetime import datetime

import pandas as pd
import pandera as pa
from pandera.typing import Series

from config.logger_config import logger
from config.project_config import PROJECT_CONFIG
from models.field_bronze_data import log_field_bronze_table, fetch_bronze_results_by_file_id
from models.validation_errors import log_errors_to_db
from utils.generate_pandera_schema import generate_pandera_class_from_table_info

# List to store validation errors
validation_errors = []
error_index = []

def integrate_custom_checks(table_name, class_name="DynamicFieldSchema"):
    """
    Generate Pandera schema with custom validation checks.
    """
    # Generate schema code dynamically
    schema_code = generate_pandera_class_from_table_info(table_name, class_name)
    exec_globals = {"pa": pa, "Series": Series, "pd": pd, "datetime": datetime}
    exec(schema_code, exec_globals)
    base_schema_class = exec_globals[class_name]

    # Define custom checks as methods in a subclass
    class CustomDynamicFieldSchema(base_schema_class):
        # Validate DiscoveryDate is <= today
        @pa.dataframe_check
        def validate_discovery_date(cls, df: pd.DataFrame) -> bool:
            """Check if DiscoveryDate is not in the future."""
            today = pd.Timestamp(datetime.now().date())
            invalid_rows = df[df["DiscoveryDate"] > today]
            if not invalid_rows.empty:
                for idx in invalid_rows.index:
                    error_index.append(idx)
                    validation_errors.append({
                        "row_index": str(idx),
                        "field_name": "DiscoveryDate",
                        "error_type": "row_validation",
                        "error_code": "future_discovery_date"
                    })
            return True

        # Ensure FieldType and DiscoveryDate are consistent for each FieldName
        @pa.dataframe_check
        def validate_consistency(cls, df: pd.DataFrame) -> bool:
            """Check consistency of FieldType and DiscoveryDate within FieldName."""
            for fieldname, group in df.groupby("FieldName"):
                if group["FieldType"].nunique() > 1 or group["DiscoveryDate"].nunique() > 1:
                    error_index.extend(group.index.tolist())
                    for idx in group.index.tolist():
                        validation_errors.append({
                            "row_index": str(idx),
                            "field_name": fieldname,
                            "error_type": "group_validation",
                            "error_code": "Inconsistent_field_data"
                        })
            return True

        # Validate Polygon Completeness (X, Y, CRS must all be present or null)
        @pa.dataframe_check
        def validate_polygon_completeness(cls, df: pd.DataFrame) -> bool:
            """Ensure X, Y, CRS are either all present or all null."""
            for fieldname, group in df.groupby("FieldName"):
                condition = (
                        (group["X"].isnull() == group["Y"].isnull()) &
                        (group["Y"].isnull() == group["CRS"].isnull())
                )
                if not condition.all():
                    error_index.extend(group.index.tolist())
                    for idx in group.index.tolist():
                        validation_errors.append({
                            "row_index": str(idx),
                            "field_name": fieldname,
                            "error_type": "group_validation",
                            "error_code": "polygon_incomplete"
                        })
            return True

        # Validate Polygon Closure (First and last X, Y must match)
        @pa.dataframe_check
        def validate_polygon_closure(cls, df: pd.DataFrame) -> bool:
            """Ensure the first and last coordinates of a polygon match."""
            for fieldname, group in df.groupby("FieldName"):
                group = group.dropna(subset=["X", "Y"])
                if len(group) >= 2 and not (
                        (group.iloc[0]["X"] == group.iloc[-1]["X"]) and
                        (group.iloc[0]["Y"] == group.iloc[-1]["Y"])
                ):
                    error_index.extend(group.index.tolist())
                    for idx in group.index.tolist():
                        validation_errors.append({
                            "row_index": str(idx),
                            "field_name": fieldname,
                            "error_type": "group_validation",
                            "error_code": "polygon_not_closed"
                        })
            return True

    return CustomDynamicFieldSchema


def log_and_save_results(df, file_id, file_name, validation_errors):
    """Log validation results and save to CSV."""
    try:
        # Log validation results
        log_field_bronze_table(df, file_id)
        log_errors_to_db(validation_errors, file_id, "BRONZE")
        result_df = fetch_bronze_results_by_file_id(file_id)

        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        result_df.to_csv(f"{output_dir}/{file_name}_validation_results.csv", index=False)
        logger.info(f"Results saved to '{output_dir}/{file_name}_validation_results.csv'.")
    except Exception as e:
        logger.error(f"Error logging and saving results: {e}")

def validate_field(df, file_id, file_name):
    """Main function to validate data."""
    try:
        DynamicFieldSchema = integrate_custom_checks(PROJECT_CONFIG["SQL_TABLES"]["FIELD"]["BRONZE_TABLE"])
        # Convert DiscoveryDate to datetime with dayfirst=True
        df['DiscoveryDate'] = pd.to_datetime(df['DiscoveryDate'], errors='coerce', dayfirst=True)

        DynamicFieldSchema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as e:
        validation_errors.extend(
            {
                "row_index": error.get("index"),
                "field_name": error.get("column"),
                "error_type": "row_validation",
                "error_code": error.get("check"),
            }
            for _, error in e.failure_cases.iterrows()
        )
        logger.warning("Validation schema errors detected.")
    except Exception as ex:
        logger.error(f"Unexpected error during validation: {traceback.format_exc()}")
    finally:
        log_and_save_results(df, file_id, file_name, validation_errors)
        validation_errors.clear()
        error_index.clear()
