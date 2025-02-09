import os

from shapely import Polygon

from config.logger_config import logger
from config.project_config import PROJECT_CONFIG
from models.field_bronze_data import fetch_bronze_results_by_file_id
import pandas as pd

from models.field_silver_data import log_field_silver_table, fetch_silver_results_by_file_id
from models.validation_errors import log_errors_to_db
from osdu.osdu_client import OSDUClient
client = OSDUClient()

import pandas as pd

def log_and_save_results(df, file_id, file_name, validation_errors):
    """Log validation results and save to CSV."""
    try:

        # Log validation results
        log_field_silver_table(df)
        log_errors_to_db(validation_errors, file_id, "SILVER")
        result_df = fetch_silver_results_by_file_id(file_id)

        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        result_df.to_csv(f"{output_dir}/{file_name}_silver_data_results.csv", index=False)
        logger.info(f"Results saved to '{output_dir}/{file_name}_silver_data_results.csv'.")
    except Exception as e:
        logger.error(f"Error logging and saving results: {e}")

def process_field_data_for_silver_zone(fileId, file_name, column_list):
    """
    Processes field data fetched from the bronze table for the silver zone.

    Parameters:
    - fileId (int): ID of the file to fetch and process data for.
    - column_list (list): List of column names to include in the output.

    Returns:
    - List[Dict]: A list of dictionaries formatted as required.
    """

    validation_errors = []

    # Fetch data from the bronze table
    df = fetch_bronze_results_by_file_id(fileId)

    # Filter out rows with 'ERROR' severity
    if PROJECT_CONFIG["IGNORE_BRONZE_WARNING"]:
        df = df[df['error_severity'].fillna('') != 'ERROR']
    else:
        df = df[~df['error_severity'].fillna('').isin(['ERROR', 'WARNING'])]

    if df.empty:
        return []

    # Ensure all columns are displayed for debugging
    pd.set_option('display.max_columns', None)

    # Process grouped data into the required format
    processed_data = []

    for index, (field_name, group) in enumerate(df.groupby('FieldName'), start=0):
        parent_field_name = None
        if 'ParentFieldName' in group.columns and not group['ParentFieldName'].dropna().empty:
            parent_field_name = group['ParentFieldName'].iloc[0]

        # Process coordinates, ensuring NaN values are handled
        coordinates = [
            {"x": row["X"], "y": row["Y"], "z": 0}
            for _, row in group.iterrows() if pd.notna(row["X"]) and pd.notna(row["Y"])
        ]

        # Handle CRS conversion
        query = f'data.ID:"{group["CRS"].iloc[0]}"' if "CRS" in group.columns else None
        PersistableReference = None
        wgs84_coordinates = []
        if query and group["CRS"].iloc[0]:
            try:
                result = client.search("CRS", query, 1)
                PersistableReference = result['results'][0]['data']['PersistableReference']
            except Exception as e:
                validation_errors.append({
                    "row_index": str(index),
                    "field_name": "CRS",
                    "error_type": "row_validation",
                    "error_code": "crs_not_found"
                })

        if PersistableReference:
            try:
                wgs84_coordinates = client.crs_converter(PersistableReference, coordinates)['points']
            except Exception as e:
                validation_errors.append({
                    "row_index": str(index),
                    "field_name": "Wgs84Coordinates",
                    "error_type": "row_validation",
                    "error_code": "crs_conversion_error"
                })

        # Construct data dictionary
        data_entry = {
            "row_index": str(index),
            "file_id": fileId,
            "AsIngestedCoordinates": coordinates,
            "Wgs84Coordinates": wgs84_coordinates
        }

        # Handle ParentField lookup
        if parent_field_name:
            try:
                parent_query = f'data.FieldName:"{parent_field_name}"'
                result = client.search("FIELD", parent_query, 1)
                data_entry["ParentFieldOSDUId"] = result['results'][0]['id']
            except Exception as e:
                validation_errors.append({
                    "row_index": str(index),
                    "field_name": "ParentFieldOSDUId",
                    "error_type": "row_validation",
                    "error_code": "parent_field_not_found"
                })

        # Add additional requested columns
        for column in column_list:
            data_entry[column] = group[column].iloc[0] if column in group.columns else None

        processed_data.append(data_entry)
    print(processed_data)
    print(validation_errors)
    log_and_save_results(pd.DataFrame(processed_data), fileId, file_name, validation_errors)
    validation_errors.clear()
    return processed_data

