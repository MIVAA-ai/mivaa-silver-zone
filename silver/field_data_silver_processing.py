import json
import os
import pandas as pd

from config.logger_config import logger
from config.project_config import PROJECT_CONFIG
from models.field_bronze_data import fetch_bronze_results_by_file_id
from models.field_silver_data import log_field_silver_table, fetch_silver_results_by_file_id
from models.validation_errors import log_errors_to_db
from osdu.osdu_client import OSDUClient

client = OSDUClient()


def log_and_save_results(df, file_id, file_name, validation_errors):
    """
    Log validation results, store them in the silver database, and save them as a CSV file.

    Parameters:
    - df (DataFrame): The processed data to be logged.
    - file_id (int): The unique file identifier.
    - file_name (str): The name of the input file.
    - validation_errors (list): List of validation errors encountered.

    Returns:
    - None
    """
    try:
        # Log validation results in the silver table
        log_field_silver_table(df)
        log_errors_to_db(validation_errors, file_id, "SILVER")

        # Fetch results from the silver table
        result_df = fetch_silver_results_by_file_id(file_id)

        # Ensure output directory exists
        output_dir = PROJECT_CONFIG.get("OUTPUT_DIRECTORY", "output")
        os.makedirs(output_dir, exist_ok=True)

        # Save the processed data to a CSV file
        result_file = f"{output_dir}/{file_name}_silver_data_results.csv"
        result_df.to_csv(result_file, index=False)
        logger.info(f"Results saved to '{result_file}'.")
    except Exception as e:
        logger.error(f"Error logging and saving results: {e}")


def get_geojson(coord_list):
    coords = [[d["x"], d["y"]] for d in coord_list]
    return json.dumps({
        "type": "geometrycollection",
        "geometries": [
            {
                "type": "polygon",
                "coordinates": [coords]
            }
        ]
    })

def fetch_and_filter_bronze_data(file_id):
    """
    Fetch field data from the bronze table and filter out rows based on severity.

    Parameters:
    - file_id (int): The unique file identifier.

    Returns:
    - DataFrame: The filtered data.
    """
    df = fetch_bronze_results_by_file_id(file_id)

    # Apply filtering based on configuration
    if PROJECT_CONFIG["IGNORE_BRONZE_WARNING"]:
        df = df[df['error_severity'].fillna('') != 'ERROR']
    else:
        df = df[~df['error_severity'].fillna('').isin(['ERROR', 'WARNING'])]

    # Log if no data is available after filtering
    if df.empty:
        logger.warning(f"No valid data found for file ID {file_id}. Processing stopped.")

    return df


def get_crs_reference(crs_value: str, row_index: int, validation_errors: list):
    """
    Retrieve the CRS persistable reference from the OSDU client.

    Parameters:
    - crs_value (str): The CRS value from the dataset.
    - row_index (int): Row index for error tracking.
    - validation_errors (list): List to store validation errors.

    Returns:
    - dict | None: A dictionary containing CRS info if found, otherwise None.
    """

    if not crs_value:
        # No CRS value provided, so we have nothing to look up
        return None

    payload = {
        "kind": PROJECT_CONFIG["MASTER_DATA_KINDS"]["CRS"],
        "returnedFields": [
            "kind",
            "data.PersistableReference",
            "data.Name",
            "id"
        ],
        "limit": 1,
        "offset": 0,
        "query": f'data.ID:"{crs_value}"'
    }

    try:
        result = client.search(payload)
        if not result.get('results'):
            # No results found
            validation_errors.append({
                "row_index": str(row_index),
                "field_name": "CRS",
                "error_type": "row_validation",
                "error_code": "crs_not_found"
            })
            return None

        # If we got here, there's at least one matching record
        record = result['results'][0]
        data = record['data']

        return {
            "kind": record["kind"],
            "name": data["Name"],
            "persistableReference": data["PersistableReference"],
            "coordinateReferenceSystemID": record["id"]
        }

    except Exception as e:
        # Catch and record any unexpected exceptions from the client search
        validation_errors.append({
            "row_index": str(row_index),
            "field_name": "CRS",
            "error_type": "row_validation",
            "error_code": "crs_not_found",
            "error_message": str(e)
        })
        return None


def convert_coordinates(persistable_reference, coordinates, index, validation_errors):
    """
    Convert given coordinates to WGS84 format using the OSDU client.

    Parameters:
    - persistable_reference (str): Reference ID for CRS conversion.
    - coordinates (list): List of coordinate dictionaries.
    - index (int): Row index for error tracking.
    - validation_errors (list): List to store validation errors.

    Returns:
    - list: List of converted coordinates, or an empty list if conversion fails.
    """
    if not persistable_reference:
        return None

    try:
        return client.crs_converter(persistable_reference, PROJECT_CONFIG["TO_CRS"], coordinates)['points']
    except Exception as e:
        validation_errors.append({
            "row_index": str(index),
            "field_name": "Wgs84Coordinates",
            "error_type": "row_validation",
            "error_type": "row_validation",
            "error_code": "crs_conversion_error"
        })
        return None

def get_search_field_query(field_name):
    """
      Constructs a search query payload for fetching field details from OSDU.

      This function creates a query to search for a specific field by its `FieldName`
      in the OSDU system using the configured `MASTER_DATA_KINDS["FIELD"]` kind.

      Parameters:
      - field_name (str): The name of the field to search for in OSDU.

      Returns:
      - dict: A structured payload for the OSDU search API.
      """
    return {
        "kind": PROJECT_CONFIG["MASTER_DATA_KINDS"]["FIELD"],
        "returnedFields": [
            "id"
        ],
        'limit': 1,
        'offset': 0,
        'query': f'data.FieldName:"{field_name}"'
    }

def get_parent_field_id(parent_field_name, index, validation_errors):
    """
    Retrieve the OSDU ID of the parent field.

    Parameters:
    - parent_field_name (str): Name of the parent field.
    - index (int): Row index for error tracking.
    - validation_errors (list): List to store validation errors.

    Returns:
    - str: The OSDU ID of the parent field, or None if not found.
    """
    if not parent_field_name:
        return None

    try:
        result = client.search(get_search_field_query(parent_field_name))
        if len(result['results']) > 1:
            logger.warning(f"Multiple parent fields found for {parent_field_name}. Using the first match.")
        return result['results'][0]['id']
    except Exception as e:
        validation_errors.append({
            "row_index": str(index),
            "field_name": "ParentFieldOSDUId",
            "error_type": "row_validation",
            "error_code": "parent_field_not_found"
        })
        return None


def check_field_name_exists(field_name, index, validation_errors):
    """
    Check if a given field name already exists in OSDU.

    Parameters:
    - field_name (str): The name of the field to check.
    - index (int): The row index for error tracking.
    - validation_errors (list): List to store validation errors.

    Returns:
    - None
    """
    try:
        result = client.search(get_search_field_query(field_name))  # Fetch only 1 result for efficiency

        # Ensure results exist and contain at least one entry
        if result.get('results') and len(result['results']) > 0:
            field_id = result['results'][0].get('id')  # Use `.get()` to avoid KeyError
            if field_id:
                logger.error(f"Field '{field_name}' already exists in OSDU with ID '{field_id}'.")

                # Append to validation errors
                validation_errors.append({
                    "row_index": str(index),
                    "field_name": "FieldName",
                    "error_type": "data_validation",
                    "error_code": "field_already_exists"
                })
    except IndexError:
        logger.warning(f"No results found while checking field '{field_name}' in OSDU.")
    except KeyError as e:
        logger.error(f"Unexpected response structure while checking field '{field_name}': {e}")
    except Exception as e:
        logger.error(f"Error checking field existence for '{field_name}': {e}")


def process_single_field(field_name, group, index, file_id, column_list, validation_errors):
    """
    Process a single field group, including coordinate conversion and parent lookup.

    Parameters:
    - field_name (str): Name of the field being processed.
    - group (DataFrame): Grouped data for the field.
    - index (int): Row index for tracking.
    - file_id (int): File ID for logging.
    - column_list (list): List of additional columns to include.
    - validation_errors (list): List to store validation errors.

    Returns:
    - dict: Processed data for the field.
    """
    # ✅ Check if the field name already exists in OSDU
    check_field_name_exists(field_name, index, validation_errors)

    parent_field_name = group['ParentFieldName'].dropna().iloc[0] if 'ParentFieldName' in group.columns and not group[
        'ParentFieldName'].dropna().empty else None

    coordinates = [
        {"x": row["X"], "y": row["Y"], "z": 0}
        for _, row in group.iterrows() if pd.notna(row["X"]) and pd.notna(row["Y"])
    ]

    crs_value = group["CRS"].iloc[0] if "CRS" in group.columns and pd.notna(group["CRS"].iloc[0]) else None
    crs_reference = get_crs_reference(crs_value, index, validation_errors)

    wgs84_coordinates = None
    if crs_reference:
        wgs84_coordinates = convert_coordinates(crs_reference['persistableReference'], coordinates, index, validation_errors)

    ingested_polygon = None
    if coordinates:
        ingested_polygon = get_geojson(coordinates)

    wgs84_polygon = None
    if wgs84_coordinates:
        wgs84_polygon = get_geojson(wgs84_coordinates)

        print(json.dumps(get_geojson(wgs84_coordinates), indent=2))
    data_entry = {
        "row_index": str(index),
        "file_id": file_id,
        "AsIngestedCoordinates": ingested_polygon,
        "Wgs84Coordinates": wgs84_polygon
    }

    data_entry["ParentFieldOSDUId"] = get_parent_field_id(parent_field_name, index, validation_errors)

    for column in column_list:
        data_entry[column] = group[column].iloc[0] if column in group.columns else None

    data_entry["CRS"] = json.dumps(crs_reference) if crs_reference else None

    return data_entry


def process_field_data_for_silver_zone(file_id, file_name, column_list):
    """
    Processes field data from bronze and transforms it for the silver zone.

    Parameters:
    - file_id (int): File ID for processing.
    - file_name (str): Name of the input file.
    - column_list (list): List of columns to be included.

    Returns:
    - list: Processed field data.
    """
    validation_errors = []
    df = fetch_and_filter_bronze_data(file_id)

    if df.empty:
        return []

    processed_data = [
        process_single_field(field_name, group, index, file_id, column_list, validation_errors)
        for index, (field_name, group) in enumerate(df.groupby('FieldName', as_index=False), start=0)
    ]

    log_and_save_results(pd.DataFrame(processed_data), file_id, file_name, validation_errors)
    return processed_data
