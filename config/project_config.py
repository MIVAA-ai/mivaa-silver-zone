# Configuration settings

PROJECT_CONFIG = {
  "IGNORE_BRONZE_WARNING": True,
  "SQL_TABLES": {
    "FIELD": {
      "BRONZE_TABLE": "field_bronze_data",
      "SILVER_TABLE": "field_silver_data"
    }
  },
  "MASTER_DATA_KINDS": {
    "CRS": "osdu:wks:reference-data--CoordinateReferenceSystem:1.1.0",
    "FIELD": "osdu:wks:master-data--Field:1.*.*"
  },
  "WGS84CRS": "{\"authCode\":{\"auth\":\"EPSG\",\"code\":\"4326\"},\"name\":\"GCS_WGS_1984\",\"type\":\"LBC\",\"ver\":\"PE_10_3_1\",\"wkt\":\"GEOGCS[\\\"GCS_WGS_1984\\\",DATUM[\\\"D_WGS_1984\\\",SPHEROID[\\\"WGS_1984\\\",6378137.0,298.257223563]],PRIMEM[\\\"Greenwich\\\",0.0],UNIT[\\\"Degree\\\",0.0174532925199433],AUTHORITY[\\\"EPSG\\\",4326]]\"}"
}

# Define SEARCH_QUERIES separately after PROJECT_CONFIG
SEARCH_QUERIES = {
    "CRS": {
        "kind": PROJECT_CONFIG["MASTER_DATA_KINDS"]["CRS"],
        "returnedFields": [
            "data.PersistableReference"
        ]
    },
    "FIELD": {
        "kind": PROJECT_CONFIG["MASTER_DATA_KINDS"]["FIELD"],
        "returnedFields": [
            "id"
        ]
    }
}

# Add SEARCH_QUERIES back to PROJECT_CONFIG
PROJECT_CONFIG["SEARCH_QUERIES"] = SEARCH_QUERIES



