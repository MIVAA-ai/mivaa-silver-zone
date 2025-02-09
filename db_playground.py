from utils.db_util import get_session, text
from tabulate import tabulate  # Install this library using pip install tabulate

with get_session() as session:
    # List tables
    try:
        tables = session.execute(text("SHOW TABLES")).fetchall()
        print("Tables in the database:")
        print(tabulate(tables, headers=["Table Name"], tablefmt="grid"))
    except Exception as e:
        print("Error listing tables:", e)

    # Query data
    try:
        result = session.execute(text("SELECT * FROM sql_script_store")).fetchall()
        if result:
            # Fetch column names explicitly using the result object
            headers = result[0]._fields  # Extract column names from the result's metadata
            rows = [tuple(row) for row in result]  # Convert rows to tuples
            print("Contents of sql_script_store:")
            print(tabulate(rows, headers=headers, tablefmt="grid"))
        else:
            print("No data found in sql_script_store.")
    except Exception as e:
        print("Error querying sql_script_store:", e)


    # Query data
    try:
        result = session.execute(text("SELECT * FROM error_messages")).fetchall()
        if result:
            # Fetch column names explicitly using the result object
            headers = result[0]._fields  # Extract column names from the result's metadata
            rows = [tuple(row) for row in result]  # Convert rows to tuples
            print("Contents of error_messages:")
            print(tabulate(rows, headers=headers, tablefmt="grid"))
        else:
            print("No data found in error_messages.")
    except Exception as e:
        print("Error querying error_messages:", e)


    # Query data
    try:
        result = session.execute(text("SELECT * FROM validation_errors")).fetchall()
        if result:
            # Fetch column names explicitly using the result object
            headers = result[0]._fields  # Extract column names from the result's metadata
            rows = [tuple(row) for row in result]  # Convert rows to tuples
            print("Contents of validation_errors:")
            print(tabulate(rows, headers=headers, tablefmt="grid"))
        else:
            print("No data found in validation_errors.")
    except Exception as e:
        print("Error querying validation_errors:", e)


    # # Query data
    # try:
    #     result = session.execute(text("SELECT * FROM files")).fetchall()
    #     if result:
    #         # Fetch column names explicitly using the result object
    #         headers = result[0]._fields  # Extract column names from the result's metadata
    #         rows = [tuple(row) for row in result]  # Convert rows to tuples
    #         print("Contents of files:")
    #         print(tabulate(rows, headers=headers, tablefmt="grid"))
    #     else:
    #         print("No data found in files.")
    # except Exception as e:
    #     print("Error querying files:", e)


    # Query data
    try:
        result = session.execute(text("SELECT * FROM field_bronze_data")).fetchall()
        if result:
            # Fetch column names explicitly using the result object
            headers = result[0]._fields  # Extract column names from the result's metadata
            rows = [tuple(row) for row in result]  # Convert rows to tuples
            print("Contents of field_bronze_data:")
            print(tabulate(rows, headers=headers, tablefmt="grid"))
        else:
            print("No data found in field_bronze_data.")
    except Exception as e:
        print("Error querying field_bronze_data:", e)

    # Query data
    # try:
    #     result = session.execute(text("SELECT * FROM field_silver_data")).fetchall()
    #     if result:
    #         # Fetch column names explicitly using the result object
    #         headers = result[0]._fields  # Extract column names from the result's metadata
    #         rows = [tuple(row) for row in result]  # Convert rows to tuples
    #         print("Contents of field_silver_data:")
    #         print(tabulate(rows, headers=headers, tablefmt="grid"))
    #     else:
    #         print("No data found in field_silver_data.")
    # except Exception as e:
    #     print("Error querying field_silver_data:", e)
