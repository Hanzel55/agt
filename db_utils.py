import psycopg2
import pyodbc
# utils.py  (or put at top of agent.py)
import datetime
import decimal
import uuid
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype
from pandas_utils import is_uuid_column
def convert_to_string_if_needed(df):
    """Convert columns containing decimal, datetime, uuid, and bytes types to string.
       Works on entire DataFrame columns for better performance.
       If any value in a column is of a specific type, converts the entire column to string.
       Returns the converted DataFrame.
    """
    df = df.copy()  # Work on a copy to avoid modifying original
    
    for column in df.columns:
        # Check if the column contains any non-null values
        non_null_values = df[column].dropna()
        
        if len(non_null_values) > 0:
            # Check if any value in the column is of a specific type
            # Check for decimal/float
            if pd.api.types.is_float_dtype(df[column]):
                df[column] = df[column].astype(str)
                continue
            
            # Check for datetime/date/time
            if is_datetime64_any_dtype(df[column]):
                df[column] = df[column].astype(str)
                continue
            
            # # Check for UUID
            if is_uuid_column(df[column]):
                df[column] = df[column].astype(str)
                continue
            
            # # Check for bytes/bytearray
            # if any(isinstance(val, (bytes, bytearray)) for val in non_null_values[:100]):
            #     df[column] = df[column].apply(lambda x: str(x) if pd.notna(x) else x)
            #     continue
    
    return df


def connect_to_postgres():
    # Connection parameters (replace these with your actual details)
    host = "35.202.253.83"         # Database host (e.g., 'localhost' or an IP)
    port = "5432"              # Database port (default PostgreSQL port is 5432)
    dbname = "C00001"   # Name of your database
    user = "postgres"         # Your PostgreSQL username
    password = "spectra123" # Your PostgreSQL password

    try:
        # Establish connection to the PostgreSQL database
        connection = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )

        # Create a cursor object to interact with the database
        cursor = connection.cursor()
        if cursor:
            print("connected")

        # Define the query to retrieve the first 10 rows from GENERAL.PROJECT
        query = 'SELECT * FROM "GENERAL".src_tables LIMIT 10;'

        # Execute the query
        cursor.execute(query)

        # Fetch the result (10 rows)
        rows = cursor.fetchall()

        # Get column names
        column_names = [desc[0] for desc in cursor.description]
        
        # Load data into DataFrame
        df = pd.DataFrame(rows, columns=column_names)
        
        # Convert columns with special types to string
        df = convert_to_string_if_needed(df)
        
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')

        # Return the list of records
        print("send")
        return records 

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        return None  # Return None in case of an error

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def connect_to_sql_server():
    # Connection parameters (replace these with your actual details)
    server = "10.128.0.5"         # Database server (e.g., 'localhost' or an IP)
    port = "1433"                # Database port (default SQL Server port is 1433)
    database = "SampleDB"          # Name of your database
    username = "SA"              # Your SQL Server username
    password = "Password@4455"  # Your SQL Server password
    driver = "{ODBC Driver 17 for SQL Server}"  # ODBC driver (adjust if needed)

    connection = None
    cursor = None

    try:
        # Build connection string
        connection_string = (
            f"DRIVER={driver};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
        )

        # Establish connection to the SQL Server database
        connection = pyodbc.connect(connection_string)

        # Create a cursor object to interact with the database
        cursor = connection.cursor()
        if cursor:
            print("connected")

        # Define the query to retrieve the first 10 rows
        # Adjust the query based on your SQL Server schema
        query = 'SELECT TOP 10 * FROM INFORMATION_SCHEMA.TABLES;'

        # Execute the query
        cursor.execute(query)

        # Fetch the result (10 rows)
        rows = cursor.fetchall()

        # Get column names
        column_names = [column[0] for column in cursor.description]
        
        # Load data into DataFrame
        df = pd.DataFrame(rows, columns=column_names)
        
        # Convert columns with special types to string
        df = convert_to_string_if_needed(df)
        
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        
        # Return the list of records
        print("send")
        return records 

    except Exception as error:
        print("Error while connecting to SQL Server:", error)
        return None  # Return None in case of an error

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

