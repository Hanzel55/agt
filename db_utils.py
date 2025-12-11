import psycopg2
import pyodbc
# utils.py  (or put at top of agent.py)
import datetime
import decimal
import uuid
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype
from pandas_utils import is_uuid_column
def convert_to_string_if_needed(df , convert_columns):
    """Convert columns containing decimal, datetime, uuid, and bytes types to string.
       Works on entire DataFrame columns for better performance.
       If any value in a column is of a specific type, converts the entire column to string.
       Returns the converted DataFrame.
    """
    df = df.copy()  # Work on a copy to avoid modifying original
    
    for column in convert_columns:
        df[column] = df[column].astype(str)
    
    return df


def connect_to_postgres(query):
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
        # query = 'SELECT * FROM "GENERAL".src_tables LIMIT 10;'

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


def connect_to_sql_server(query,tbl_name):
    # Connection parameters (replace these with your actual details)
    server = "127.0.0.1"         # Database server (e.g., 'localhost' or an IP)
    # server = "10.128.0.5"         # Database server (e.g., 'localhost' or an IP)
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
        # query = 'SELECT TOP 10 * FROM INFORMATION_SCHEMA.TABLES;'

        # Define the table name and schema
        schema = 'dbo'  # You can change this to your specific schema (e.g., 'schema_name')
        table_name = tbl_name

        # SQL query to get column metadata from INFORMATION_SCHEMA.COLUMNS
        sql_query = f"""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    CASE 
                        WHEN DATA_TYPE IN ('varchar', 'nvarchar') 
                            THEN DATA_TYPE + '(' + 
                                    CASE 
                                        WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN 'max'  -- Explicitly handle MAX types
                                        ELSE CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR(10)) 
                                    END + ')'
                        WHEN DATA_TYPE IN ('binary', 'varbinary') 
                            THEN DATA_TYPE + '(' + 
                                    CASE 
                                        WHEN CHARACTER_OCTET_LENGTH = -1 THEN 'max'  -- Handle MAX types for binary
                                        ELSE CAST(CHARACTER_OCTET_LENGTH AS VARCHAR(10)) 
                                    END + ')'
                        WHEN DATA_TYPE IN ('decimal', 'numeric') 
                            THEN DATA_TYPE + '(' + CAST(NUMERIC_PRECISION AS VARCHAR(10)) + ',' + CAST(NUMERIC_SCALE AS VARCHAR(10)) + ')'
                        ELSE DATA_TYPE 
                    END AS DataTypeWithSize
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table_name}'
                AND TABLE_SCHEMA = '{schema}';
        """

        # Execute the query
        cursor.execute(sql_query)

        # Fetch the results
        columns = cursor.fetchall()

        # Prepare the metadata as a list of dictionaries
        column_metadata = []
        for column in columns:
            column_info = {
                'Column Name': column.COLUMN_NAME,
                'Data Type': column.DATA_TYPE,
                'Data Type With Size': column.DataTypeWithSize,
            }
            column_metadata.append(column_info)

        convert_columns = []
        for column in column_metadata:
            if column['Data Type'] in ['float','decimal','numeric','binary','varbinary','real','uniqueidentifier','datetime','smalldatetime','date','time','datetime2','datetimeoffset']:
                convert_columns.append(column['Column Name'])  
                # Execute the query
        cursor.execute(query)

        # Fetch the result (10 rows)
        rows = cursor.fetchall()
        # Get column names
        column_names = [column[0] for column in cursor.description]
        
        # Convert pyodbc.Row objects to lists to ensure proper DataFrame creation
        # This fixes the "Shape of passed values" error
        rows_list = [list(row) for row in rows]
        
        # Load data into DataFrame
        df = pd.DataFrame(rows_list, columns=column_names)
        
        # Convert columns with special types to string
        df = convert_to_string_if_needed(df,convert_columns)
        
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')

        
        # Return the list of records
        print("send")
        return None, records , column_metadata  # firet one is error, so if records and column_metadata then error is None

    except Exception as error:
        print("Error while connecting to SQL Server:", error)
        return str(error), None , None  # Return None in case of an error and column_metadata is None

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

