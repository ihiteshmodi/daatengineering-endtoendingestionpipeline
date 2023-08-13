import pandas as pd
from snowflake.connector import connect

#All the environment variables to be plugged into Lambda function
user = os.environ.get('SNOWFLAKE_USER')
password = os.environ.get('SNOWFLAKE_PASSWORD')
account = os.environ.get('SNOWFLAKE_ACCOUNT')
warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
database = os.environ.get('SNOWFLAKE_DATABASE')
schema = os.environ.get('SNOWFLAKE_SCHEMA')

conn = connect(
    user = os.environ.get('SNOWFLAKE_USER'),
    password = os.environ.get('SNOWFLAKE_PASSWORD'),
    account = os.environ.get('SNOWFLAKE_ACCOUNT'),
    warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE'),
    database = os.environ.get('SNOWFLAKE_DATABASE'),
    schema = os.environ.get('SNOWFLAKE_SCHEMA')
)

#the def ingest_into_snowflake fucntion shall be sued fo ingestion of metadata only, the main dat ais big and should be kept in the staging S3 directory

def ingest_into_snowflake(df, table_to_ingest_into):
    try:
        # Replace placeholders with your table and columns names
        table_name = table_to_ingest_into

        # Upload data to the table
        df.to_sql(table_name, conn, index=False, if_exists='replace')
    except:
        print('Error while uploading data into Snowflake.')

def execute_stored_procedure(Procedure_name):
    try:    
        # Create a cursor
        cursor = conn.cursor()

        # Execute the stored procedure
        cursor.execute(f"CALL {Procedure_name}()")
        
        # Close the cursor and connection
        cursor.close()
        conn.close()
    except:
        raise Exception("Failed to run Stored Procedure.")