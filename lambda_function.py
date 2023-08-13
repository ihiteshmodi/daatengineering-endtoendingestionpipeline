import boto3
import pandas as pd
from ETL_function import ETLFunction
from snowflake_ingestion import ingest_into_snowflake, execute_stored_procedure

def lambda_handler(event, context):
    try:
        # Extract S3 bucket, file path, and template path from the event
        s3_bucket = event['s3Bucket']
        s3_key = event['s3Key']
        s3_template = "<Path to your ETL Template as the Eventbridge will return only s3 file location>"
        snowflake_statging_bucket_path = "<Path to your S3 location that is used as a stage for snowflake>"
        file_name = s3_key.str_split["/"][-1] #retaining the original file name
        final_file_name_with_staging_bucket_path = os.path.join(snowflake_statging_bucket_path, file_name)

        # Initialize S3 client
        s3 = boto3.client('s3')

        # Read S3 object contents
        s3_object = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        file_content = s3_object['Body'].read().decode('utf-8')

        # Convert file content to Pandas DataFrame
        raw_data = pd.read_csv(pd.compat.StringIO(file_content))
        template = pd.read_csv(s3_template)

        # Initialize ETLFunction with template and raw data
        etl_function = ETLFunction(template, raw_data)

        # Apply ETL steps sequentially
        etl_function.convert_to_numeric() \
                    .drop_null_columns() \
                    .convert_date_columns() \
                    .create_fileid() \
                    .rename_columns() \
                    .create_hash_key() \
                    .group_by() \
                    .keep_required_columns() \
                    .update_metadata()
        
        
        #exporting dataframe to staging folder
        raw_data.to_csv(final_file_name_with_staging_bucket_path, index = False)
        print("Save raw data to S3 bucket")

        #saving the metadata in snowflake table
        ingest_into_snowflake(etl_function.metadata_df, "FILE_LOG")

        #executing the stored procedure to transfer data from staging to main table
        execute_stored_procedure("INGEST_AND_UPDATE_COUNTS")
        
        #Now, We will delete all the objects in the staging tbale path so the staging table is empty and ready for new ingestions
        # List all objects in the folder
        objects = s3.list_objects_v2(Bucket=s3_bucket, Prefix=snowflake_statging_bucket_path)

        # Delete each object in the folder
        for obj in objects.get('Contents', []):
            s3.delete_object(Bucket=s3_bucket, Key=obj['Key'])

        return {
            'statusCode': 200,
            'body': 'Data processed successfully'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
