# daatengineering-endtoendingestionpipeline

# AWS End to End Data Ingestion Pipeline

Prerequisite
1) Setting up AWS Eventbridge to trigger lambda functions when a file (with .csv extension only) arrives in s3 bucket. rest of the files should be ignored.
2) Utilizing S3 bucket as Snowflake Stage.

Project level - The real project architecture wise is going to look very similar to this, in some cases even a 1:1 match. The only thing that will change si the ETL. In real world ETL cases you have to deal with too many scenarious like
1) Inconsistent column names
2) Inconsistent column order (we deal with this in our code here as well)
3) inconsistent date types - We are using only YYYY-MM-DD here but in real life we may have to try 4 different formats if this one fails. Exception handling, firefighting and test cases are a lot in real life. 
4) In case if the ETL is more ocmplex and File sizes are Huge, We use AWS Glue instead of lambda. Lambda can work for max 15 mins but Id not recomemnd using Lambdas for anything that takes more than 3-5 mins. Rather use AWS Glue and with Glue dynamic datfarmes (spark).

Project Description - 
1) The client is dropping the data in s3 folders everyday (which are created in Part 1 of this project which can be found here : https://github.com/ihiteshmodi/dataengineering-ingestionpipeline-s3foldercreator)

2) The file has to autoinvoke an AWS lambda function (Using AWS Eventbridge)

3) The Aws fucntion will read the ETL details (cast type, column name change, and column order). turns the data consistent and loads to a S3 path that is being used as a stage in snwoflake.

4) then runs a stored procedure (On Snowflake SQL) that ingests data from staging table to Snowflake Main table. 

5) The filelog table will be update dby the lambda code until ingestion, the stored procedure will be ingesting rest of the details like ho wmnay rows were ingested into main table

NOTE: Its important to clear the stage after we are done ingesting. Run a delete command on the s3 bucket that is linekd ot stage.

-------------------------------------------------------------

NOTE : Please be aware that this architecture is designed to function seamlessly when the stored procedure is initiated by the Lambda or Glue code itself, as is the case in our context. However, if the stored procedures are manually triggered by individuals, a potential issue arises. In scenarios where multiple data sources are being merged into a single source, Snowflake will only update a single file log if multiple individuals activate the stored procedure concurrently.




# End-to-End AWS Data Ingestion Pipeline

## Prerequisites:
1. Establish AWS EventBridge to trigger Lambda functions exclusively for arriving .csv files in S3.
2. Leverage S3 buckets as Snowflake stages.

## Project Overview:
The project architecture closely aligns with this schematic, often in a 1:1 correlation, with the primary distinction lying in the ETL process. In actual ETL workflows, we grapple with numerous complexities such as:
1. Inconsistent column naming
2. Variable column sequencing (addressed in our code as well)
3. Heterogeneous date formats – in practice, multiple formats are handled, requiring rigorous exception handling, testing, and troubleshooting.
4. For intricate ETLs with extensive data volumes, AWS Glue supersedes Lambda due to time constraints (Lambda's 15-minute max execution). AWS Glue, integrated with dynamic dataframes (Spark), is better suited for lengthier as well as heavier processing.

## Project Description:
1. The client deposits daily data into S3 folders (auto-created in Part 1 of the project which can be found ![here](https://github.com/ihiteshmodi/dataengineering-ingestionpipeline-s3foldercreator)).
2. AWS Lambda is triggered automatically (via AWS EventBridge).
3. Lambda assesses ETL specifications (type casting, column name adjustments, sequencing), ensuring data consistency. Processed data lands in a designated S3 stage used for Snowflake.
4. A Snowflake SQL stored procedure is executed, transferring data from the stage to the main table.
5. The Lambda code maintains the filelog table until ingestion is complete. The stored procedure subsequently updates specifics like the number of rows ingested into the main table.

## Data Hygiene:
Clearing the stage after ingestion is vital. Post-ingestion, a delete command is executed on the associated S3 bucket linked to the stage.

---

**Note**: This architectural design functions seamlessly when the Lambda or Glue code itself initiates the stored procedure, as in our context. However, manual triggering of stored procedures by individuals can lead to potential conflicts. Specifically, in situations where diverse data sources merge into one, Snowflake updates only one file log if multiple individuals concurrently activate the stored procedure.
