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
5. In real time data, the error eg - Duplicates have to be in the filelog as well as as there has to be a seperate folders where only the duplicate piece from the file is saved with same file name to identify which all rows were duplicates

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
