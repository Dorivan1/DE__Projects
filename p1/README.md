# AWS Glue ETL Job for Data Processing

This project involves setting up an ETL job using AWS Glue to process and analyze data stored in S3. The ETL job reads the data from S3, transforms it, and loads it into Amazon Redshift for further analysis. The project includes setting up the necessary AWS infrastructure, connecting to Redshift, creating tables, and loading data from S3 to Redshift.

## Project Overview

### Objectives
- Create an AWS user and set up security configurations.
- Set up S3 buckets and make objects public.
- Create security groups and configure security settings.
- Set billing alarms for cost management.
- Create and configure an AWS Redshift cluster.
- Create tables and data models in Redshift.
- Load data from S3 into Redshift.
- Use Jupyter Notebook for data processing and analysis.
- Delete the Redshift cluster to manage costs.

### Tools and Services Used
- AWS Glue
- AWS S3
- AWS Redshift
- Jupyter Notebook
- Python (boto3, pandas, psycopg2)

## Project Steps

### 1. AWS Setup
- Created an AWS user with appropriate permissions.
- Set up an S3 bucket and uploaded data files.
- Configured security settings and created security groups.
- Set billing alarms to monitor and manage AWS costs.

### 2. Redshift Cluster Setup
- Created an Amazon Redshift cluster and configured it.
- Connected to the Redshift cluster using Jupyter Notebook.
- Created tables and data models in Redshift.

### 3. Data Processing and Loading
- Loaded data from S3 into Redshift tables using SQL COPY commands.
- Performed data processing and analysis using Jupyter Notebook.

### 4. Clean Up
- Deleted the Redshift cluster to avoid incurring additional costs.



## Conclusion
This project provided hands-on experience with AWS services, including Glue, S3, and Redshift. It demonstrated the end-to-end process of setting up an ETL pipeline, from data ingestion to processing and analysis.

## Jupyter Notebook
The Jupyter Notebook script used for this project is included in the repository. It contains the code for connecting to AWS, creating tables in Redshift, loading data from S3, and performing data analysis.

## Data
The data used for this project can be found [here](https://www.youtube.com/redirect?event=video_description&redir_token=QUFFLUhqa2RocTRPRnNta3lrWVZsQVJtRnEwMjRudXJPQXxBQ3Jtc0tsX0o4RW1qd1Jra3ZHWVJEdnVMYmhWeEx4YnJtWVBKN0RENlQ4bWhiblJwbnNrc3pMTmpwTEdVM3hzdjIzX2xmUF8xZjlNMU9QRGdwWG9DYkhGeFFKRXZSR2xERkNoNzcxWUctd3hqZVRxU2JFbk9Wcw&q=https%3A%2F%2Fdocs.aws.amazon.com%2Fredshift%2Flatest%2Fgsg%2Fsamples%2Ftickitdb.zip&v=BopMJPEH6AE).