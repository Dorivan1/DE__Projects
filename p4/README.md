# Data Engineering Project AWS

In this project, we aimed to develop an AWS-based data pipeline using Apache Airflow, Python, and AWS services. Unfortunately, most project files, including the Airflow DAG and Redshift YAML, were lost due to improper saving and not planning to update these files into the GitHub repository.

## Project Overview

The objective was to extract weather data from OpenWeather, store it in S3, and load it into Redshift using Airflow.

### First Airflow DAG: Data Extraction to S3

**Task 1: Extracting Data from OpenWeather API**
- Utilized Python to fetch weather data from OpenWeather API.

**Task 2: Storing Data in S3**
- Structured and stored extracted data in an S3 bucket.

### Second Airflow DAG: ETL to Redshift

**Task 1: Dependency on First DAG**
- Ensured completion of the data extraction process.

**Task 2: ETL to Redshift using Glue Operator**
- Transformed and loaded data into Redshift using Glue and Spark.

## Conclusion

Despite the loss of project files, this project aimed to automate weather data updates in Redshift through a robust AWS infrastructure.