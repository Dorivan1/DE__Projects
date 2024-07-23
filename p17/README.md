# Weather Data Pipeline Project



## Overview


This project automates the process of fetching, processing, and storing weather data every hour using a combination of AWS services and Snowflake. The pipeline involves fetching data from a weather API, storing it in DynamoDB, processing streams, saving it in S3, and loading it into Snowflake for analysis.



## Prerequisites

- AWS Account
- Snowflake Account
- Weather API Key



## Architecture


1. **Weather API:** Fetch weather data using a Python script
2. **EventBridge:** Schedule a Lambda function to run every hour
3. **Lambda Function:** Fetch data from the Weather API and store it in DynamoDB
4. **DynamoDB Stream:** Trigger another Lambda function to process the stream
5. **S3 Bucket:** Store processed data in CSV format
6. **Snowpipe:** Notify Snowflake when new data is available in S3
7. **Snowflake:** Load data from S3 into Snowflake for analysis


## Steps

### 1. Weather API Key

Obtain a weather API key from a weather data provider website.

### 2. EventBridge

Create an EventBridge schedule to trigger a Lambda function every hour.

### 3. Lambda Function

Set up a Lambda function to fetch weather data and store it in DynamoDB.

### 4. DynamoDB Stream

Enable DynamoDB Streams and create another Lambda function to process the stream and save data in S3.

### 5. S3 Bucket

Create an S3 bucket to store the CSV files.

### 6. Snowpipe

Configure Snowpipe to notify Snowflake when new data is available in S3 and load the data into Snowflake.

### 7. Snowflake Setup

Create a database and table in Snowflake to store the weather data.


### 8. Querying Data in Snowflake

Run queries in Snowflake to verify data loading and analyze the weather data.



## Conclusion

This project sets up a robust and automated pipeline for continuous weather data ingestion and analysis, leveraging AWS and Snowflake to ensure scalability and reliability.





