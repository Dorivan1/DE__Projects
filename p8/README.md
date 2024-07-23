# Daily Top 5 Premier League Scorers Pipeline

## Project Overview

This project involves creating an Apache Airflow pipeline that connects to a football API to fetch and update the top 5 scorers in the Premier League daily. The fetched data is stored in a SQLite database. The pipeline is scheduled to run daily.

## Steps Performed

1. **Configure Airflow**: Set up Airflow to manage the data pipeline.
2. **Connect to Football API**: Use the RapidAPI Football API to fetch data on the top 5 Premier League scorers.
3. **Create Function to Fetch and Transform Data**:
   - Connect to the API.
   - Transform the JSON response to extract relevant information.
   - Insert the relevant data into a SQLite database.
4. **Schedule DAG**: Create a DAG in Airflow to schedule the data fetching and updating task daily.

## Files

### `main.py`

This file defines the Airflow DAG and the function to fetch and update the top 5 scorers' data daily.

#### Functionality:
- Connects to the SQLite database `top5scorers.db`.
- Creates the `top_scorers` table if it doesn't exist.
- Deletes existing records in the `top_scorers` table.
- Connects to the football API to fetch the top 5 scorers.
- Inserts the fetched data into the `top_scorers` table.
- Commits the transaction and closes the database connection.


## Conclusion
This project demonstrates how to set up an Airflow pipeline to automate data fetching and updating tasks using external APIs and a SQLite database. The daily scheduled task ensures that the top 5 Premier League scorers' information is always up-to-date.



