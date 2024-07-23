# Spotify Data Pipeline with Airflow

## Overview

In this project, we create a data pipeline using Apache Airflow to fetch recently played songs from the Spotify API, store the data in a SQLite database, and schedule the process to run daily. This pipeline leverages Airflow's capabilities to manage and automate the ETL (Extract, Transform, Load) workflow.

## Project Steps

1. **Fetch Spotify Data**: Retrieve recently played songs from the Spotify API.
2. **Validate Data**: Ensure the data integrity by checking for duplicates and null values.
3. **Store Data**: Save the validated data into a SQLite database.

### Installation

To run this project locally, ensure you have:

- Airflow 2.3+
- Python 3.8+
- Docker Desktop

### Docker Setup

1. Start Docker and navigate to the project directory.
   
   ```bash
   cd path/to/your/project/directory
   docker-compose up -d

2. Access the Airflow web interface:

    Open a web browser and go to http://localhost:8080.

3. Monitor and manage your DAGs through the Airflow UI.



## Files

- `spotify_etl.py`: Contains the ETL process code to fetch and store Spotify data.
- `spotify_dag.py`: Defines the Airflow DAG and tasks to schedule the ETL process.

## Code Overview

### spotify_etl.py

This file includes functions to:

- Retrieve access tokens from Spotify.
- Check if the data is valid.
- Fetch recently played songs from the Spotify API.
- Load the data into a SQLite database.

### spotify_dag.py

This file sets up the Airflow DAG, including:

- Default arguments for the DAG.
- A PythonOperator to run the ETL process from `spotify_etl.py`.

## Conclusion

This project demonstrates how to build a data pipeline using Airflow, Docker, and Python to automate data fetching and storage processes.