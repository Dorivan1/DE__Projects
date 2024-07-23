# Podcast Data Pipeline with Airflow


## Overview

In this project, we create a data pipeline using Apache Airflow to download podcast episodes, store metadata in a SQLite database, and transcribe audio files using Vosk. This pipeline is designed to automate the process and provide scheduling capabilities.



## Project Steps

1. **Download Podcast Metadata**: Fetches podcast metadata from a specified URL using XML parsing.
2. **Create SQLite Database**: Sets up a SQLite database to store podcast episode metadata.
3. **Download Podcast Episodes**: Utilizes requests library to download podcast audio files.


### Installation

To run this project locally, ensure you have:

- Airflow 2.3+
- Python 3.8+
- docker desktop

### Docker Setup

1. Start Docker and navigate to the project directory.
   
   ```bash
   cd path/to/your/project/directory
   docker-compose up -d

2. Access the Airflow web interface:

Open a web browser and go to http://localhost:8080.

3. Monitor and manage your DAGs through the Airflow UI.


## Conclusion
This project demonstrates how to build a robust data pipeline using Airflow, Docker, and Python