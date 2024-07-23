import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sqlite3

def fetch_and_update_data():


	# Connect to the SQLite database
	conn = sqlite3.connect('top5scorers.db')

	cursor = conn.cursor()

	# Define the schema
	CREATE_TABLE_QUERY = """
	CREATE TABLE IF NOT EXISTS top_scorers (
		id INTEGER PRIMARY KEY,
		player_name TEXT,
		age INTEGER,
		club TEXT,
		goals INTEGER,
		assists INTEGER,
		photo_url TEXT
	)
	"""

	cursor.execute(CREATE_TABLE_QUERY)
	cursor.execute("DELETE FROM top_scorers")
	conn.commit()
	

	url = "https://api-football-v1.p.rapidapi.com/v3/players/topscorers"

	querystring = {"league":"39","season":"2023"}

	headers = {
		"X-RapidAPI-Key": "",# use your generated key here
	
		"X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
	}

	response = requests.get(url, headers=headers, params=querystring)

	data = response.json()

	if 'response' in data:
		print("data was found")
		top_scorers = data['response']
		counter = 0
		formatted_data = ""
		for player_info in top_scorers:
			if counter == 5:
				break
			player = player_info['player']
			statistics = player_info['statistics'][0]

			# Extracting relevant information
			player_name = player['name']
			age = player['age']
			club = statistics['team']['name']
			goals = statistics['goals']['total']
			assists = statistics['goals']['assists']
			photo_url = player['photo']

			# Formatting the data
			player_data = f"Player: {player_name}\nAge: {age}\nClub: {club}\nGoals: {goals}\nAssists: {assists}\nPhoto: {photo_url}\n"

			formatted_data += player_data
			counter += 1
			insert_query = """
				INSERT INTO top_scorers (player_name, age, club, goals, assists, photo_url)
				VALUES (?, ?, ?, ?, ?, ?)
			"""
			cursor.execute(insert_query, (player_name, age, club, goals, assists, photo_url))
		print(formatted_data)
		conn.commit()  # <-- Commit after the loop completes

		# Close the connection
		conn.close()
			
	else:
		print("No data found.")



default_args = {
	'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,4,24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

dag = DAG(
    'update_top_scorers_data',
    default_args=default_args,
    description='Fetches and updates top 5 scorers data daily',
    schedule_interval=timedelta(days=1),
)

fetch_and_update_operator = PythonOperator(
    task_id='fetch_and_update_data_task',
    python_callable=fetch_and_update_data,
    dag=dag,
)

fetch_and_update_operator

