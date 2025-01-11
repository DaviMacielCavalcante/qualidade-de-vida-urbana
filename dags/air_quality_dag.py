from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests
from dotenv import dotenv_values

API_KEY = Variable.get('API_KEY', default_var=None)
url = f'https://airquality.googleapis.com/v1/currentConditions:lookup?key={API_KEY}'
latitude = Variable.get('AIRFLOW_VAR_LATITUDE', default_var=None)
longitude = Variable.get('AIRFLOW_VAR_LONGITUDE', default_var=None)

def fetch_air_data(url, latitude, longitude):
    payload = {
    "universalAqi": "true",
    "location": {
        "latitude":latitude,
        "longitude":longitude
    },
    "extraComputations": [
        "DOMINANT_POLLUTANT_CONCENTRATION",
        "POLLUTANT_CONCENTRATION",
        "LOCAL_AQI"
    ],
    "languageCode": "pt-br"
    }
    headers = {
    "Content-Type": "application/json"
    }
    response = requests.post(url, json=payload, headers=headers)

    return response.json()


def return_json_body():
    data = fetch_air_data(url, latitude, longitude)
    return data

with DAG(
    'air_quality_etl',
    start_date=datetime(2025, 1, 12),
    description='ETL for Air Quality Data',
    tags=['air_quality'],
    schedule='@daily',
    catchup=False):
    task_fetch = PythonOperator(
        task_id='fetch_data',
        python_callable=return_json_body
    )

