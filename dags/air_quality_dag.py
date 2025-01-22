from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
import duckdb
import json
import pandas as pd

API_KEY = Variable.get('API_KEY')
endpoint = f'currentConditions:lookup?key={API_KEY}'
latitude = Variable.get('LATITUDE')
longitude = Variable.get('LONGITUDE')
headers = {
    "Content-Type": "application/json"
    }

def generate_data(ti):

    json_value = json.loads(ti.xcom_pull(task_ids='fetch_data', key='return_value'))

    date_time = json_value["dateTime"]
    pollutants = json_value["pollutants"]

    conn = duckdb.connect()

    conn.execute("""
        CREATE TABLE pollutants_data (
            date_time TEXT,
            code TEXT,
            concentration_value DOUBLE,
            concentration_units TEXT
        )
    """)

    conn.executemany(
        "INSERT INTO pollutants_data VALUES (?, ?, ?, ?)",
        [
            (
                date_time,  # Mesmo valor de `dateTime` para todos os poluentes
                pollutant["code"],
                pollutant["concentration"]["value"],
                pollutant["concentration"]["units"],
            )
            for pollutant in pollutants
        ]
    )

    pollutants_data = conn.execute("SELECT * FROM pollutants_data").fetchall()

    print(pollutants_data)

    conn.execute("""
        COPY (
            SELECT *
            FROM pollutants_data
        ) TO './data/bronze/pollutants_data.parquet' (FORMAT PARQUET);
    """)

    conn.close()

    ti.xcom_push(key='pollutants_data', value=pollutants_data)    


def save(ti):

    pollutants_data = ti.xcom_pull(key='pollutants_data')

    df = pd.DataFrame(pollutants_data, columns=['date_time', 'code', 'concentration_value', 'concentration_units'])

    print(df)

    df.to_csv('./data/bronze/pollutants_data.csv', index=False)

with DAG(
    'air_quality_etl',
    start_date=datetime(2025, 1, 2),
    description='ETL for Air Quality Data',
    tags=['air_quality'],
    schedule=timedelta(hours=8),
    catchup=False) as dag:

    task_fetch = HttpOperator(
        task_id='fetch_data',
        http_conn_id='air_quality_connection',
        endpoint=endpoint,
        method='POST',
        data=json.dumps({
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
        }),
        headers= headers,
        response_check=lambda response: response.json(),
        do_xcom_push=True,
        log_response=True
    )

    task_generate_data = PythonOperator(
        task_id='generate_data',
        python_callable=generate_data,
        provide_context=True
    )

    task_save = PythonOperator(
        task_id='save',
        python_callable=save,
        provide_context=True
    )

    task_fetch >> task_generate_data >> task_save