from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests
import json
import pandas as pd
from pyspark.sql import SparkSession

API_KEY = Variable.get('API_KEY', default_var=None)
url = f'https://airquality.googleapis.com/v1/currentConditions:lookup?key={API_KEY}'
latitude = Variable.get('LATITUDE', default_var=None)
longitude = Variable.get('LONGITUDE', default_var=None)
endpoint = f'currentConditions:lookup?key={API_KEY}'
headers = {
    "Content-Type": "application/json"
    }

def fetch_air_data(ti):
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

    ti.xcom_push(key="air_quality_api_data", value=response.json())

def generate_df(ti):
    data = ti.xcom_pull(task_ids='fetch_data', key='air_quality_api_data')
    
    data_list = []

    for p in data['pollutants']:
        data_list.append({
            "datetime": data['dateTime'],
            "latitude": latitude,
            "longitude": longitude,
            "pollutant": p["displayName"].lower(),
            "value": p["concentration"]["value"],
            "unit": p["concentration"]["units"].lower()
        })

    df = pd.DataFrame(data_list)

    df["datetime"] = pd.to_datetime(df["datetime"])
    df["year"] = df["datetime"].dt.year
    df["month"] = df["datetime"].dt.month
    df["day"] = df["datetime"].dt.day
    df["hour"] = df["datetime"].dt.hour
    df.drop("datetime", axis=1, inplace=True)
    df["latitude"] = df["latitude"].astype(float)
    df["longitude"] = df["longitude"].astype(float)
    df["value"] = df["value"].astype(float)

    ti.xcom_push(key="pandas_df", value= df.to_json(orient='records'))


def generate_parquet(ti):
    pandas_json = ti.xcom_pull(task_ids='generate_df', key='pandas_df')
    pandas_df = pd.read_json(pandas_json, orient='records')

    spark = SparkSession.builder.appName("parquet_generator").getOrCreate()

    spark_df = spark.createDataFrame(pandas_df)

    try:
        spark_df.write.mode("append").partitionBy("year", "month", "day").parquet('./datalake/raw')
    except Exception as e:
        print(f'erro: {e}')

    spark.stop()

with DAG(
    'air_quality_etl',
    start_date=datetime(2025, 1, 22),
    description='ETL for Air Quality Data',
    tags=['air_quality'],
    schedule=timedelta(hours=8),
    catchup=False) as dag:

    # teste = SimpleHttpOperator(
    #     task_id='test_api',
    #     http_conn_id='air_quality_connection',
    #     endpoint=endpoint,
    #     method='POST',
    #     data=json.dumps({
    #         "universalAqi": "true",
    #         "location": {
    #             "latitude":latitude,
    #             "longitude":longitude
    #         },
    #         "extraComputations": [
    #             "DOMINANT_POLLUTANT_CONCENTRATION",
    #             "POLLUTANT_CONCENTRATION",
    #             "LOCAL_AQI"
    #         ],
    #         "languageCode": "pt-br"
    #     }),
    #     headers= headers,
    #     response_check=lambda response: response.json(),
    #     log_response=True
    # )

    task_fetch = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_air_data
    )

    task_generate_df = PythonOperator(
        task_id='generate_df',
        python_callable=generate_df,
        provide_context=True
    )

    task_generate_parquets = PythonOperator(
        task_id="generate_parquet",
        python_callable=generate_parquet,
        provide_context=True
    )

    task_fetch >> task_generate_df >> task_generate_parquets