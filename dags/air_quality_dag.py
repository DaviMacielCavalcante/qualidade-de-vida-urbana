from datetime import datetime, timedelta
from airflow.providers.http.operators.http import HttpOperator
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag,task
from airflow.utils.task_group import TaskGroup
import json

API_KEY = Variable.get('GOOGLE_AIR_QUALITY_API_KEY')
weather = Variable.get('WEATHER_API_KEY')
endpoint = f'currentConditions:lookup?key={API_KEY}'
latitude = Variable.get('LATITUDE')
longitude = Variable.get('LONGITUDE')
headers = {
    "Content-Type": "application/json"
    }

@dag(start_date=datetime(2025, 1, 6), schedule=timedelta(hours=8), catchup=False, description='ETL for Air Quality Data', tags=['air_quality'])
def air_quality_etl():

    with TaskGroup(group_id="get_data") as get_data:

        task_fetch_google_api = HttpOperator(
            task_id='fetch_data_air_quality_google',
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

        task_fetch_weather_api = HttpOperator(
            task_id='fetch_data_weather_api',
            http_conn_id='weather_api_connection',
            endpoint=f"current.json?key={weather}&q= {latitude}, {longitude}&aqi=no",
            method='GET',
            response_check=lambda response: response.json(),
            do_xcom_push=True,
            log_response=True
        )

    with TaskGroup(group_id="push_to_postgres") as push_to_postgres:

        task_create_schemas_raw = SQLExecuteQueryOperator(
            task_id='create_schemas_raw',
            conn_id='postgres_conn',
            sql='SQL/DDL/create_schemas.sql'
        )

        task_create_google_tables_raw = SQLExecuteQueryOperator(
            task_id='create_google_tables_raw',
            conn_id='postgres_conn',
            sql='SQL/DDL/google/create_tables_raw.sql'
        )

        task_create_weather_tables_raw = SQLExecuteQueryOperator(
            task_id='create_weather_tables_raw',
            conn_id='postgres_conn',
            sql='SQL/DDL/weather/create_tables_raw.sql'
        )

        task_create_schemas_raw >> [task_create_google_tables_raw, task_create_weather_tables_raw]

       
    [task_fetch_google_api, task_fetch_weather_api] >> push_to_postgres
    

air_quality_etl()