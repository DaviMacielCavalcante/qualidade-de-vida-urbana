from datetime import datetime, timedelta
from airflow.providers.http.operators.http import HttpOperator
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag,task
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowBadRequest, AirflowFailException, AirflowNotFoundException

@dag(start_date=datetime(2025, 1, 6), schedule=timedelta(hours=1), catchup=False, description='ETL for Air Quality Data', tags=['air_quality'])
def air_quality_etl():

    with TaskGroup(group_id="get_data") as get_data:

        @task
        def get_bairros():
            bairros = Variable.get('DICIONARIO_BAIRRO', deserialize_json=True)
            bairros_list = [
                {"nome": bairro, "latitude": coords[0], "longitude": coords[1]} 
                for bairro, coords in bairros.items()
            ]
            return bairros_list
    
        @task
        def fetch_weather(bairro_info):
            import requests
            
            weather_key = Variable.get('WEATHER_API_KEY')
            url = "https://api.weatherapi.com/v1/current.json"
            params = {
                "key": weather_key,
                "q": f"{bairro_info['latitude']},{bairro_info['longitude']}",
                "aqi": "no"
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()

            return {
                "bairro": bairro_info["nome"],
                "data": response.json()
            }
        
        @task
        def fetch_google(bairro_info):
            import requests

            try:
                API_KEY = Variable.get('GOOGLE_AIR_QUALITY_API_KEY')

                headers = {
                "Content-Type": "application/json"
                }
                url = f"https://airquality.googleapis.com/v1/currentConditions:lookup?key={API_KEY}"
                body = {
                    "universalAqi": True,
                    "location": {
                    "latitude": bairro_info["latitude"],
                    "longitude": bairro_info["longitude"],
                    },
                    "extraComputations": [
                        "DOMINANT_POLLUTANT_CONCENTRATION",
                        "POLLUTANT_CONCENTRATION",
                        "LOCAL_AQI"
                    ],
                    "languageCode": "pt-br"
                }

                response = requests.post(url, headers=headers, json=body)
                response.raise_for_status()
            except Exception as e:
                if isinstance(e, AirflowNotFoundException):
                    raise AirflowNotFoundException(f"Erro ao obter variável de ambiente: {e}")
                elif isinstance(e, AirflowBadRequest):
                    raise AirflowBadRequest(f"Erro ao fazer a requisição para o bairro {bairro_info['bairro']} na API de Air Quality do Google: {e}")
                else:
                    raise AirflowFailException(f"Erro inesperado: {e}")

            return {
                "bairro": bairro_info["nome"],
                "data": response.json()
            }
        
        @task
        def process_results_google(google_results):
            processed_data = {}
            for result in google_results:
                bairro = result["bairro"]
                google_data = result["data"]
                
                processed_data[bairro] = {
                    "datetime": google_data.get("dateTime", {}),
                    "indexes": google_data.get("indexes", {}),
                    "pollutants": google_data.get("pollutants", {}),
                }
            
            return processed_data
        
        @task
        def process_results_weather(weather_results):
            processed_data = {}
            for result in weather_results:
                bairro = result["bairro"]
                weather_data = result["data"]
                
                dados = weather_data.get("current", {})
                
                processed_data[bairro] = {
                    "latitude": weather_data.get("location", {}).get("lat"),
                    "longitude": weather_data.get("location", {}).get("lon"),
                    "data": dados
                }
            
            return processed_data
        
        bairros_list = get_bairros()

        google_results = fetch_google.expand(bairro_info=bairros_list)
        
        google_data = process_results_google(google_results)
        
        weather_results = fetch_weather.expand(bairro_info=bairros_list)
        
        weather_data = process_results_weather(weather_results)

    with TaskGroup(group_id="push_to_postgres") as push_to_postgres:

        task_create_schemas = SQLExecuteQueryOperator(
            task_id='create_schemas',
            conn_id='postgres_conn',
            sql='SQL/DDL/create_schemas.sql'
        )

        task_create_google_tables = SQLExecuteQueryOperator(
            task_id='create_google_tables',
            conn_id='postgres_conn',
            sql='SQL/DDL/google/create_tables.sql'
        )

        task_create_google_triggers = SQLExecuteQueryOperator(
            task_id='create_google_triggers',
            conn_id='postgres_conn',
            sql='SQL/DDL/google/triggers_google.sql'
        )

        task_create_weather_tables = SQLExecuteQueryOperator(
            task_id='create_weather_tables',
            conn_id='postgres_conn',
            sql='SQL/DDL/weather/create_tables.sql'
        )

        task_create_weather_triggers = SQLExecuteQueryOperator(
            task_id='create_weather_triggers',
            conn_id='postgres_conn',
            sql='SQL/DDL/weather/triggers_weather.sql'
        )

        @task
        def task_insert_google_bronze(json_data: dict):
            import json

            for bairro, dados in json_data.items():

                bairro_json = json.dumps({bairro: dados})

                hook = PostgresHook(postgres_conn_id='postgres_conn')

                insert_query = """
                    INSERT INTO bronze.google_api_data(data)
                    VALUES (%s)            
                """

                hook.run(insert_query, parameters=(bairro_json,))   

        @task
        def task_insert_weather_bronze(json_data: dict):
            import json

            for bairro, dados in json_data.items():

                bairro_json = json.dumps({bairro: dados})

                hook = PostgresHook(postgres_conn_id='postgres_conn')

                insert_query = """
                    INSERT INTO bronze.weather_api_data(data)
                    VALUES (%s)            
                """

                hook.run(insert_query, parameters=(bairro_json,))   

        chain(task_create_schemas, task_create_google_tables, task_create_google_triggers,task_create_weather_tables, task_create_weather_triggers, task_insert_weather_bronze(weather_data), task_insert_google_bronze(google_data))

       
        get_data >> push_to_postgres
    

air_quality_etl()