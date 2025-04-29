from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag,task
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowBadRequest, AirflowFailException, AirflowNotFoundException

@dag(start_date=datetime(2025, 1, 6), schedule=timedelta(hours=1), catchup=False, description='ETL for Predicted Weather Data', tags=['predicted_weather'])
def prediction_etl():

    @task
    def fetch_prediction():
        import requests
        weather_key = Variable.get('WEATHER_PREDC_API_KEY')
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            "appid": weather_key,
            "q": "Belem",
            "units": "metric",
            "lang": "pt_br"
        }

        response = requests.get(url, params=params)
        response.raise_for_status()

        response = response.json()

        # Primeiro converta o timestamp para um objeto datetime
        dt_object = datetime.fromtimestamp(response['dt'])

        # Depois adicione o ajuste de timezone
        dt_local = dt_object + timedelta(seconds=response['timezone'])

        # Por fim, converta para string no formato desejado
        response['dt'] = dt_local.strftime('%Y-%m-%d %H:%M:%S')

        return response
    
    fetch_prediction()

prediction_etl()