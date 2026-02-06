from datetime import datetime as dt, timedelta
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task, TaskGroup, Variable
from aws.secrets import get_parameter

@dag(
    start_date=dt(2025, 1, 6), 
    schedule=None, 
    catchup=False, 
    description='ETL for starting seeking Google data', 
    is_paused_upon_creation=False,
    tags=['setup', 'google'])
def google_setup():
    
    @task
    def get_last_24_h():
        import requests
        import boto3
        import pyarrow as pa
        import pyarrow.parquet as pq
        import json
        from plugins.constants.constants import AIRFLOW_GOOGLE_WEATHER_API_KEY_PATH, CASTANHAL_LATITUDE, CASTANHAL_LONGITUDE, AIRFLOW_AWS_S3_SECRET_KEY_ID_PATH, AIRFLOW_AWS_S3_SECRET_PATH, S3_REGION_PATH, S3_BUCKET_BRONZE_PATH
        from io import BytesIO
        
        url = "https://weather.googleapis.com/v1/history/hours:lookup"
        params = {
            "key": get_parameter(AIRFLOW_GOOGLE_WEATHER_API_KEY_PATH),
            "location.latitude": CASTANHAL_LATITUDE,
            "location.longitude": CASTANHAL_LONGITUDE,
            "unitsSystem": "METRIC"
        }

        response = requests.get(url=url, params=params)
        response.raise_for_status()
        
        json_data = response.json()
        
        table = pa.Table.from_pydict({
            "raw_json": [json.dumps(hora) for hora in json_data["historyHours"]]      
        })
        
        metadata = {
            b'layer': b'bronze',
            b'destiny': b'silver',
            b'fonte_nome': b'Weather_API_Google',
            b'cidade': b'CASTANHAL',
            b'fonte_datetime_insert': dt.now().isoformat().encode()
        }
        
        table = table.replace_schema_metadata(metadata)
        
        agora = dt.now()
        
        path_parquet_s3 = (
            f"{metadata[b'fonte_nome'].decode().lower()}/historical/{metadata[b'cidade'].decode().lower()}/"
            f"{metadata[b'cidade'].decode().lower()}-{agora.isoformat()}.parquet"
        )
        
        s3_client = boto3.client(
                        's3',
                        aws_access_key_id = get_parameter(AIRFLOW_AWS_S3_SECRET_KEY_ID_PATH),
                        aws_secret_access_key = get_parameter(AIRFLOW_AWS_S3_SECRET_PATH),
                        region_name = get_parameter(S3_REGION_PATH)
                    )

        bucket_name = get_parameter(S3_BUCKET_BRONZE_PATH)
        buffer = BytesIO()

        pq.write_table(
            table,
            buffer,
            compression="snappy"
        )

        buffer.seek(0)

        s3_client.put_object(
            Body=buffer.getvalue(),
            Bucket=bucket_name,
            Key=path_parquet_s3    
        )
        
        return True 
    
    trigger_last_hour_dag = TriggerDagRunOperator(
    task_id="trigger_hourly_dag",
    trigger_dag_id="last_hour_google",
    wait_for_completion=False
    )
    
    @task
    def unpause_hourly_dag():
        import requests
        
        base_url = Variable.get("BASE_URL", default="http://airflow-apiserver:8080")
        username = Variable.get("ADMIN_USER")
        password = Variable.get("ADMIN_PASSWORD")
        
        # Pega o token
        token_response = requests.post(
            f"{base_url}/auth/token",
            json={"username": username, "password": password},
            headers={"Content-Type": "application/json"}
        )
        
        if token_response.status_code != 201:
            raise Exception(f"Falha ao autenticar: {token_response.text}")
        
        token = token_response.json().get("access_token")
        
        # Ativa a DAG
        response = requests.patch(
            f"{base_url}/api/v2/dags/last_hour_google?update_mask=is_paused",
            json={"is_paused": False},
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}"
            }
        )
        
        if response.status_code == 200:
            return True
        else:
            raise Exception(f"Falha ao ativar DAG: {response.text}")
        
        
    get_last_24_h() >> unpause_hourly_dag() >> trigger_last_hour_dag
    
@dag (
    start_date=dt(2025, 1, 6),
    schedule=timedelta(hours=1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=['google', 'last_hour'])
def last_hour_google():
    
    @task 
    def get_last_hour_google():
        import requests
        import boto3
        import pyarrow as pa
        import pyarrow.parquet as pq
        import json
        from plugins.constants.constants import AIRFLOW_GOOGLE_WEATHER_API_KEY_PATH, CASTANHAL_LATITUDE, CASTANHAL_LONGITUDE, AIRFLOW_AWS_S3_SECRET_KEY_ID_PATH, AIRFLOW_AWS_S3_SECRET_PATH, S3_REGION_PATH, S3_BUCKET_BRONZE_PATH
        from io import BytesIO
        
        url = "https://weather.googleapis.com/v1/currentConditions:lookup"
        params = {
            "key": get_parameter(AIRFLOW_GOOGLE_WEATHER_API_KEY_PATH),
            "location.latitude": CASTANHAL_LATITUDE,
            "location.longitude": CASTANHAL_LONGITUDE,
            "unitsSystem": "METRIC"
        }

        response = requests.get(url=url, params=params)
        response.raise_for_status()
        
        json_data = response.json()
        
        table = pa.Table.from_pydict({
            "raw_json": [json.dumps(json_data)]
        })
        
        metadata = {
            b'layer': b'bronze',
            b'destiny': b'silver',
            b'fonte_nome': b'Weather_API_Google',
            b'cidade': b'CASTANHAL',
            b'fonte_datetime_insert': dt.now().isoformat().encode()
        }
        
        table = table.replace_schema_metadata(metadata)
        
        agora = dt.now()
        
        path_parquet_s3 = (
            f"{metadata[b'fonte_nome'].decode().lower()}/hourly/{metadata[b'cidade'].decode().lower()}/"
            f"{agora.year}/{agora.month:02d}/{agora.day:02d}/"
            f"{metadata[b'cidade'].decode().lower()}-{agora.isoformat()}.parquet"
        )
        
        s3_client = boto3.client(
                        's3',
                        aws_access_key_id = get_parameter(AIRFLOW_AWS_S3_SECRET_KEY_ID_PATH),
                        aws_secret_access_key = get_parameter(AIRFLOW_AWS_S3_SECRET_PATH),
                        region_name = get_parameter(S3_REGION_PATH)
                    )

        bucket_name = get_parameter(S3_BUCKET_BRONZE_PATH)
        buffer = BytesIO()

        pq.write_table(
            table,
            buffer,
            compression="snappy"
        )

        buffer.seek(0)

        s3_client.put_object(
            Body=buffer.getvalue(),
            Bucket=bucket_name,
            Key=path_parquet_s3    
        )
        
        return True 
    
    get_last_hour_google()
    
google_setup()
last_hour_google()