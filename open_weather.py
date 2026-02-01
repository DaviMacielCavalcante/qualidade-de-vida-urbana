from datetime import datetime, timedelta
from airflow.sdk import dag, task, TaskGroup
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowBadRequest, AirflowFailException, AirflowNotFoundException
from aws.secrets import get_parameter


@dag(
    start_date=datetime(2025, 1, 6),
    schedule=timedelta(hours=1),
    catchup=False,
    description='ETL for Air Quality Data',
    tags=['air_quality']
)
def air_quality_etl():

    with TaskGroup(group_id="get_data") as get_data:

        @task
        def get_bairros():
            """Busca lista de bairros da variável do Airflow."""
            bairros = Variable.get('DICIONARIO_BAIRRO', deserialize_json=True)
            return [
                {"nome": bairro, "latitude": coords[0], "longitude": coords[1]}
                for bairro, coords in bairros.items()
            ]

        @task
        def fetch_weather(bairro_info):
            """Busca clima atual via OpenWeather."""
            import requests

            weather_key = Variable.get('AIRFLOW_VAR_OPEN_WEATHER_API_KE')
            url = "https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API key}"
            params = {
                "lat": bairro_info["latitude"],
                "lon": bairro_info["longitude"],
                "appid": weather_key,
                "units": "metric",
            }

            response = requests.get(url, params=params)
            response.raise_for_status()

            return {
                "bairro": bairro_info["nome"],
                "latitude": bairro_info["latitude"],
                "longitude": bairro_info["longitude"],
                "data": response.json()
            }

        @task
        def fetch_google(bairro_info):
            """Busca qualidade do ar via Google Air Quality para um bairro."""
            import requests

            try:
                API_KEY = Variable.get('GOOGLE_AIR_QUALITY_API_KEY')
                url = f"https://airquality.googleapis.com/v1/currentConditions:lookup?key={API_KEY}"
                headers = {"Content-Type": "application/json"}
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
                    raise AirflowBadRequest(f"Erro na API Google para o bairro {bairro_info['nome']}: {e}")
                else:
                    raise AirflowFailException(f"Erro inesperado: {e}")

            return {
                "bairro": bairro_info["nome"],
                "data": response.json()
            }

        @task
        def process_results_weather(weather_results):
            """Processa e estrutura os resultados do OpenWeatherMap."""
            processed_data = {}
            for result in weather_results:
                bairro = result["bairro"]
                weather = result["data"]

                # Ajuste de timezone
                dt_object = datetime.fromtimestamp(weather["dt"])
                dt_local = dt_object + timedelta(seconds=weather["timezone"])

                processed_data[bairro] = {
                    "latitude": result["latitude"],
                    "longitude": result["longitude"],
                    "data": {
                        "data_hora": dt_local.strftime("%Y-%m-%d %H:%M:%S"),
                        "temperatura": weather["main"]["temp"],
                        "sensacao_termica": weather["main"]["feels_like"],
                        "temperatura_min": weather["main"]["temp_min"],
                        "temperatura_max": weather["main"]["temp_max"],
                        "umidade": weather["main"]["humidity"],
                        "pressao": weather["main"]["pressure"],
                        "velocidade_vento": weather["wind"]["speed"],
                        "direcao_vento": weather["wind"].get("deg", 0),
                        "rajada_vento": weather["wind"].get("gust", 0.0),
                        "chuva_1h": weather.get("rain", {}).get("1h", 0.0),
                        "nuvens_porcentagem": weather["clouds"]["all"],
                        "visibilidade": weather.get("visibility", 0),
                        "descricao": weather["weather"][0]["description"],
                        "nascer_sol": datetime.fromtimestamp(weather["sys"]["sunrise"]).strftime("%H:%M:%S"),
                        "por_sol": datetime.fromtimestamp(weather["sys"]["sunset"]).strftime("%H:%M:%S"),
                    }
                }

            return processed_data

        @task
        def process_results_google(google_results):
            """Processa e estrutura os resultados do Google Air Quality."""
            processed_data = {}
            for result in google_results:
                bairro = result["bairro"]
                google_data = result["data"]

                processed_data[bairro] = {
                    "datetime": google_data.get("dateTime", ""),
                    "indexes": google_data.get("indexes", []),
                    "pollutants": google_data.get("pollutants", []),
                }

            return processed_data

    
        bairros_list = get_bairros()

        weather_results = fetch_weather.expand(bairro_info=bairros_list)
        weather_data = process_results_weather(weather_results)

        google_results = fetch_google.expand(bairro_info=bairros_list)
        google_data = process_results_google(google_results)

    
    with TaskGroup(group_id="push_to_postgres") as push_to_postgres:

        task_create_schemas = SQLExecuteQueryOperator(
            task_id='create_schemas',
            conn_id='postgres_conn',
            sql='SQL/DDL/create_schemas.sql'
        )

        task_create_google_tables = SQLExecuteQueryOperator(
            task_id='create_google_tables',
            conn_id='postgres_conn',
            sql='SQL/DDL/weather/create_tables.sql'
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

            hook = PostgresHook(postgres_conn_id='postgres_conn')
            for bairro, dados in json_data.items():
                bairro_json = json.dumps({bairro: dados})
                hook.run(
                    "INSERT INTO bronze.google_api_data(data) VALUES (%s)",
                    parameters=(bairro_json,)
                )

        @task
        def task_insert_weather_bronze(json_data: dict):
            import json

            hook = PostgresHook(postgres_conn_id='postgres_conn')
            for bairro, dados in json_data.items():
                bairro_json = json.dumps({bairro: dados})
                hook.run(
                    "INSERT INTO bronze.weather_api_data(data) VALUES (%s)",
                    parameters=(bairro_json,)
                )

        chain(
            task_create_schemas,
            task_create_google_tables,
            task_create_google_triggers,
            task_create_weather_tables,
            task_create_weather_triggers,
            task_insert_weather_bronze(weather_data),
            task_insert_google_bronze(google_data)
        )

    with TaskGroup(group_id="push_to_s3") as push_to_s3:

        @task
        def save_weather_to_s3(weather_data: dict):
            """Salva dados do OpenWeatherMap no S3 em Parquet, um arquivo por bairro."""
            from io import BytesIO
            import pyarrow as pa
            import pyarrow.parquet as pq
            import boto3

            schema = pa.schema([
                ("bairro", pa.string()),
                ("latitude", pa.float64()),
                ("longitude", pa.float64()),
                ("data_hora", pa.string()),
                ("temperatura", pa.float64()),
                ("sensacao_termica", pa.float64()),
                ("temperatura_min", pa.float64()),
                ("temperatura_max", pa.float64()),
                ("umidade", pa.int64()),
                ("pressao", pa.int64()),
                ("velocidade_vento", pa.float64()),
                ("direcao_vento", pa.int64()),
                ("rajada_vento", pa.float64()),
                ("chuva_1h", pa.float64()),
                ("nuvens_porcentagem", pa.int64()),
                ("visibilidade", pa.int64()),
                ("descricao", pa.string()),
                ("nascer_sol", pa.string()),
                ("por_sol", pa.string()),
            ])

            s3_client = boto3.client(
                "s3",
                aws_access_key_id=get_parameter("/tcc/dev/airflow_aws_s3_key_id"),
                aws_secret_access_key=get_parameter("/tcc/dev/airflow_s3_secret"),
                region_name=get_parameter("/tcc/dev/aws_region")
            )
            bucket_name = get_parameter("/tcc/dev/aws_s3_bucket_bronze")
            agora = datetime.now()

            for bairro, info in weather_data.items():
                linha = {"bairro": bairro, "latitude": info["latitude"], "longitude": info["longitude"]}
                linha.update(info["data"])

                dados_colunar = {k: [v] for k, v in linha.items()}
                table = pa.Table.from_pydict(dados_colunar, schema=schema)

                metadata = {
                    b"layer": b"bronze",
                    b"destiny": b"silver",
                    b"fonte_nome": b"OpenWeatherMap",
                    b"fonte_cidade": bairro.encode(),
                    b"fonte_datetime_insert": agora.isoformat().encode()
                }
                table = table.replace_schema_metadata(metadata)

                path = (
                    f"openweathermap/{bairro.lower().replace(' ', '_')}/"
                    f"year={agora.year}/month={agora.month:02d}/day={agora.day:02d}/"
                    f"{bairro.lower().replace(' ', '_')}-{agora.isoformat()}.parquet"
                )

                buffer = BytesIO()
                pq.write_table(table, buffer, compression="snappy")
                buffer.seek(0)

                s3_client.put_object(Body=buffer.getvalue(), Bucket=bucket_name, Key=path)
                print(f"✅ Weather salvo: s3://{bucket_name}/{path}")

        @task
        def save_google_to_s3(google_data: dict):
            """Salva dados do Google Air Quality no S3 em Parquet, um arquivo por bairro."""
            from io import BytesIO
            import pyarrow as pa
            import pyarrow.parquet as pq
            import boto3
            import json

            # Google retorna dados aninhados (indexes, pollutants são listas)
            # Salvamos como JSON string dentro do Parquet para preservar a estrutura
            schema = pa.schema([
                ("bairro", pa.string()),
                ("datetime_aqi", pa.string()),
                ("indexes", pa.string()),       # JSON serializado
                ("pollutants", pa.string()),    # JSON serializado
            ])

            s3_client = boto3.client(
                "s1",
                aws_access_key_id=get_parameter("/tcc/dev/airflow_aws_s3_key_id"),
                aws_secret_access_key=get_parameter("/tcc/dev/airflow_s3_secret"),
                region_name=get_parameter("/tcc/dev/aws_region")
            )
            bucket_name = get_parameter("/tcc/dev/aws_s3_bucket_bronze")
            agora = datetime.now()

            for bairro, info in google_data.items():
                linha = {
                    "bairro": bairro,
                    "datetime_aqi": info["datetime"],
                    "indexes": json.dumps(info["indexes"]),
                    "pollutants": json.dumps(info["pollutants"]),
                }

                dados_colunar = {k: [v] for k, v in linha.items()}
                table = pa.Table.from_pydict(dados_colunar, schema=schema)

                metadata = {
                    b"layer": b"bronze",
                    b"destiny": b"silver",
                    b"fonte_nome": b"GoogleAirQuality",
                    b"fonte_cidade": bairro.encode(),
                    b"fonte_datetime_insert": agora.isoformat().encode()
                }
                table = table.replace_schema_metadata(metadata)

                path = (
                    f"google_air_quality/{bairro.lower().replace(' ', '_')}/"
                    f"year={agora.year}/month={agora.month:02d}/day={agora.day:02d}/"
                    f"{bairro.lower().replace(' ', '_')}-{agora.isoformat()}.parquet"
                )

                buffer = BytesIO()
                pq.write_table(table, buffer, compression="snappy")
                buffer.seek(0)

                s3_client.put_object(Body=buffer.getvalue(), Bucket=bucket_name, Key=path)
                print(f"✅ Google AQI salvo: s3://{bucket_name}/{path}")

        save_weather_to_s3(weather_data)
        save_google_to_s3(google_data)

    get_data >> push_to_postgres >> push_to_s3


air_quality_etl()