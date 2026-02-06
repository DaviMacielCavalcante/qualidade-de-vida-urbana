from datetime import datetime, timedelta
from airflow.sdk import dag, task, TaskGroup
from airflow.models import Variable
from aws.secrets import get_parameter
from plugins.constants.constants import (
    OPEN_WEATHER_API_KEY_PATH,
    S3_REGION_PATH,
    AIRFLOW_AWS_S3_SECRET_KEY_ID_PATH,
    AIRFLOW_AWS_S3_SECRET_PATH,
    S3_BUCKET_BRONZE_PATH
)
import boto3


@dag(
    start_date=datetime(2025, 1, 6),
    schedule=timedelta(hours=1),
    catchup=False,
    description='OpenWeatherMap One Call API',
    tags=['openweather', 's3']
)
def openweather_dag():

    with TaskGroup(group_id="get_data") as get_data:

        @task
        def fetch_current_weather():
            import requests

            
            api_key = get_parameter(OPEN_WEATHER_API_KEY_PATH)
            
            
            lat = "-1.2969"
            lon = "-47.9219"

            

            response = requests.get(
                "https://api.openweathermap.org/data/3.0/onecall",
                params={
                    "appid": api_key,
                    "lat": lat,
                    "lon": lon,
                    "units": "metric",
                    "lang": "pt_br",
                    "exclude": "minutely,hourly,daily,alerts" 
                }
            )
            
            
            response.raise_for_status()
            weather_data = response.json()
            
            
            current = weather_data.get("current", {})
            
            if not current:
                raise Exception("Resposta da API não contém campo 'current'")
            
            
            dt_local = datetime.fromtimestamp(current["dt"])

            dados = {
                "data_hora": dt_local.strftime("%Y-%m-%d %H:%M:%S"),
                "temperatura": current.get("temp", 0.0),
                "sensacao_termica": current.get("feels_like", 0.0),
                "umidade": current.get("humidity", 0),
                "pressao": current.get("pressure", 0),
                "velocidade_vento": current.get("wind_speed", 0.0),
                "direcao_vento": current.get("wind_deg", 0),
                "rajada_vento": current.get("wind_gust", 0.0),
                "chuva_1h": current.get("rain", {}).get("1h", 0.0) if isinstance(current.get("rain"), dict) else 0.0,
                "nuvens_porcentagem": current.get("clouds", 0),
                "visibilidade": current.get("visibility", 0),
                "descricao": current.get("weather", [{}])[0].get("description", "N/A"),
                "nascer_sol": datetime.fromtimestamp(current.get("sunrise", 0)).strftime("%H:%M:%S") if current.get("sunrise") else "00:00:00",
                "por_sol": datetime.fromtimestamp(current.get("sunset", 0)).strftime("%H:%M:%S") if current.get("sunset") else "00:00:00",
                "indice_uv": current.get("uvi", 0.0),
                "ponto_orvalho": current.get("dew_point", 0.0),
            }

            
            return dados

        @task
        def save_current_to_s3(dados: dict):
            """
            Salva dado atual no S3 (camada bronze).
            Estrutura: s3://bucket/openweathermap/castanhal/year=2026/month=02/day=05/castanhal-timestamp.parquet
            """
            import pyarrow as pa
            import pyarrow.parquet as pq
            from io import BytesIO

            
            s3_client = boto3.client(
                's3',
                aws_access_key_id=get_parameter(AIRFLOW_AWS_S3_SECRET_KEY_ID_PATH),
                aws_secret_access_key=get_parameter(AIRFLOW_AWS_S3_SECRET_PATH),
                region_name=get_parameter(S3_REGION_PATH)
            )

            bucket_name = get_parameter(S3_BUCKET_BRONZE_PATH)


            
            schema = pa.schema([
                ("data_hora", pa.string()),
                ("temperatura", pa.float64()),
                ("sensacao_termica", pa.float64()),
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
                ("indice_uv", pa.float64()),
                ("ponto_orvalho", pa.float64()),
            ])

            
            dados_colunar = {k: [v] for k, v in dados.items()}
            table = pa.Table.from_pydict(dados_colunar, schema=schema)
            
            
            table = table.replace_schema_metadata({
                b'layer': b'bronze',
                b'destiny': b'silver',
                b'fonte_nome': b'OpenWeatherMap_OneCall',
                b'fonte_estacao': b'CASTANHAL',
                b'fonte_datetime_insert': datetime.now().isoformat().encode(),
                b'api_version': b'3.0'
            })

            agora = datetime.now()
            
            
            path_parquet_s3 = (
                f"openweathermap/castanhal/"
                f"year={agora.year}/month={agora.month:02d}/day={agora.day:02d}/"
                f"castanhal-{agora.isoformat()}.parquet"
            )

            
            buffer = BytesIO()
            pq.write_table(table, buffer, compression="snappy")
            buffer.seek(0)

            
            s3_client.put_object(
                Body=buffer.getvalue(),
                Bucket=bucket_name,
                Key=path_parquet_s3
            )

            file_size = len(buffer.getvalue())
            s3_uri = f"s3://{bucket_name}/{path_parquet_s3}"

            

            return s3_uri

        
        @task
        def fetch_historical_weather():
            
            import requests

            
            api_key = get_parameter(OPEN_WEATHER_API_KEY_PATH)
            
            
            lat = "-1.2969"
            lon = "-47.9219"

            
            dias_historicos = int(Variable.get('HISTORICAL_DAYS', default_var='7'))

            dados_historicos = []

            

            for dias_atras in range(dias_historicos, 0, -1):
                alvo = datetime.now() - timedelta(days=dias_atras)
                timestamp_unix = int(alvo.replace(hour=0, minute=0, second=0).timestamp())

                

                try:
                    response = requests.get(
                        "https://api.openweathermap.org/data/3.0/onecall/timemachine",
                        params={
                            "appid": api_key,
                            "lat": lat,
                            "lon": lon,
                            "dt": timestamp_unix,
                            "units": "metric",
                            "lang": "pt_br"
                        }
                    )


                    response.raise_for_status()
                    dia_data = response.json()

                    
                    dados_horas = dia_data.get("data") or dia_data.get("hourly", [])

                    
                    for hora in dados_horas:
                        dt_hora = datetime.fromtimestamp(hora["dt"])
                        dados_historicos.append({
                            "data_hora": dt_hora.strftime("%Y-%m-%d %H:%M:%S"),
                            "temperatura": hora.get("temp", 0.0),
                            "sensacao_termica": hora.get("feels_like", 0.0),
                            "umidade": hora.get("humidity", 0),
                            "pressao": hora.get("pressure", 0),
                            "velocidade_vento": hora.get("wind_speed", 0.0),
                            "direcao_vento": hora.get("wind_deg", 0),
                            "rajada_vento": hora.get("wind_gust", 0.0),
                            "chuva_1h": hora.get("rain", {}).get("1h", 0.0) if isinstance(hora.get("rain"), dict) else 0.0,
                            "nuvens_porcentagem": hora.get("clouds", 0),
                            "visibilidade": hora.get("visibility", 0),
                            "descricao": hora.get("weather", [{}])[0].get("description", "N/A"),
                            "indice_uv": hora.get("uvi", 0.0),
                            "ponto_orvalho": hora.get("dew_point", 0.0),
                        })

                except requests.exceptions.RequestException as e:
                    continue

            

            return dados_historicos

        @task
        def save_historical_to_s3(dados_historicos: list):
            
            if not dados_historicos:
                return []

            from collections import defaultdict
            import pyarrow as pa
            import pyarrow.parquet as pq
            from io import BytesIO

            
            s3_client = boto3.client(
                's3',
                aws_access_key_id=get_parameter(AIRFLOW_AWS_S3_SECRET_KEY_ID_PATH),
                aws_secret_access_key=get_parameter(AIRFLOW_AWS_S3_SECRET_PATH),
                region_name=get_parameter(S3_REGION_PATH)
            )

            bucket_name = get_parameter(S3_BUCKET_BRONZE_PATH)

            
            schema = pa.schema([
                ("data_hora", pa.string()),
                ("temperatura", pa.float64()),
                ("sensacao_termica", pa.float64()),
                ("umidade", pa.int64()),
                ("pressao", pa.int64()),
                ("velocidade_vento", pa.float64()),
                ("direcao_vento", pa.int64()),
                ("rajada_vento", pa.float64()),
                ("chuva_1h", pa.float64()),
                ("nuvens_porcentagem", pa.int64()),
                ("visibilidade", pa.int64()),
                ("descricao", pa.string()),
                ("indice_uv", pa.float64()),
                ("ponto_orvalho", pa.float64()),
            ])

            
            por_dia = defaultdict(list)
            for registro in dados_historicos:
                dia = registro["data_hora"][:10]
                por_dia[dia].append(registro)

            arquivos_salvos = []

            

            for dia, registros in sorted(por_dia.items()):
                ano, mes, dia_num = dia.split("-")

                dados_colunar = {campo: [r[campo] for r in registros] for campo in schema.names}
                table = pa.Table.from_pydict(dados_colunar, schema=schema)

                table = table.replace_schema_metadata({
                    b'layer': b'bronze',
                    b'destiny': b'silver',
                    b'fonte_nome': b'OpenWeatherMap_OneCall',
                    b'fonte_estacao': b'CASTANHAL',
                    b'fonte_datetime_insert': datetime.now().isoformat().encode(),
                    b'api_version': b'3.0'
                })

                path_parquet_s3 = (
                    f"openweathermap/historical/castanhal/"
                    f"year={ano}/month={mes}/day={dia_num}/"
                    f"castanhal-{dia}.parquet"
                )

                buffer = BytesIO()
                pq.write_table(table, buffer, compression="snappy")
                buffer.seek(0)

                s3_client.put_object(
                    Body=buffer.getvalue(),
                    Bucket=bucket_name,
                    Key=path_parquet_s3
                )

                file_size = len(buffer.getvalue())
                s3_uri = f"s3://{bucket_name}/{path_parquet_s3}"
                arquivos_salvos.append(s3_uri)
            
            return arquivos_salvos

        current = fetch_current_weather()
        save_current_to_s3(current)

        historical = fetch_historical_weather()
        save_historical_to_s3(historical)


openweather_dag()