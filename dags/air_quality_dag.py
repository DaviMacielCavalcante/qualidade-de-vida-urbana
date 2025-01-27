from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import duckdb
import json

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
            latitude NUMERIC,
            longitude NUMERIC,     
            concentration_value DOUBLE,
            concentration_units TEXT
        )
    """)

    conn.executemany(
        "INSERT INTO pollutants_data VALUES (?, ?, ?,?, ?, ?)",
        [
            (
                date_time,  # Mesmo valor de `dateTime` para todos os poluentes
                pollutant["code"],
                latitude,
                longitude,
                pollutant["concentration"]["value"],
                pollutant["concentration"]["units"],
            )
            for pollutant in pollutants
        ]
    )

    conn.execute(f"""
        COPY (
            SELECT *
            FROM pollutants_data
        ) TO './data/bronze/pollutants_data.parquet' (FORMAT PARQUET);
    """)

    conn.close()   

def push_to_s3_raw():

    s3 = "desafio4-raw"

    hook = S3Hook('aws-teste', region_name='sa-east-1')
    
    hook.load_file(filename='./data/bronze/pollutants_data.parquet', key=f'pollutants_data_{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.parquet', bucket_name=s3)
    
def push_to_s3_silver():

    s3 = "desafio4-silver"

    hook = S3Hook('aws-teste', region_name='sa-east-1')
    
    conn = duckdb.connect()

    conn.execute("""
        CREATE TABLE pollutants_data AS SELECT * FROM './data/bronze/pollutants_data.parquet'        
    """)

    conn.execute("""
    CREATE TABLE pollutants_silver(
        data DATETIME NOT NULL,
        code VARCHAR(10) NOT NULL,
        latitude NUMERIC NOT NULL,
        longitude NUMERIC NOT NULL,
        values NUMERIC(5,2) NOT NULL,
        units VARCHAR(30) NOT NULL
    )
""")
    
    conn.execute("""
    INSERT INTO pollutants_silver SELECT * FROM pollutants_data
""")
    
    conn.execute("""
        COPY (
            SELECT *
            FROM pollutants_silver
        ) TO './data/silver/pollutants_data.parquet' (FORMAT PARQUET);
    """)

    conn.close()

    hook.load_file(filename='./data/silver/pollutants_data.parquet', key=f'pollutants_data_{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.parquet', bucket_name=s3)

def push_to_s3_gold():

    s3 = "desafio4-gold"

    hook = S3Hook('aws-teste', region_name='sa-east-1')
    
    conn = duckdb.connect()

    conn.execute("""
        CREATE TABLE pollutants_data AS SELECT * FROM './data/silver/pollutants_data.parquet'        
    """)

    conn.execute("""
    CREATE TABLE pollutants_gold(
        data DATETIME NOT NULL,
        code VARCHAR(10) NOT NULL,
        latitude NUMERIC NOT NULL,
        longitude NUMERIC NOT NULL,
        values NUMERIC(5,2) NOT NULL,
        units VARCHAR(30) NOT NULL
    )
""")
    
    conn.execute("""
    INSERT INTO pollutants_gold SELECT * FROM pollutants_data
""")
    
    conn.execute("""
    ALTER TABLE pollutants_gold
    ADD COLUMN year INT
""")
    
    conn.execute("""
    ALTER TABLE pollutants_gold
    ADD COLUMN month INT
""")
    
    conn.execute("""
    ALTER TABLE pollutants_gold
    ADD COLUMN day INT
""")
    
    conn.execute("""
    ALTER TABLE pollutants_gold
    ADD COLUMN time TIME
""")
    
    conn.execute("""
    UPDATE pollutants_gold
        SET year = EXTRACT(YEAR FROM CAST("data" AS DATE)),
            month = EXTRACT(MONTH FROM CAST("data" AS DATE)),
            day = EXTRACT(DAY FROM CAST("data" AS DATE)),
            time = CAST(strftime('%H:%M:%S', "data") AS TIME)
""")
    
    conn.execute("UPDATE pollutants_gold SET units = LOWER(units)")
    
    conn.execute("ALTER TABLE pollutants_gold DROP COLUMN data")
    
    
    conn.execute("""
        COPY (
            SELECT *
            FROM pollutants_gold
        ) TO './data/gold/pollutants_data.parquet' (FORMAT PARQUET);
    """)

    conn.close()

    hook.load_file(filename='./data/gold/pollutants_data.parquet', key=f'pollutants_data_{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.parquet', bucket_name=s3)

def push_to_s3_diamond():

    s3 = "desafio4-diamond"

    hook = S3Hook('aws-teste', region_name='sa-east-1')
    
    conn = duckdb.connect()

    conn.execute("""
        CREATE TABLE pollutants_data AS SELECT * FROM './data/gold/pollutants_data.parquet'        
    """)

    conn.execute("""CREATE TABLE pollutants_diamond(
        code_id INT,     
        latitude NUMERIC,
        longitude NUMERIC,
        values NUMERIC(5,2),
        units_id INT,
        year_id INT,
        month_id int,
        day_id INT,
        time TIME             
    )""")

    conn.execute("""
    CREATE TABLE codes AS
    SELECT ROW_NUMBER() OVER () AS id, code
    FROM (SELECT DISTINCT code FROM pollutants_data);
""")
    
    conn.execute("""
    CREATE TABLE years AS
    SELECT ROW_NUMBER() OVER () AS id, year
    FROM (SELECT DISTINCT year FROM pollutants_data);
""")
    
    conn.execute("""
    CREATE TABLE months AS
    SELECT ROW_NUMBER() OVER () AS id, month
    FROM (SELECT DISTINCT month FROM pollutants_data);
""")
    
    conn.execute("""
    CREATE TABLE days AS
    SELECT ROW_NUMBER() OVER () AS id, day
    FROM (SELECT DISTINCT day FROM pollutants_data);
""")
    
    conn.execute("""
    CREATE TABLE units AS
    SELECT ROW_NUMBER() OVER () AS id, units
    FROM (SELECT DISTINCT units FROM pollutants_data);
""")
    
    conn.execute("""
    INSERT INTO pollutants_diamond (code_id, values, latitude, longitude, units_id, year_id, month_id, day_id, time)
    SELECT
        (SELECT id FROM codes WHERE code = pollutants_data.code),
        values, latitude, longitude,
        (SELECT id FROM units WHERE units = pollutants_data.units),
        (SELECT id FROM years WHERE year = pollutants_data.year),
        (SELECT id FROM months WHERE month = pollutants_data.month),
        (SELECT id FROM days WHERE day = pollutants_data.day),
        pollutants_data.time
    FROM pollutants_data;    
""")
    
    conn.execute("""
        COPY (
            SELECT *
            FROM pollutants_diamond
        ) TO './data/diamond/pollutants_data.parquet' (FORMAT PARQUET);
    """)

    conn.execute("""
        COPY (
            SELECT *
            FROM years
        ) TO './data/diamond/years.parquet' (FORMAT PARQUET);
    """)

    conn.execute("""
        COPY (
            SELECT *
            FROM months
        ) TO './data/diamond/months.parquet' (FORMAT PARQUET);
    """)

    conn.execute("""
        COPY (
            SELECT *
            FROM days
        ) TO './data/diamond/days.parquet' (FORMAT PARQUET);
    """)

    conn.execute("""
        COPY (
            SELECT *
            FROM codes
        ) TO './data/diamond/codes.parquet' (FORMAT PARQUET);
    """)

    conn.execute("""
        COPY (
            SELECT *
            FROM units
        ) TO './data/diamond/units.parquet' (FORMAT PARQUET);
    """)

    conn.close()

    hook.load_file(filename='./data/diamond/pollutants_data.parquet', key=f'pollutants_data_{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.parquet', bucket_name=s3)

    hook.load_file(filename='./data/diamond/codes.parquet', key=f'codes_{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.parquet', bucket_name=s3)

    hook.load_file(filename='./data/diamond/years.parquet', key=f'years_{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.parquet', bucket_name=s3)

    hook.load_file(filename='./data/diamond/months.parquet', key=f'months_{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.parquet', bucket_name=s3)

    hook.load_file(filename='./data/diamond/days.parquet', key=f'days_{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.parquet', bucket_name=s3)

    hook.load_file(filename='./data/diamond/units.parquet', key=f'units_{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.parquet', bucket_name=s3)


def create_postgres_tables():
    hook = PostgresHook(postgres_conn_id='postgres_raw')

    create_query = """
        CREATE TABLE IF NOT EXISTS pollutants_raw(
        id SERIAL PRIMARY KEY,
        datetime VARCHAR(30) NOT NULL,
        latitude VARCHAR(30) NOT NULL,
        longitude VARCHAR(30) NOT NULL,
        pollutant VARCHAR(10) NOT NULL,
        value DECIMAL(5,2) NOT NULL,
        unit VARCHAR(30) NOT NULL
        )
    """

    hook.run(create_query)

    hook = PostgresHook(postgres_conn_id='postgres_silver')

    create_query = """
        CREATE TABLE IF NOT EXISTS pollutants_silver(
        id SERIAL PRIMARY KEY,
        datetime TIMESTAMP NOT NULL,
        latitude NUMERIC(16,15) NOT NULL,
        longitude NUMERIC(16,15) NOT NULL,
        pollutant VARCHAR(10) NOT NULL,
        value DECIMAL(5,2) NOT NULL,
        unit VARCHAR(30) NOT NULL
        )
    """

    hook.run(create_query)

    hook = PostgresHook(postgres_conn_id='postgres_gold')

    create_query = """
        CREATE TABLE IF NOT EXISTS pollutants_gold(
        id SERIAL PRIMARY KEY,
        datetime TIMESTAMP NOT NULL,
        latitude NUMERIC(16,15) NOT NULL,
        longitude NUMERIC(16,15) NOT NULL,
        pollutant VARCHAR(10) NOT NULL,
        value DECIMAL(5,2) NOT NULL,
        unit VARCHAR(30) NOT NULL,
        year INT NOT NULL,
        month INT NOT NULL,
        day INT NOT NULL,
        time TIMESTAMP NOT NULL
        )
    """

    hook.run(create_query)

    hook = PostgresHook(postgres_conn_id='postgres_diamond')

    create_query = """
        CREATE TABLE IF NOT EXISTS code(
            id SERIAL PRIMARY KEY,
            code VARCHAR(10) NOT NULL
        )
    """

    hook.run(create_query)

    create_query = """
        CREATE TABLE IF NOT EXISTS units(
            id SERIAL PRIMARY KEY,
            unit VARCHAR(30) NOT NULL
        )
    """

    hook.run(create_query)

    create_query = """
        CREATE TABLE IF NOT EXISTS years(
            id SERIAL PRIMARY KEY,
            year INT NOT NULL
        )
    """

    hook.run(create_query)

    create_query = """
        CREATE TABLE IF NOT EXISTS months(
            id SERIAL PRIMARY KEY,
            month INT NOT NULL
        )
    """

    hook.run(create_query)

    create_query = """
        CREATE TABLE IF NOT EXISTS days(
            id SERIAL PRIMARY KEY,
            day INT NOT NULL
        )
    """

    hook.run(create_query)

    create_query = """
        CREATE TABLE IF NOT EXISTS pollutants_diamond(
        id SERIAL PRIMARY KEY,
        code_id INTEGER NOT NULL REFERENCES code(id),
        latitude NUMERIC(16,15) NOT NULL,
        longitude NUMERIC(16,15) NOT NULL,
        value DECIMAL(5,2) NOT NULL,
        unit_id INTEGER NOT NULL REFERENCES units(id),
        year_id INTEGER NOT NULL REFERENCES years(id),
        month_id INTEGER NOT NULL REFERENCES months(id),
        day_id INTEGER NOT NULL REFERENCES days(id),
        time TIMESTAMP NOT NULL
        )
    """

    hook.run(create_query)


def push_to_postgres_raw():

    hook = PostgresHook(postgres_conn_id='postgres_raw')

    insert_query = """
        INSERT INTO 
    """

    return

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

    task_s3_raw = PythonOperator(
        task_id='push_to_s3_raw',
        python_callable=push_to_s3_raw
    )

    task_s3_silver = PythonOperator(
        task_id='push_to_s3_silver',
        python_callable=push_to_s3_silver
    )

    task_s3_gold = PythonOperator(
        task_id='push_to_s3_gold',
        python_callable=push_to_s3_gold
    )

    task_s3_diamond = PythonOperator(
        task_id='push_to_s3_diamond',
        python_callable=push_to_s3_diamond
    )

    task_create_tables = PythonOperator(
        task_id='create_postgres_tables',
        python_callable=create_postgres_tables
    )

    task_fetch >> task_generate_data >> [task_s3_raw, task_s3_silver, task_s3_gold, task_s3_diamond] >> task_create_tables