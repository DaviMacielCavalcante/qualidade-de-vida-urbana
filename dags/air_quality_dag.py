from datetime import datetime, timedelta
from airflow.providers.http.operators.http import HttpOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag,task
from airflow.utils.task_group import TaskGroup
import duckdb
import json

API_KEY = Variable.get('API_KEY')
endpoint = f'currentConditions:lookup?key={API_KEY}'
latitude = Variable.get('LATITUDE')
longitude = Variable.get('LONGITUDE')
headers = {
    "Content-Type": "application/json"
    }

@dag(start_date=datetime(2025, 1, 2), schedule=timedelta(hours=8), catchup=False, description='ETL for Air Quality Data', tags=['air_quality'])
def air_quality_etl():

    with TaskGroup(group_id="get_data") as get_data:

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

        @task
        def generate_data(json_response: json):

            json_value = json.loads(json_response)

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

        generate_data(task_fetch.output)

    with TaskGroup(group_id="push_to_s3") as push_to_s3:

        @task
        def push_to_s3_raw():

            s3 = "desafio4-raw"

            hook = S3Hook('aws-teste', region_name='sa-east-1')
            
            hook.load_file(filename='./data/bronze/pollutants_data.parquet', key=f'pollutants_data_{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}.parquet', bucket_name=s3)

        @task
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

        @task
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

        @task
        def push_to_s3_diamond():

            s3 = "desafio4-diamond"

            hook = S3Hook('aws-teste', region_name='sa-east-1')
            
            conn = duckdb.connect()

            conn.execute("""
                CREATE TABLE pollutants_data AS SELECT * FROM './data/gold/pollutants_data.parquet'        
            """)

            conn.execute("""CREATE TABLE pollutants_diamond(
                code VARCHAR,     
                latitude NUMERIC,
                longitude NUMERIC,
                values NUMERIC(5,2),
                units VARCHAR(30),
                year INT,
                month INT,
                day INT,
                time TIME             
            )""")

            conn.execute("""
            CREATE TABLE codes AS
            SELECT DISTINCT code FROM pollutants_data;
        """)
            
            conn.execute("""
            CREATE TABLE years AS SELECT DISTINCT year FROM pollutants_data;
        """)
            
            conn.execute("""
            CREATE TABLE months AS SELECT DISTINCT month FROM pollutants_data;
        """)
            
            conn.execute("""
            CREATE TABLE days AS SELECT DISTINCT day FROM pollutants_data;
        """)
            
            conn.execute("""
            CREATE TABLE units AS SELECT DISTINCT units FROM pollutants_data;
        """)
            
            conn.execute("""
            INSERT INTO pollutants_diamond (code, values, latitude, longitude, units, year, month, day, time)
            SELECT
                code,
                values, latitude, longitude,
                units,
                year,
                month,
                day,
                time
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

        push_to_s3_raw() >> push_to_s3_silver() >> push_to_s3_gold() >> push_to_s3_diamond()


    with TaskGroup(group_id="push_to_postgres") as push_to_postgres:

        @task
        def create_postgres_tables():
            hook = PostgresHook(postgres_conn_id='postgres_raw')

            create_query = """
                CREATE TABLE IF NOT EXISTS pollutants_raw(
                id SERIAL PRIMARY KEY,
                datetime VARCHAR(30) NOT NULL,
                pollutant VARCHAR(10) NOT NULL,
                latitude VARCHAR(30) NOT NULL,
                longitude VARCHAR(30) NOT NULL,
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
                pollutant VARCHAR(10) NOT NULL,
                latitude NUMERIC(6,3) NOT NULL,
                longitude NUMERIC(6,3) NOT NULL,
                value DECIMAL(5,2) NOT NULL,
                unit VARCHAR(30) NOT NULL
                )
            """

            hook.run(create_query)

            hook = PostgresHook(postgres_conn_id='postgres_gold')

            create_query = """
                CREATE TABLE IF NOT EXISTS pollutants_gold(
                id SERIAL PRIMARY KEY,
                pollutant VARCHAR(10) NOT NULL,
                latitude NUMERIC(6,3) NOT NULL,
                longitude NUMERIC(6,3) NOT NULL,
                value DECIMAL(5,2) NOT NULL,
                unit VARCHAR(30) NOT NULL,
                year INT NOT NULL,
                month INT NOT NULL,
                day INT NOT NULL,
                time TIME WITHOUT TIME ZONE NOT NULL
                )
            """

            hook.run(create_query)

            hook = PostgresHook(postgres_conn_id='postgres_diamond')

            create_query = """
                CREATE TABLE IF NOT EXISTS codes(
                    id SERIAL PRIMARY KEY,
                    code VARCHAR(10) NOT NULL UNIQUE
                )
            """

            hook.run(create_query)

            create_query = """
                CREATE TABLE IF NOT EXISTS units(
                    id SERIAL PRIMARY KEY,
                    unit VARCHAR(30) NOT NULL UNIQUE
                )
            """

            hook.run(create_query)

            create_query = """
                CREATE TABLE IF NOT EXISTS years(
                    id SERIAL PRIMARY KEY,
                    year INT NOT NULL UNIQUE
                )
            """

            hook.run(create_query)

            create_query = """
                CREATE TABLE IF NOT EXISTS months(
                    id SERIAL PRIMARY KEY,
                    month INT NOT NULL UNIQUE
                )
            """

            hook.run(create_query)

            create_query = """
                CREATE TABLE IF NOT EXISTS days(
                    id SERIAL PRIMARY KEY,
                    day INT NOT NULL UNIQUE
                )
            """

            hook.run(create_query)

            create_query = """
                CREATE TABLE IF NOT EXISTS pollutants_diamond(
                id SERIAL PRIMARY KEY,
                code_id INTEGER NOT NULL REFERENCES codes(id),
                latitude NUMERIC(6,3) NOT NULL,
                longitude NUMERIC(6,3) NOT NULL,
                value DECIMAL(5,2) NOT NULL,
                unit_id INTEGER NOT NULL REFERENCES units(id),
                year_id INTEGER NOT NULL REFERENCES years(id),
                month_id INTEGER NOT NULL REFERENCES months(id),
                day_id INTEGER NOT NULL REFERENCES days(id),
                time TIME WITHOUT TIME ZONE NOT NULL
                )
            """

            hook.run(create_query)

        @task
        def push_to_postgres_raw():

            hook = PostgresHook(postgres_conn_id='postgres_raw')

            conn = duckdb.connect()

            conn.execute("CREATE TABLE pollutants_raw AS SELECT * FROM read_parquet('./data/bronze/pollutants_data.parquet')")
            data = conn.execute("SELECT * FROM pollutants_raw").fetchall()

            insert_query = """
                INSERT INTO pollutants_raw(datetime, pollutant, latitude, longitude, value, unit)  
                VALUES (%s, %s, %s, %s, %s, %s)
            """

            for row in data:
                hook.run(insert_query, parameters=row)

            conn.close()

        @task
        def push_to_postgres_silver():

            hook = PostgresHook(postgres_conn_id='postgres_silver')

            conn = duckdb.connect()

            conn.execute("CREATE TABLE pollutants_silver AS SELECT * FROM read_parquet('./data/silver/pollutants_data.parquet')")
            data = conn.execute("SELECT * FROM pollutants_silver").fetchall()

            insert_query = """
                INSERT INTO pollutants_silver(datetime, pollutant,latitude, longitude, value, unit)  
                VALUES (%s, %s, %s, %s, %s, %s)
            """

            for row in data:
                hook.run(insert_query, parameters=row)

            conn.close()

        @task
        def push_to_postgres_gold():

            hook = PostgresHook(postgres_conn_id='postgres_gold')

            conn = duckdb.connect()

            conn.execute("CREATE TABLE pollutants_gold AS SELECT * FROM read_parquet('./data/gold/pollutants_data.parquet')")
            data = conn.execute("SELECT * FROM pollutants_gold").fetchall()
            insert_query = """
                INSERT INTO pollutants_gold(pollutant,latitude, longitude, value, unit, year, month, day, time)  
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            for row in data:
                hook.run(insert_query, parameters=row)

            conn.close()

        @task
        def push_to_postgres_diamond():

            hook = PostgresHook(postgres_conn_id='postgres_diamond')

            conn = duckdb.connect()

            # Tabela principal

            conn.execute("CREATE TABLE pollutants_gold AS SELECT * FROM read_parquet('./data/gold/pollutants_data.parquet')")

            data_main = conn.execute("SELECT * FROM pollutants_gold").fetchall()

            # Tabela dias

            conn.execute("CREATE TABLE days AS SELECT DISTINCT * FROM read_parquet('./data/diamond/days.parquet')").fetchall()

            days_data = conn.execute("SELECT * FROM days").fetchall()

            insert_query = """
                INSERT INTO days(day)  
                VALUES (%s)
            """    

            try:
                for row in days_data:
                    hook.run(insert_query, parameters=row)
            except Exception as e:
                print("Erro ao inserir dados na tabela days:", e)
            finally:
                print("Dados duplicados, pulando etapa.")

            # Tabela codes

            conn.execute("CREATE TABLE code AS SELECT DISTINCT * FROM read_parquet('./data/diamond/codes.parquet')").fetchall()

            code_data = conn.execute("SELECT * FROM code").fetchall()

            insert_query = """
                INSERT INTO codes(code)  
                VALUES (%s)
            """    
            try:
                for row in code_data:
                    hook.run(insert_query, parameters=row)
            except Exception as e:
                print("Erro ao inserir dados na tabela codes:", e)
            finally:
                print("Dados duplicados, pulando etapa.")

            # Tabela units

            conn.execute("CREATE TABLE units AS SELECT DISTINCT * FROM read_parquet('./data/diamond/units.parquet')").fetchall()

            units_data = conn.execute("SELECT * FROM units").fetchall()

            insert_query = """
                INSERT INTO units(unit)  
                VALUES (%s)
            """    

            try:
                for row in units_data:
                    hook.run(insert_query, parameters=row)
            except Exception as e:
                print("Erro ao inserir dados na tabela units:", e)
            finally:
                print("Dados duplicados, pulando etapa.")


            # Tabela years

            conn.execute("CREATE TABLE years AS SELECT DISTINCT * FROM read_parquet('./data/diamond/years.parquet')").fetchall()

            years_data = conn.execute("SELECT * FROM years").fetchall()

            insert_query = """
                INSERT INTO years(year)  
                VALUES (%s)
            """    

            try:
                for row in years_data:
                    hook.run(insert_query, parameters=row)
            except Exception as e:
                print("Erro ao inserir dados na tabela years:", e)
            finally:
                print("Dados duplicados, pulando etapa.")

            # Tabela months

            conn.execute("CREATE TABLE months AS SELECT DISTINCT * FROM read_parquet('./data/diamond/months.parquet')").fetchall()

            months_data = conn.execute("SELECT * FROM months").fetchall()

            insert_query = """
                INSERT INTO months(month)  
                VALUES (%s)
            """    

            try:
                for row in months_data:
                    hook.run(insert_query, parameters=row)
            except Exception as e:
                print("Erro ao inserir dados na tabela months:", e)
            finally:
                print("Dados duplicados, pulando etapa.")


            # Tabela main

            hook = PostgresHook(postgres_conn_id='postgres_diamond')
            postgres_conn = hook.get_conn()
            cursor = postgres_conn.cursor()

            
            insert_query = """
                INSERT INTO pollutants_diamond (code_id, latitude, longitude, value, unit_id, year_id, month_id, day_id, time)
                SELECT
                    c.id AS code_id,
                    %s,  -- latitude (valor direto)
                    %s,  -- longitude (valor direto)
                    %s,  -- value (valor direto)
                    u.id AS unit_id,
                    y.id AS year_id,
                    m.id AS month_id,
                    d.id AS day_id,
                    %s::TIME  -- time (valor direto convertido para TIME)
                FROM codes c
                JOIN units u ON u.unit = %s
                JOIN years y ON y.year = %s
                JOIN months m ON m.month = %s
                JOIN days d ON d.day = %s
                WHERE c.code = %s;
            """

            
            data_to_insert = [
                (
                    float(row[1]),  # latitude
                    float(row[2]),  # longitude
                    float(row[3]),  # value
                    str(row[8]),  # time (convertido para string)
                    row[4],  # unit (para JOIN)
                    row[5],  # year (para JOIN)
                    row[6],  # month (para JOIN)
                    row[7],  # day (para JOIN)
                    row[0],  # code (para JOIN)
                )
                for row in data_main
            ]

            
            cursor.executemany(insert_query, data_to_insert)

            
            postgres_conn.commit()
            cursor.close()
            postgres_conn.close()

        create_postgres_tables() >> push_to_postgres_raw() >> push_to_postgres_silver() >> push_to_postgres_gold() >> push_to_postgres_diamond()
    
    get_data >> push_to_s3 >> push_to_postgres

air_quality_etl()