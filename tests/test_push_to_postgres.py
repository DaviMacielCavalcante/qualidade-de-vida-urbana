import pytest
import duckdb
from dags.air_quality_dag import air_quality_etl
from airflow.providers.postgres.hooks.postgres import PostgresHook

def test_push_to_postgres_raw(monkeypatch):
    class FakeDuckDBConnection:
        def __init__(self):
            self.table_created = False

        def execute(self, query, *args, **kwargs):
            if query.startswith("CREATE TABLE pollutants_raw AS SELECT * FROM read_parquet"):
                self.table_created = True
                return self  # Permite chaining
            elif query.startswith("SELECT * FROM pollutants_raw"):
                class FakeResult:
                    def fetchall(self):
                        return [
                            ("2025-01-02T00:00:00Z", "PM2.5", "1.0", "2.0", 10.5, "ug/m3"),
                            ("2025-01-02T00:00:00Z", "O3", "1.0", "2.0", 15.2, "ug/m3")
                        ]
                return FakeResult()
            else:
                return self

        def close(self):
            pass

        def __getattr__(self, name):
            return lambda *args, **kwargs: None

    def fake_duckdb_connect(*args, **kwargs):
        return FakeDuckDBConnection()

    monkeypatch.setattr(duckdb, "connect", fake_duckdb_connect)

    postgres_calls = []

    def fake_run(self, query, *args, **kwargs):
        postgres_calls.append({
            "query": query,
            "parameters": kwargs.get("parameters")
        })
        return None

    monkeypatch.setattr(PostgresHook, "run", fake_run)

    dag = air_quality_etl()
    task = dag.get_task("push_to_postgres.push_to_postgres_raw")
    push_to_postgres_raw_func = task.python_callable

    push_to_postgres_raw_func()

    assert len(postgres_calls) == 2, f"Esperado 2 inserções, mas foram chamadas {len(postgres_calls)} vezes."

    expected_query = "INSERT INTO pollutants_raw(datetime, pollutant, latitude, longitude, value, unit) VALUES (%s, %s, %s, %s, %s, %s)"
    normalized_expected = " ".join(expected_query.split())

    for call in postgres_calls:
        normalized_query = " ".join(call["query"].split())
        assert normalized_query == normalized_expected, f"Query incorreta: {normalized_query}"
        params = call["parameters"]
        assert isinstance(params, tuple) and len(params) == 6, "Parâmetros incorretos para a inserção."

def test_push_to_postgres_silver(monkeypatch):
    class FakeDuckDBConnection:
        def __init__(self):
            self.table_created = False

        def execute(self, query, *args, **kwargs):
            if query.startswith("CREATE TABLE pollutants_silver AS SELECT * FROM read_parquet"):
                self.table_created = True
                return self 
            elif query.startswith("SELECT * FROM pollutants_silver"):
                class FakeResult:
                    def fetchall(self):
                        return [
                            ("2025-01-02T00:00:00Z", "PM2.5", "1.0", "2.0", 10.5, "ug/m3"),
                            ("2025-01-02T00:00:00Z", "O3", "1.0", "2.0", 15.2, "ug/m3")
                        ]
                return FakeResult()
            else:
                return self

        def close(self):
            pass

        def __getattr__(self, name):
            return lambda *args, **kwargs: None

    def fake_duckdb_connect(*args, **kwargs):
        return FakeDuckDBConnection()

    monkeypatch.setattr(duckdb, "connect", fake_duckdb_connect)

    postgres_calls = []

    def fake_run(self, query, *args, **kwargs):
        postgres_calls.append({
            "query": query,
            "parameters": kwargs.get("parameters")
        })
        return None

    monkeypatch.setattr(PostgresHook, "run", fake_run)

    dag = air_quality_etl()
    task = dag.get_task("push_to_postgres.push_to_postgres_silver")
    push_to_postgres_silver_func = task.python_callable

    push_to_postgres_silver_func()

    assert len(postgres_calls) == 2, f"Esperado 2 inserções, mas foram chamadas {len(postgres_calls)} vezes."

    expected_query = "INSERT INTO pollutants_silver(datetime, pollutant,latitude, longitude, value, unit) VALUES (%s, %s, %s, %s, %s, %s)"
    normalized_expected = " ".join(expected_query.split())

    for call in postgres_calls:
        normalized_query = " ".join(call["query"].split())
        assert normalized_query == normalized_expected, f"Query incorreta: {normalized_query}"
        params = call["parameters"]
        assert isinstance(params, tuple) and len(params) == 6, "Parâmetros incorretos para a inserção."

def test_push_to_postgres_gold(monkeypatch):
    class FakeDuckDBConnection:
        def __init__(self):
            self.table_created = False

        def execute(self, query, *args, **kwargs):
            if query.startswith("CREATE TABLE pollutants_gold AS SELECT * FROM read_parquet"):
                self.table_created = True
                return self 
            elif query.startswith("SELECT * FROM pollutants_gold"):
                class FakeResult:
                    def fetchall(self):
                        return [
                             ("PM2.5", "1.0", "2.0", 10.5, "ug/m3", 2025, 2, 1, "10:00:00"),
                            ("O3", "1.0", "2.0", 15.2, "ug/m3", 2025, 2, 1, "10:00:00")
                        ]
                return FakeResult()
            else:
                return self

        def close(self):
            pass

        def __getattr__(self, name):
            return lambda *args, **kwargs: None

    def fake_duckdb_connect(*args, **kwargs):
        return FakeDuckDBConnection()

    monkeypatch.setattr(duckdb, "connect", fake_duckdb_connect)

    postgres_calls = []

    def fake_run(self, query, *args, **kwargs):
        postgres_calls.append({
            "query": query,
            "parameters": kwargs.get("parameters")
        })
        return None

    monkeypatch.setattr(PostgresHook, "run", fake_run)

    dag = air_quality_etl()
    task = dag.get_task("push_to_postgres.push_to_postgres_gold")
    push_to_postgres_gold_func = task.python_callable

    push_to_postgres_gold_func()

    assert len(postgres_calls) == 2, f"Esperado 2 inserções, mas foram chamadas {len(postgres_calls)} vezes."

    expected_query = "INSERT INTO pollutants_gold(pollutant,latitude, longitude, value, unit, year, month, day, time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

    normalized_expected = " ".join(expected_query.split())

    for call in postgres_calls:
        normalized_query = " ".join(call["query"].split())
        assert normalized_query == normalized_expected, f"Query incorreta: {normalized_query}"
        params = call["parameters"]
        assert isinstance(params, tuple) and len(params) == 9, "Parâmetros incorretos para a inserção."

def test_push_to_postgres_diamond(monkeypatch):
    class FakeDuckDBConnection:
        def __init__(self):
            self.table_created = False

        def execute(self, query, *args, **kwargs):
            if query.startswith("CREATE TABLE pollutants_gold AS SELECT * FROM read_parquet"):
                self.table_created = True
                return self
            elif query.startswith("SELECT * FROM pollutants_gold"):
                class FakeResult:
                    def fetchall(self):
                        return [
                            ("PM2.5", "1.0", "2.0", "10.5", "ug/m3", 2025, 2, 1, "10:00:00", "2025-01-02T00:00:00Z"),
                            ("O3", "1.5", "2.5", "15.2", "ug/m3", 2025, 2, 1, "10:00:00", "2025-01-02T00:00:00Z")
                        ]

                return FakeResult()
            elif query.startswith("CREATE TABLE days AS SELECT DISTINCT"):
                return self
            elif query.startswith("SELECT * FROM days"):
                class FakeResultDays:
                    def fetchall(self):
                        return [(1,)]
                return FakeResultDays()
            elif query.startswith("CREATE TABLE code AS SELECT DISTINCT"):
                return self
            elif query.startswith("SELECT * FROM code"):
                class FakeResultCodes:
                    def fetchall(self):
                        return [("PM2.5",)]
                return FakeResultCodes()
            elif query.startswith("CREATE TABLE units AS SELECT DISTINCT"):
                return self
            elif query.startswith("SELECT * FROM units"):
                class FakeResultUnits:
                    def fetchall(self):
                        return [("ug/m3",)]
                return FakeResultUnits()
            elif query.startswith("CREATE TABLE years AS SELECT DISTINCT"):
                return self
            elif query.startswith("SELECT * FROM years"):
                class FakeResultYears:
                    def fetchall(self):
                        return [(2025,)]
                return FakeResultYears()
            elif query.startswith("CREATE TABLE months AS SELECT DISTINCT"):
                return self
            elif query.startswith("SELECT * FROM months"):
                class FakeResultMonths:
                    def fetchall(self):
                        return [(2,)]
                return FakeResultMonths()
            else:
                return self

        def close(self):
            pass

        def __getattr__(self, name):
            return lambda *args, **kwargs: None

    def fake_duckdb_connect(*args, **kwargs):
        return FakeDuckDBConnection()

    monkeypatch.setattr(duckdb, "connect", fake_duckdb_connect)

    postgres_calls = []
    def fake_run(self, query, *args, **kwargs):
        postgres_calls.append({
            "query": query,
            "parameters": kwargs.get("parameters")
        })
        return None

    monkeypatch.setattr(PostgresHook, "run", fake_run)

    fake_executemany_calls = []
    class FakeCursor:
        def executemany(self, query, data):
            fake_executemany_calls.append({
                "query": query,
                "data": data
            })
        def close(self):
            pass
    class FakePostgresConnection:
        def cursor(self):
            return FakeCursor()
        def commit(self):
            pass
        def close(self):
            pass
    def fake_get_conn(self):
        return FakePostgresConnection()
    monkeypatch.setattr(PostgresHook, "get_conn", fake_get_conn)

    dag = air_quality_etl()
    task = dag.get_task("push_to_postgres.push_to_postgres_diamond")
    push_to_postgres_diamond_func = task.python_callable

    push_to_postgres_diamond_func()

    assert len(fake_executemany_calls) == 1, "Esperado 1 chamada a executemany para a inserção principal na tabela pollutants_diamond"

    expected_main_query = """
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
    def normalize(s):
        return " ".join(s.split())
    normalized_expected = normalize(expected_main_query)
    normalized_actual = normalize(fake_executemany_calls[0]["query"])
    assert normalized_actual == normalized_expected, f"Query de inserção principal incorreta: {normalized_actual}"

    inserted_data = fake_executemany_calls[0]["data"]
    assert isinstance(inserted_data, list)
    assert len(inserted_data) == 2, f"Esperado 2 linhas inseridas, mas foram inseridas {len(inserted_data)}"
