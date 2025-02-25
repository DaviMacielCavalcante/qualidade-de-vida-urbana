import pytest
from dags.air_quality_dag import air_quality_etl
from airflow.providers.postgres.hooks.postgres import PostgresHook

def test_create_postgres_tables(monkeypatch):
    
    queries_run = {
         'postgres_raw': [],
         'postgres_silver': [],
         'postgres_gold': [],
         'postgres_diamond': []
    }

    def fake_run(self, query, *args, **kwargs):
         queries_run[self.postgres_conn_id].append(query)
         return None

    monkeypatch.setattr(PostgresHook, "run", fake_run)

    dag = air_quality_etl()
    task = dag.get_task("push_to_postgres.create_postgres_tables")
    
    create_tables_func = task.python_callable

    create_tables_func()

    raw_queries = queries_run['postgres_raw']
    assert any("CREATE TABLE IF NOT EXISTS pollutants_raw" in q for q in raw_queries), "Query para pollutants_raw não foi executada"

    silver_queries = queries_run['postgres_silver']
    assert any("CREATE TABLE IF NOT EXISTS pollutants_silver" in q for q in silver_queries), "Query para pollutants_silver não foi executada"

    gold_queries = queries_run['postgres_gold']
    assert any("CREATE TABLE IF NOT EXISTS pollutants_gold" in q for q in gold_queries), "Query para pollutants_gold não foi executada"

    diamond_queries = queries_run['postgres_diamond']
    assert any("CREATE TABLE IF NOT EXISTS codes" in q for q in diamond_queries), "Query para codes não foi executada"
    assert any("CREATE TABLE IF NOT EXISTS units" in q for q in diamond_queries), "Query para units não foi executada"
    assert any("CREATE TABLE IF NOT EXISTS years" in q for q in diamond_queries), "Query para years não foi executada"
    assert any("CREATE TABLE IF NOT EXISTS months" in q for q in diamond_queries), "Query para months não foi executada"
    assert any("CREATE TABLE IF NOT EXISTS days" in q for q in diamond_queries), "Query para days não foi executada"
    assert any("CREATE TABLE IF NOT EXISTS pollutants_diamond" in q for q in diamond_queries), "Query para pollutants_diamond não foi executada"
