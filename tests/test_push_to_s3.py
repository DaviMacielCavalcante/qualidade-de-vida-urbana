import pytest
from dags.air_quality_dag import air_quality_etl
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

parametros = {}

def fake_load_file(self, filename, key, bucket_name, *args, **kwargs):
    parametros['filename'] = filename
    parametros['key'] = key
    parametros['bucket_name'] = bucket_name

    return None

def test_push_to_s3_raw(monkeypatch):
    
    monkeypatch.setattr(S3Hook, "load_file", fake_load_file)

    dag = air_quality_etl()
    task = dag.get_task("push_to_s3.push_to_s3_raw")

    push_to_s3_raw_func = task.python_callable

    push_to_s3_raw_func()

    assert parametros.get("bucket_name") == "desafio4-raw", "Bucket errado"

    assert parametros.get("filename") == "./data/bronze/pollutants_data.parquet", "Filename errado"

    assert parametros.get("key").startswith("pollutants_data_"), "arquivo errado"

def test_push_to_s3_silver(monkeypatch):
    
    monkeypatch.setattr(S3Hook, "load_file", fake_load_file)

    dag = air_quality_etl()
    task = dag.get_task("push_to_s3.push_to_s3_silver")

    push_to_s3_silver_func = task.python_callable

    push_to_s3_silver_func()

    assert parametros.get("bucket_name") == "desafio4-silver", "Bucket errado"

    assert parametros.get("filename") == "./data/silver/pollutants_data.parquet", "Filename errado"

    assert parametros.get("key").startswith("pollutants_data_"), "arquivo errado"

def test_push_to_s3_gold(monkeypatch):
    
    monkeypatch.setattr(S3Hook, "load_file", fake_load_file)

    dag = air_quality_etl()
    task = dag.get_task("push_to_s3.push_to_s3_gold")

    push_to_s3_gold_func = task.python_callable

    push_to_s3_gold_func()

    assert parametros.get("bucket_name") == "desafio4-gold", "Bucket errado"

    assert parametros.get("filename") == "./data/gold/pollutants_data.parquet", "Filename errado"

    assert parametros.get("key").startswith("pollutants_data_"), "arquivo errado"

def test_push_to_s3_diamond(monkeypatch):

    execucoes = []
    
    monkeypatch.setattr(S3Hook, "load_file", fake_load_file)

    dag = air_quality_etl()
    task = dag.get_task("push_to_s3.push_to_s3_diamond")

    push_to_s3_diamond_func = task.python_callable

    push_to_s3_diamond_func()

    for execucao in execucoes:
        assert execucao["bucket_name"] == "desafio4-diamond", f"Bucket errado: {execucao['bucket_name']}"
        
        nomes_validos = ["pollutants_data_", "codes_", "years_", "months_", "days_", "units_"]
        assert any(execucao["key"].startswith(nome) for nome in nomes_validos), f"Key {execucao['key']} não está no formato esperado"



        