import os
import json
import duckdb
import tempfile
import pytest
from dags.air_quality_dag import air_quality_etl

def test_generate_data(monkeypatch):
   
    with tempfile.TemporaryDirectory() as tmpdir:
        
        original_connect = duckdb.connect

        # Cria uma classe wrapper para interceptar a execução do método "execute"
        class FakeDuckDBConnection:
            def __init__(self, conn, tmpdir):
                self._conn = conn
                self.tmpdir = tmpdir

            def execute(self, query, *args, **kwargs):
                # Se a query COPY for chamada, substitui o caminho fixo pelo diretório temporário
                if query.strip().upper().startswith("COPY"):
                    new_path = os.path.join(self.tmpdir, "pollutants_data.parquet")
                    query = query.replace("./data/bronze/pollutants_data.parquet", new_path)
                return self._conn.execute(query, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(self._conn, name)

        # Define uma nova função de conexão que retorna a nossa conexão "fake"
        def fake_connect(*args, **kwargs):
            conn = original_connect(*args, **kwargs)
            return FakeDuckDBConnection(conn, tmpdir)

        # Monkeypatch: substitui duckdb.connect pela nossa fake_connect
        monkeypatch.setattr(duckdb, "connect", fake_connect)

        
        dag = air_quality_etl()
        
        task = dag.get_task("get_data.generate_data")
        
        generate_data_func = task.python_callable

        
        sample_json = json.dumps({
            "dateTime": "2025-01-02T00:00:00Z",
            "pollutants": [
                {
                    "code": "PM2.5",
                    "concentration": {
                        "value": 10.5,
                        "units": "ug/m3"
                    }
                },
                {
                    "code": "O3",
                    "concentration": {
                        "value": 15.2,
                        "units": "ug/m3"
                    }
                }
            ]
        })

        generate_data_func(sample_json)

        
        output_file = os.path.join(tmpdir, "pollutants_data.parquet")
        assert os.path.exists(output_file), "O arquivo Parquet não foi criado"

        
        con = duckdb.connect()
        df = con.execute(f"SELECT * FROM read_parquet('{output_file}')").fetchdf()
        con.close()

        
        assert len(df) == 2, "Número de registros diferente do esperado"
        pm25 = df[df['code'] == 'PM2.5']
        o3 = df[df['code'] == 'O3']
        assert not pm25.empty and not o3.empty, "Dados dos poluentes não encontrados"
