import pytest
import json
import responses
import re 
from datetime import datetime
from airflow.models import DagBag, Connection
from airflow.utils.session import create_session


@pytest.fixture(scope="session", autouse=True)
def setup_air_quality_connection():

    with create_session() as session:
        conn = session.query(Connection).filter(Connection.conn_id == "air_quality_connection").first()
        if not conn:
            conn = Connection(
                conn_id="air_quality_connection",
                conn_type="http",
                host="http://dummyhost"
            )
            session.add(conn)
            session.commit()

@pytest.fixture
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)

@responses.activate
def test_task_execution(dagbag):

    responses.add(
        responses.POST,
        re.compile(r"http://dummyhost/currentConditions:lookup.*"), 
        json={"status": "ok", "data": {"value": 42}},
        status=200
    )

    dag = dagbag.dags.get("air_quality_etl")
    assert dag is not None, "DAG 'air_quality_etl' não encontrada"

    task = dag.get_task("get_data.fetch_data")
    assert task is not None, "Task 'get_data.fetch_data' não encontrada"

    execution_date = datetime.now()
    context = {
        "dag": dag,
        "ti": None, 
        "execution_date": execution_date,
    }

    result = task.execute(context=context)

    if isinstance(result, str):
        result = json.loads(result)

    assert isinstance(result, dict), "O resultado não é um JSON"
    assert result.get("status") == "ok", "Resposta inesperada: status diferente de 'ok'"
    assert result.get("data", {}).get("value") == 42, "Resposta inesperada: valor incorreto"
