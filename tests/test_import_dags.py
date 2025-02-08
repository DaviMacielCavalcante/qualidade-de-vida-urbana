import pytest
from airflow.models import DagBag

@pytest.fixture
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)

def test_dag_import(dagbag):
    assert len(dagbag.import_errors) == 0, f"Erro ao importar DAGs: {dagbag.import_errors}"
