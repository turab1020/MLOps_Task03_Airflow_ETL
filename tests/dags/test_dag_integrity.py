"""DAG integrity tests: the DAG imports cleanly and has the expected shape."""
import os

import pytest
from airflow.models import DagBag

DAG_ID = "currency_exchange_etl"


@pytest.fixture(scope="module")
def dagbag():
    dags_folder = os.path.join(os.path.dirname(__file__), "..", "..", "dags")
    return DagBag(dag_folder=dags_folder)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, dagbag.import_errors


def test_dag_has_three_tasks_in_order(dagbag):
    dag = dagbag.dags.get(DAG_ID)
    assert dag is not None
    extract = dag.get_task("extract_exchange_rates")
    transform = dag.get_task("transform_exchange_rates")
    load = dag.get_task("load_exchange_rates")
    assert extract.downstream_task_ids == {"transform_exchange_rates"}
    assert transform.downstream_task_ids == {"load_exchange_rates"}
    assert load.downstream_task_ids == set()


def test_dag_is_scheduled_daily_with_retries(dagbag):
    dag = dagbag.dags.get(DAG_ID)
    assert dag.catchup is False
    for t in dag.tasks:
        assert t.retries >= 1
