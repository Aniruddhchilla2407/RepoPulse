from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

DAG_FOLDER = Path(__file__).resolve().parents[1] / "airflow" / "dags"
ORDER = ["ingest", "bronze", "silver", "dq_gate", "load_warehouse", "dbt_run", "dbt_test"]


def load_dag():
    bag = DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)
    assert bag.import_errors == {}
    return bag.dags["repopulse_pipeline"]


def test_dag_imports_without_errors():
    assert load_dag().dag_id == "repopulse_pipeline"


def test_tasks_run_in_a_single_chain():
    dag = load_dag()
    assert [t.task_id for t in dag.topological_sort()] == ORDER
    for upstream, downstream in zip(ORDER, ORDER[1:]):
        assert dag.get_task(upstream).downstream_task_ids == {downstream}


def test_run_settings_and_retry_policy():
    dag = load_dag()
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.get_task("ingest").retries >= 2
    assert dag.get_task("dq_gate").retries == 0  # a retry cannot fix bad data
    assert dag.get_task("dbt_test").retries == 0


def test_range_params_have_defaults():
    dag = load_dag()
    for name in ("date", "start_hour", "end_hour"):
        assert name in dag.params