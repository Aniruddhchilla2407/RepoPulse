"""RepoPulse daily pipeline.

ingest -> bronze -> silver -> dq_gate -> load_warehouse -> dbt_run -> dbt_test

* One run processes one day (`ds`), hours start_hour..end_hour (DAG params).
* Every step is idempotent: re-running a day replaces the same partitions.
* dq_gate and dbt_test fail the run on bad data and are never retried (a retry cannot fix data).
* catchup is off on purpose; use `airflow dags backfill` to process past days explicitly.
"""

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

REPO = "/opt/repopulse"
PY = "/home/airflow/pipeline-venv/bin/python"
DBT = "/home/airflow/pipeline-venv/bin/dbt"
DEFAULT_END_HOUR = int(os.environ.get("REPOPULSE_DEFAULT_END_HOUR", "23"))

# `params.date` (optional override) falls back to the run's logical date.
RANGE_ARGS = (
    "--start {{ params.date or ds }} --end {{ params.date or ds }} "
    "--start-hour {{ params.start_hour }} --end-hour {{ params.end_hour }}"
)


def cmd(command: str) -> str:
    return f"set -euo pipefail; cd {REPO}; {command}"


default_args = {
    "owner": "repopulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(hours=3),
}

with DAG(
    dag_id="repopulse_pipeline",
    description="GH Archive -> Spark bronze/silver -> DQ gate -> DuckDB warehouse -> dbt",
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "date": Param("", type="string", description="YYYY-MM-DD; empty = the run's logical date"),
        "start_hour": Param(0, type="integer", minimum=0, maximum=23),
        "end_hour": Param(DEFAULT_END_HOUR, type="integer", minimum=0, maximum=23),
    },
    tags=["repopulse"],
    doc_md=__doc__,
) as dag:
    ingest = BashOperator(
        task_id="ingest",
        bash_command=cmd(f"{PY} -m repopulse.ingest {RANGE_ARGS}"),
        retries=4,
        retry_delay=timedelta(minutes=1),
    )

    bronze = BashOperator(
        task_id="bronze",
        bash_command=cmd(f"{PY} -m repopulse.jobs.bronze_job {RANGE_ARGS}"),
    )

    silver = BashOperator(
        task_id="silver",
        bash_command=cmd(f"{PY} -m repopulse.jobs.silver_job {RANGE_ARGS}"),
    )

    dq_gate = BashOperator(
        task_id="dq_gate",
        bash_command=cmd(f"{PY} -m repopulse.dq {RANGE_ARGS}"),
        retries=0,
    )

    load_warehouse = BashOperator(
        task_id="load_warehouse",
        bash_command=cmd(f"{PY} -m repopulse.load_warehouse {RANGE_ARGS}"),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=cmd(f"{DBT} run --project-dir dbt --profiles-dir dbt"),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=cmd(f"{DBT} test --project-dir dbt --profiles-dir dbt"),
        retries=0,
    )

    ingest >> bronze >> silver >> dq_gate >> load_warehouse >> dbt_run >> dbt_test