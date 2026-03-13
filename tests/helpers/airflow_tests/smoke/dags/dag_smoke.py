"""Smoke test DAG for dlt + Airflow integration.

Run with the real Airflow scheduler — no dag.test() or test harness.
Set DLT_SMOKE_DB_PATH env var to control where the sqlite destination file is stored.
"""

import os
from datetime import datetime

from airflow import DAG

import dlt
from dlt.helpers.airflow_helper import PipelineTasksGroup


@dlt.source
def smoke_source():
    @dlt.resource
    def users():
        yield [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]

    @dlt.resource
    def events():
        yield [
            {"id": 1, "user_id": 1, "action": "login"},
            {"id": 2, "user_id": 2, "action": "click"},
            {"id": 3, "user_id": 1, "action": "logout"},
            {"id": 4, "user_id": 3, "action": "login"},
        ]

    return users, events


DB_PATH = os.environ.get("DLT_SMOKE_DB_PATH", "/tmp/dlt_smoke/smoke.db")
DATASET_NAME = "smoke_data"

with DAG(
    dag_id="dlt_smoke",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    tasks = PipelineTasksGroup("dlt_smoke_tasks", wipe_local_data=True)

    pipeline = dlt.pipeline(
        pipeline_name="dlt_smoke",
        dataset_name=DATASET_NAME,
        destination=dlt.destinations.sqlalchemy(credentials=f"sqlite:///{DB_PATH}"),
    )

    # test decompose="none" — single task for all resources
    none_tasks = tasks.add_run(
        pipeline,
        smoke_source(),
        decompose="none",
    )

    # test decompose="serialize" — sequential tasks per component
    ser_tasks = tasks.add_run(
        pipeline,
        smoke_source(),
        decompose="serialize",
    )

    # test decompose="parallel" with serialize_first_task=False
    par_tasks = tasks.add_run(
        pipeline,
        smoke_source(),
        decompose="parallel",
        serialize_first_task=False,
    )

    # serialize runs after none, parallel runs independently
    none_tasks[-1] >> ser_tasks[0] >> par_tasks[0]
