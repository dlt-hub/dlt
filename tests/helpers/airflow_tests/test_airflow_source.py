import argparse

import pytest
from airflow import DAG
from airflow.cli.commands.db_command import resetdb
from airflow.configuration import conf
from airflow.exceptions import TaskNotFound
from airflow.utils.state import State, DagRunState
from airflow.utils.types import DagRunType

import dlt
from dlt.common import pendulum
from dlt.helpers.airflow.pipeline.pipeline import DltAirflowPipeline
from dlt.helpers.airflow.source.source import DltAirflowSource

DEFAULT_DATE = pendulum.datetime(2022, 3, 4, tz='Europe/Berlin')
RECORDS = 100
DESTINATION = "duckdb"


@pytest.fixture(scope="session", autouse=True)
def initialize_airflow_db():
    # Disable loading examples
    conf.set("core", "load_examples", "False")

    # Prepare the arguments for the initdb function
    args = argparse.Namespace()
    args.backend = conf.get(section='core', key='sql_alchemy_conn')

    # Run Airflow resetdb before running any tests
    args.yes = True
    args.skip_init = False
    resetdb(args)


@dlt.source
def mock_source():
    @dlt.resource
    def dummy_data():
        for _ in range(RECORDS):
            yield f"dummy_data_{_}"

    return dummy_data()


def test_airflow_dag_source_task_execution():
    """Tests the execution of DltAirflowSource tasks within a DAG."""
    dag = DAG(dag_id="test", default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
    with DltAirflowPipeline(
        name="test_pipeline",
        destination=DESTINATION,
        dataset_name="test_dataset",
        full_refresh=False,
        dag=dag
    ) as _pipeline:
        DltAirflowSource(
            name="test_source",
            source=mock_source(),
            dag=dag,
        )

    dag_run = dag.create_dagrun(state=DagRunState.RUNNING,
                                execution_date=DEFAULT_DATE,
                                start_date=DEFAULT_DATE,
                                run_type=DagRunType.MANUAL)

    # Check if the task is in the DAG
    try:
        ti = dag_run.get_task_instance("test_pipeline.test_source")
    except TaskNotFound:
        pytest.fail("Task 'test_pipeline.test_source' not found in the DAG")

    # Test the task execution
    ti.task = dag.get_task(task_id="test_pipeline.test_source")
    ti.run()
    assert ti.state == State.SUCCESS
