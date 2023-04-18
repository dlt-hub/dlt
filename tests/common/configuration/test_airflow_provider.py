import argparse
import pytest
from airflow import DAG
from airflow.cli.commands.db_command import resetdb
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import State, DagRunState
from airflow.utils.types import DagRunType
from airflow.configuration import conf

from dlt.common import pendulum
from dlt.common.configuration.providers.airflow import (
    AirflowSecretsTomlProvider,
    AIRFLOW_SECRETS_TOML_VARIABLE_KEY,
)

DEFAULT_DATE = pendulum.datetime(2023, 4, 18, tz='Europe/Berlin')


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


# Test data
SECRETS_TOML_CONTENT = """
[sources]
api_key = "test_value"
"""


def test_airflow_secrets_toml_provider():
    dag = DAG(dag_id='test_dag', start_date=DEFAULT_DATE)

    def test_task():
        Variable.set(AIRFLOW_SECRETS_TOML_VARIABLE_KEY, SECRETS_TOML_CONTENT)

        provider = AirflowSecretsTomlProvider()

        api_key, _ = provider.get_value("api_key", str, "sources")

        return {
            'name': provider.name,
            'supports_secrets': provider.supports_secrets,
            'api_key_from_provider': api_key,
        }

    task = PythonOperator(
        task_id='test_task',
        python_callable=test_task,
        dag=dag
    )

    dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=DEFAULT_DATE,
        start_date=DEFAULT_DATE,
        run_type=DagRunType.MANUAL
    )

    ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

    ti.run()

    result = ti.xcom_pull(task_ids='test_task')

    assert ti.state == State.SUCCESS
    assert result['name'] == 'Airflow Secrets TOML Provider'
    assert result['supports_secrets']
    assert result['api_key_from_provider'] == 'test_value'
