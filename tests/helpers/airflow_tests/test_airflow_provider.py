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

import dlt
from dlt.common import pendulum
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext

from dlt.common.configuration.providers.airflow import AIRFLOW_SECRETS_TOML_VARIABLE_KEY

DEFAULT_DATE = pendulum.datetime(2023, 4, 18, tz='Europe/Berlin')


@pytest.fixture(scope='function', autouse=True)
def initialize_airflow_db():
    # Disable loading examples
    conf.set('core', 'load_examples', 'False')

    # Prepare the arguments for the initdb function
    args = argparse.Namespace()
    args.backend = conf.get(section='core', key='sql_alchemy_conn')

    # Run Airflow resetdb before running any tests
    args.yes = True
    args.skip_init = False
    resetdb(args)
    yield
    # Make sure the variable is not set
    Variable.delete(AIRFLOW_SECRETS_TOML_VARIABLE_KEY)


# Test data
SECRETS_TOML_CONTENT = """
[sources]
api_key = "test_value"
"""


def test_airflow_secrets_toml_provider():
    dag = DAG(dag_id='test_dag', start_date=DEFAULT_DATE)

    def test_task():
        from dlt.common.configuration.providers.airflow import (
            AirflowSecretsTomlProvider,
            AIRFLOW_SECRETS_TOML_VARIABLE_KEY,
        )

        Variable.set(AIRFLOW_SECRETS_TOML_VARIABLE_KEY, SECRETS_TOML_CONTENT)

        provider = AirflowSecretsTomlProvider()

        api_key, _ = provider.get_value('api_key', str, None, 'sources')

        # There's no pytest context here in the task, so we need to return
        # the results as a dict and assert them in the test function.
        # See ti.xcom_pull() below.
        return {
            'name': provider.name,
            'supports_secrets': provider.supports_secrets,
            'api_key_from_provider': api_key,
        }

    task = PythonOperator(
        task_id='test_task', python_callable=test_task, dag=dag
    )

    dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=DEFAULT_DATE,
        start_date=DEFAULT_DATE,
        run_type=DagRunType.MANUAL,
    )

    ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

    ti.run()

    result = ti.xcom_pull(task_ids='test_task')

    assert ti.state == State.SUCCESS
    assert result['name'] == 'Airflow Secrets TOML Provider'
    assert result['supports_secrets']
    assert result['api_key_from_provider'] == 'test_value'


def test_airflow_secrets_toml_provider_is_loaded():
    dag = DAG(dag_id='test_dag', start_date=DEFAULT_DATE)

    def test_task():
        from dlt.common.configuration.specs import config_providers_context
        from dlt.common.configuration.providers.airflow import (
            AirflowSecretsTomlProvider,
            AIRFLOW_SECRETS_TOML_VARIABLE_KEY,
        )

        Variable.set(AIRFLOW_SECRETS_TOML_VARIABLE_KEY, SECRETS_TOML_CONTENT)

        providers_context = Container()[ConfigProvidersContext]

        astp_is_loaded = any(
            isinstance(provider, AirflowSecretsTomlProvider)
            for provider in providers_context.providers
        )

        # insert provider into context, in tests this will not happen automatically
        # providers_context = Container()[ConfigProvidersContext]
        # providers_context.add_provider(providers[0])

        # get secret value using accessor
        api_key = dlt.secrets["sources.api_key"]

        # remove provider for clean context
        # providers_context.providers.remove(providers[0])

        # There's no pytest context here in the task, so we need to return
        # the results as a dict and assert them in the test function.
        # See ti.xcom_pull() below.
        return {
            'airflow_secrets_toml_provider_is_loaded': astp_is_loaded,
            'api_key_from_provider': api_key,
        }

    task = PythonOperator(
        task_id='test_task', python_callable=test_task, dag=dag
    )

    dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=DEFAULT_DATE,
        start_date=DEFAULT_DATE,
        run_type=DagRunType.MANUAL,
    )

    ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

    ti.run()

    result = ti.xcom_pull(task_ids='test_task')

    assert ti.state == State.SUCCESS
    assert result['airflow_secrets_toml_provider_is_loaded']
    assert result['api_key_from_provider'] == 'test_value'


def test_airflow_secrets_toml_provider_missing_variable():
    dag = DAG(dag_id='test_dag', start_date=DEFAULT_DATE)

    def test_task():
        from dlt.common.configuration.specs import config_providers_context
        from dlt.common.configuration.providers.airflow import (
            AirflowSecretsTomlProvider,
            AIRFLOW_SECRETS_TOML_VARIABLE_KEY,
        )

        # Make sure the variable is not set
        Variable.delete(AIRFLOW_SECRETS_TOML_VARIABLE_KEY)

        providers = config_providers_context._extra_providers()

        astp_is_loaded = any(
            isinstance(provider, AirflowSecretsTomlProvider)
            for provider in providers
        )

        # There's no pytest context here in the task, so we need to return
        # the results as a dict and assert them in the test function.
        # See ti.xcom_pull() below.
        return {
            'airflow_secrets_toml_provider_is_loaded': astp_is_loaded,
        }

    task = PythonOperator(
        task_id='test_task', python_callable=test_task, dag=dag
    )

    dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=DEFAULT_DATE,
        start_date=DEFAULT_DATE,
        run_type=DagRunType.MANUAL,
    )

    ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

    ti.run()

    result = ti.xcom_pull(task_ids='test_task')

    assert ti.state == State.SUCCESS
    assert not result['airflow_secrets_toml_provider_is_loaded']


def test_airflow_secrets_toml_provider_invalid_content():
    dag = DAG(dag_id='test_dag', start_date=DEFAULT_DATE)

    def test_task():
        import tomlkit
        from dlt.common.configuration.providers.airflow import (
            AirflowSecretsTomlProvider,
            AIRFLOW_SECRETS_TOML_VARIABLE_KEY,
        )

        Variable.set(AIRFLOW_SECRETS_TOML_VARIABLE_KEY, 'invalid_content')

        # There's no pytest context here in the task, so we need
        # to catch the exception manually and return the result
        # as a dict and do the assertion in the test function.
        exception_raised = False
        try:
            AirflowSecretsTomlProvider()
        except tomlkit.exceptions.ParseError:
            exception_raised = True

        return {
            'exception_raised': exception_raised,
        }

    task = PythonOperator(
        task_id='test_task', python_callable=test_task, dag=dag
    )

    dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=DEFAULT_DATE,
        start_date=DEFAULT_DATE,
        run_type=DagRunType.MANUAL,
    )

    ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

    ti.run()

    result = ti.xcom_pull(task_ids='test_task')

    assert ti.state == State.SUCCESS
    assert result['exception_raised']
