import pytest

from dlt.common.typing import AnyType

pytest.importorskip("airflow")

from airflow import DAG
from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable

import dlt
from dlt.common import pendulum
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import PluggableRunContext
from dlt.common.configuration.providers.vault import SECRETS_TOML_KEY

from tests.helpers.airflow_tests.utils import run_task

DEFAULT_DATE = pendulum.datetime(2023, 4, 18, tz="Europe/Berlin")


def test_airflow_secrets_toml_provider() -> None:
    @dag(start_date=DEFAULT_DATE)
    def test_dag():
        from dlt.common.configuration.providers.airflow import AirflowSecretsTomlProvider

        # make sure provider works while creating DAG
        provider = AirflowSecretsTomlProvider()
        assert provider.get_value("api_key", str, None, "sources")[0] == "test_value"

        @task()
        def test_task():
            provider = AirflowSecretsTomlProvider()

            api_key, _ = provider.get_value("api_key", str, None, "sources")

            # There's no pytest context here in the task, so we need to return
            # the results as a dict and assert them in the test function.
            # See ti.xcom_pull() below.
            return {
                "name": provider.name,
                "supports_secrets": provider.supports_secrets,
                "api_key_from_provider": api_key,
            }

        test_task()

    dag_def: DAG = test_dag()
    ti = run_task(dag_def, "test_task", execution_date=DEFAULT_DATE)

    result = ti.xcom_pull(task_ids="test_task")

    assert result["name"] == "Airflow Secrets TOML Provider"
    assert result["supports_secrets"]
    assert result["api_key_from_provider"] == "test_value"


def test_airflow_secrets_toml_provider_import_dlt_dag() -> None:
    """Tests if the provider is functional when defining DAG"""

    @dag(start_date=DEFAULT_DATE)
    def test_dag():
        from dlt.common.configuration.accessors import secrets

        # this will initialize provider context
        api_key = secrets["sources.api_key"]
        assert api_key == "test_value"

        @task()
        def test_task():
            return {
                "api_key_from_provider": api_key,
            }

        test_task()

    dag_def: DAG = test_dag()
    ti = run_task(dag_def, "test_task", execution_date=DEFAULT_DATE)

    result = ti.xcom_pull(task_ids="test_task")

    assert result["api_key_from_provider"] == "test_value"


def test_airflow_secrets_toml_provider_import_dlt_task() -> None:
    """Tests if the provider is functional when running in task"""

    @dag(start_date=DEFAULT_DATE)
    def test_dag():
        @task()
        def test_task():
            from dlt.common.configuration.accessors import secrets

            # this will initialize provider context
            api_key = secrets["sources.api_key"]

            return {
                "api_key_from_provider": api_key,
            }

        test_task()

    dag_def: DAG = test_dag()
    ti = run_task(dag_def, "test_task", execution_date=DEFAULT_DATE)

    result = ti.xcom_pull(task_ids="test_task")

    assert result["api_key_from_provider"] == "test_value"


def test_airflow_secrets_toml_provider_is_loaded():
    dag_inst = DAG(dag_id="test_dag", start_date=DEFAULT_DATE)

    def test_task():
        from dlt.common.configuration.providers.airflow import AirflowSecretsTomlProvider

        providers_context = Container()[PluggableRunContext].providers

        astp_is_loaded = any(
            isinstance(provider, AirflowSecretsTomlProvider)
            for provider in providers_context.providers
        )

        # get secret value using accessor
        api_key = dlt.secrets["sources.api_key"]

        # There's no pytest context here in the task, so we need to return
        # the results as a dict and assert them in the test function.
        # See ti.xcom_pull() below.
        return {
            "airflow_secrets_toml_provider_is_loaded": astp_is_loaded,
            "api_key_from_provider": api_key,
        }

    PythonOperator(task_id="test_task", python_callable=test_task, dag=dag_inst)
    ti = run_task(dag_inst, "test_task", execution_date=DEFAULT_DATE)

    result = ti.xcom_pull(task_ids="test_task")

    assert result["airflow_secrets_toml_provider_is_loaded"]
    assert result["api_key_from_provider"] == "test_value"


def test_airflow_secrets_toml_provider_missing_variable():
    dag_inst = DAG(dag_id="test_dag", start_date=DEFAULT_DATE)

    def test_task():
        from dlt.common.configuration.specs import config_providers_context
        from dlt.common.configuration.providers.airflow import AirflowSecretsTomlProvider

        # Make sure the variable is not set
        Variable.delete(SECRETS_TOML_KEY)
        providers = config_providers_context._extra_providers()
        provider = next(
            provider for provider in providers if isinstance(provider, AirflowSecretsTomlProvider)
        )
        return {
            "airflow_secrets_toml": provider.to_toml(),
        }

    PythonOperator(task_id="test_task", python_callable=test_task, dag=dag_inst)
    ti = run_task(dag_inst, "test_task", execution_date=DEFAULT_DATE)

    result = ti.xcom_pull(task_ids="test_task")

    assert result["airflow_secrets_toml"] == ""


def test_airflow_secrets_toml_provider_invalid_content():
    dag_inst = DAG(dag_id="test_dag", start_date=DEFAULT_DATE)

    def test_task():
        from dlt.common.configuration.providers.airflow import AirflowSecretsTomlProvider

        Variable.set(SECRETS_TOML_KEY, "invalid_content")

        # There's no pytest context here in the task, so we need
        # to catch the exception manually and return the result
        # as a dict and do the assertion in the test function.
        exception_raised = False
        try:
            AirflowSecretsTomlProvider().get_value("content", AnyType, None)
        except ValueError:
            exception_raised = True

        return {
            "exception_raised": exception_raised,
        }

    PythonOperator(task_id="test_task", python_callable=test_task, dag=dag_inst)
    ti = run_task(dag_inst, "test_task", execution_date=DEFAULT_DATE)

    result = ti.xcom_pull(task_ids="test_task")

    assert result["exception_raised"]


def test_airflow_pipeline_scoped_secrets_toml():
    dag_inst = DAG(dag_id="test_dag", start_date=DEFAULT_DATE)

    def test_task():
        import dlt

        fragment = """
[pipelines.my_pipeline]
dataset_name="test_dataset_scoped"
"""

        Variable.set("my_pipeline." + SECRETS_TOML_KEY, fragment)

        # this should access pipeline-scoped secrets.toml
        pipeline = dlt.pipeline("my_pipeline")
        # dataset name was configured
        return {
            "dataset_name": pipeline.dataset_name,
        }

    PythonOperator(task_id="test_task", python_callable=test_task, dag=dag_inst)
    ti = run_task(dag_inst, "test_task", execution_date=DEFAULT_DATE)

    result = ti.xcom_pull(task_ids="test_task")

    assert result["dataset_name"] == "test_dataset_scoped"
