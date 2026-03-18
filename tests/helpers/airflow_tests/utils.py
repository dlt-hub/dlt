from configparser import DuplicateSectionError
import os
from typing import Any, TYPE_CHECKING, Optional
from packaging.version import Version
from importlib.metadata import version as pkg_version

import pytest

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import PluggableRunContext
from dlt.common.configuration.providers.vault import SECRETS_TOML_KEY

if TYPE_CHECKING:
    from airflow import DAG
    from airflow.models import TaskInstance

AIRFLOW_VERSION: "Optional[Version]" = None


def _get_airflow_version() -> "Version":
    global AIRFLOW_VERSION
    if AIRFLOW_VERSION is None:
        AIRFLOW_VERSION = Version(pkg_version("apache-airflow"))
    return AIRFLOW_VERSION


# Test data
SECRETS_TOML_CONTENT = """
[sources]
api_key = "test_value"
"""


@pytest.fixture(scope="function", autouse=True)
def initialize_airflow_db():
    from airflow.models.variable import Variable

    setup_airflow()
    # backup context providers
    providers = Container()[PluggableRunContext].providers
    # allow airflow provider
    os.environ["PROVIDERS__ENABLE_AIRFLOW_SECRETS"] = "true"
    Variable.set(SECRETS_TOML_KEY, SECRETS_TOML_CONTENT)
    # re-create providers
    Container()[PluggableRunContext].reload_providers()
    try:
        yield
    finally:
        # restore providers
        Container()[PluggableRunContext].providers = providers
        # Make sure the variable is not set
        Variable.delete(SECRETS_TOML_KEY)


def setup_airflow() -> None:
    """Initialize Airflow DB in test storage so it is wiped with test cleanup."""
    _get_airflow_version()
    from airflow.configuration import conf

    # disable loading examples
    try:
        conf.add_section("core")
    except DuplicateSectionError:
        pass
    conf.set("core", "load_examples", "False")

    # point airflow DB at test storage so it gets cleaned up between tests
    from tests.utils import get_test_storage_root

    db_path = os.path.abspath(os.path.join(get_test_storage_root(), "airflow.db"))
    db_uri = f"sqlite:///{db_path}"

    if _get_airflow_version().major >= 3:
        conf.set("database", "sql_alchemy_conn", db_uri)
    else:
        conf.set("core", "sql_alchemy_conn", db_uri)

    # airflow caches sql_alchemy_conn in settings at import time — reconfigure
    from airflow import settings

    settings.SQL_ALCHEMY_CONN = db_uri
    settings.configure_orm()

    # initialize the DB
    if _get_airflow_version().major >= 3:
        from airflow.utils.db import initdb

        initdb()
    else:
        from airflow.utils.db import resetdb

        resetdb()


def exec_dag_test(dag_def: "DAG", execution_date: "Any" = None) -> None:
    """Call dag.test() with the correct date parameter for AF2/AF3."""
    if _get_airflow_version().major >= 3:
        dag_def.test(logical_date=execution_date)  # type: ignore[call-arg,unused-ignore]
    else:
        dag_def.test(execution_date=execution_date)


def run_task(
    dag_def: "DAG",
    task_id: str,
    execution_date: "Any" = None,
    data_interval: "Any" = None,
) -> "Any":
    """Run a single task in the DAG and return the TaskInstance.

    Works on both Airflow 2 and 3. On AF2 uses create_dagrun + TaskInstance.run().
    On AF3 uses dag.test() and retrieves the TaskInstance from the DB.
    """
    if _get_airflow_version().major >= 3:
        exec_dag_test(dag_def, execution_date=execution_date)
        from airflow.models import DagRun
        from airflow.utils.session import create_session

        with create_session() as session:
            dr = (
                session.query(DagRun)
                .filter(DagRun.dag_id == dag_def.dag_id)
                .order_by(DagRun.logical_date.desc())  # type: ignore[attr-defined,unused-ignore]
                .first()
            )
            ti = dr.get_task_instance(task_id, session=session)
            session.expunge_all()
        return ti
    else:
        from airflow.models import TaskInstance
        from airflow.utils.state import DagRunState
        from airflow.utils.types import DagRunType

        dag_def.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=execution_date,
            start_date=execution_date,
            run_type=DagRunType.MANUAL,
            **({"data_interval": data_interval} if data_interval is not None else {}),
        )
        task_def = dag_def.task_dict[task_id]
        ti = TaskInstance(task=task_def, execution_date=execution_date)
        ti.run()
        return ti
