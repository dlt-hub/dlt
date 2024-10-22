import os
import dataclasses
import logging
import sys
import pytest
from typing import List, Iterator
from importlib.metadata import version as pkg_version
from packaging.version import Version

# patch which providers to enable
from dlt.common.configuration.providers import (
    ConfigProvider,
    EnvironProvider,
    SecretsTomlProvider,
    ConfigTomlProvider,
)
from dlt.common.configuration.specs.config_providers_context import (
    ConfigProvidersConfiguration,
)
from dlt.common.runtime.run_context import RunContext


def initial_providers(self) -> List[ConfigProvider]:
    # do not read the global config
    return [
        EnvironProvider(),
        SecretsTomlProvider(settings_dir="tests/.dlt"),
        ConfigTomlProvider(settings_dir="tests/.dlt"),
    ]


RunContext.initial_providers = initial_providers  # type: ignore[method-assign]
# also disable extras
ConfigProvidersConfiguration.enable_airflow_secrets = False
ConfigProvidersConfiguration.enable_google_secrets = False


def pytest_configure(config):
    # patch the configurations to use test storage by default, we modify the types (classes) fields
    # the dataclass implementation will use those patched values when creating instances (the values present
    # in the declaration are not frozen allowing patching)

    from dlt.common.configuration.specs import runtime_configuration
    from dlt.common.storages import configuration as storage_configuration

    test_storage_root = "_storage"
    runtime_configuration.RuntimeConfiguration.config_files_storage_path = os.path.join(
        test_storage_root, "config/"
    )
    # always use CI track endpoint when running tests
    runtime_configuration.RuntimeConfiguration.dlthub_telemetry_endpoint = (
        "https://telemetry-tracker.services4758.workers.dev"
    )
    delattr(runtime_configuration.RuntimeConfiguration, "__init__")
    runtime_configuration.RuntimeConfiguration = dataclasses.dataclass(  # type: ignore[misc]
        runtime_configuration.RuntimeConfiguration, init=True, repr=False
    )  # type: ignore
    # push telemetry to CI

    storage_configuration.LoadStorageConfiguration.load_volume_path = os.path.join(
        test_storage_root, "load"
    )
    delattr(storage_configuration.LoadStorageConfiguration, "__init__")
    storage_configuration.LoadStorageConfiguration = dataclasses.dataclass(  # type: ignore[misc,call-overload]
        storage_configuration.LoadStorageConfiguration, init=True, repr=False
    )

    storage_configuration.NormalizeStorageConfiguration.normalize_volume_path = os.path.join(
        test_storage_root, "normalize"
    )
    # delete __init__, otherwise it will not be recreated by dataclass
    delattr(storage_configuration.NormalizeStorageConfiguration, "__init__")
    storage_configuration.NormalizeStorageConfiguration = dataclasses.dataclass(  # type: ignore[misc,call-overload]
        storage_configuration.NormalizeStorageConfiguration, init=True, repr=False
    )

    storage_configuration.SchemaStorageConfiguration.schema_volume_path = os.path.join(
        test_storage_root, "schemas"
    )
    delattr(storage_configuration.SchemaStorageConfiguration, "__init__")
    storage_configuration.SchemaStorageConfiguration = dataclasses.dataclass(  # type: ignore[misc,call-overload]
        storage_configuration.SchemaStorageConfiguration, init=True, repr=False
    )

    assert runtime_configuration.RuntimeConfiguration.config_files_storage_path == os.path.join(
        test_storage_root, "config/"
    )
    assert runtime_configuration.RuntimeConfiguration().config_files_storage_path == os.path.join(
        test_storage_root, "config/"
    )

    # path pipeline instance id up to millisecond
    from dlt.common import pendulum
    from dlt.pipeline.pipeline import Pipeline

    def _create_pipeline_instance_id(self) -> str:
        return pendulum.now().format("_YYYYMMDDhhmmssSSSS")

    Pipeline._create_pipeline_instance_id = _create_pipeline_instance_id  # type: ignore[method-assign]

    # disable sqlfluff logging
    for log in ["sqlfluff.parser", "sqlfluff.linter", "sqlfluff.templater", "sqlfluff.lexer"]:
        logging.getLogger(log).setLevel("ERROR")

    # disable snowflake logging
    for log in ["snowflake.connector.cursor", "snowflake.connector.connection"]:
        logging.getLogger(log).setLevel("ERROR")

    # disable azure logging
    for log in ["azure.core.pipeline.policies.http_logging_policy"]:
        logging.getLogger(log).setLevel("ERROR")

    # disable databricks logging
    for log in ["databricks.sql.client"]:
        logging.getLogger(log).setLevel("WARNING")

    # disable httpx request logging (too verbose when testing qdrant)
    logging.getLogger("httpx").setLevel("WARNING")

    # reset and init airflow db
    import warnings

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)

        try:
            from airflow.utils import db
            import contextlib
            import io

            for log in [
                "airflow.models.crypto",
                "airflow.models.variable",
                "airflow",
                "alembic",
                "alembic.runtime.migration",
            ]:
                logging.getLogger(log).setLevel("ERROR")

            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                io.StringIO()
            ):
                db.resetdb()

        except Exception:
            pass


@pytest.fixture(autouse=True)
def pyarrow17_check(request) -> Iterator[None]:
    if "needspyarrow17" in request.keywords:
        if "pyarrow" not in sys.modules or Version(pkg_version("pyarrow")) < Version("17.0.0"):
            pytest.skip("test needs `pyarrow>=17.0.0`")
    yield
