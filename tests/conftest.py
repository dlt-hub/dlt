import os
import dataclasses
import logging
from pathlib import Path
from typing import List

# patch which providers to enable
from dlt.common.configuration.providers import (
    ConfigProvider,
    EnvironProvider,
    SecretsTomlProvider,
    ConfigTomlProvider,
)
from dlt.common.configuration.specs.config_providers_context import (
    ConfigProvidersContext,
    ConfigProvidersConfiguration,
)


TESTS_ROOT = Path(__file__).parent.absolute()


def initial_providers() -> List[ConfigProvider]:
    # do not read the global config
    return [
        EnvironProvider(),
        SecretsTomlProvider(project_dir=str(TESTS_ROOT / ".dlt"), add_global_config=False),
        ConfigTomlProvider(project_dir=str(TESTS_ROOT / ".dlt"), add_global_config=False),
    ]


ConfigProvidersContext.initial_providers = initial_providers  # type: ignore[method-assign]
# also disable extras
ConfigProvidersConfiguration.enable_airflow_secrets = False
ConfigProvidersConfiguration.enable_google_secrets = False


def pytest_configure(config):
    # patch the configurations to use test storage by default, we modify the types (classes) fields
    # the dataclass implementation will use those patched values when creating instances (the values present
    # in the declaration are not frozen allowing patching)

    from dlt.common.configuration.specs import run_configuration
    from dlt.common.storages import configuration as storage_configuration

    test_storage_root = "_storage"
    run_configuration.RunConfiguration.config_files_storage_path = os.path.join(
        test_storage_root, "config/"
    )
    run_configuration.RunConfiguration.dlthub_telemetry_segment_write_key = (
        "TLJiyRkGVZGCi2TtjClamXpFcxAA1rSB"
    )
    delattr(run_configuration.RunConfiguration, "__init__")
    run_configuration.RunConfiguration = dataclasses.dataclass(
        run_configuration.RunConfiguration, init=True, repr=False
    )  # type: ignore
    # push telemetry to CI

    storage_configuration.LoadStorageConfiguration.load_volume_path = os.path.join(
        test_storage_root, "load"
    )
    delattr(storage_configuration.LoadStorageConfiguration, "__init__")
    storage_configuration.LoadStorageConfiguration = dataclasses.dataclass(
        storage_configuration.LoadStorageConfiguration, init=True, repr=False
    )  # type: ignore[misc, call-overload]

    storage_configuration.NormalizeStorageConfiguration.normalize_volume_path = os.path.join(
        test_storage_root, "normalize"
    )
    # delete __init__, otherwise it will not be recreated by dataclass
    delattr(storage_configuration.NormalizeStorageConfiguration, "__init__")
    storage_configuration.NormalizeStorageConfiguration = dataclasses.dataclass(
        storage_configuration.NormalizeStorageConfiguration, init=True, repr=False
    )  # type: ignore[misc, call-overload]

    storage_configuration.SchemaStorageConfiguration.schema_volume_path = os.path.join(
        test_storage_root, "schemas"
    )
    delattr(storage_configuration.SchemaStorageConfiguration, "__init__")
    storage_configuration.SchemaStorageConfiguration = dataclasses.dataclass(
        storage_configuration.SchemaStorageConfiguration, init=True, repr=False
    )  # type: ignore[misc, call-overload]

    assert run_configuration.RunConfiguration.config_files_storage_path == os.path.join(
        test_storage_root, "config/"
    )
    assert run_configuration.RunConfiguration().config_files_storage_path == os.path.join(
        test_storage_root, "config/"
    )

    # path pipeline instance id up to millisecond
    from dlt.common import pendulum
    from dlt.pipeline.pipeline import Pipeline

    def _create_pipeline_instance_id(self) -> str:
        return pendulum.now().format("_YYYYMMDDhhmmssSSSS")

    Pipeline._create_pipeline_instance_id = _create_pipeline_instance_id  # type: ignore[method-assign]
    # push sentry to ci
    # os.environ["RUNTIME__SENTRY_DSN"] = "https://6f6f7b6f8e0f458a89be4187603b55fe@o1061158.ingest.sentry.io/4504819859914752"

    # disable sqlfluff logging
    for log in ["sqlfluff.parser", "sqlfluff.linter", "sqlfluff.templater", "sqlfluff.lexer"]:
        logging.getLogger(log).setLevel("ERROR")

    # disable snowflake logging
    for log in ["snowflake.connector.cursor", "snowflake.connector.connection"]:
        logging.getLogger(log).setLevel("ERROR")

    # disable azure logging
    for log in ["azure.core.pipeline.policies.http_logging_policy"]:
        logging.getLogger(log).setLevel("ERROR")
