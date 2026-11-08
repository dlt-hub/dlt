import os
from pathlib import Path
from typing import Callable, Iterator

import pytest

from dlt.common.configuration.specs import (
    AwsCredentialsWithoutDefaults,
    AzureCredentialsWithoutDefaults,
)
from dlt.common.destination.configuration import CsvFormatConfiguration
from dlt.common.utils import uniq_id

from tests.utils import get_test_storage_root


@pytest.fixture
def snowflake_home() -> Iterator[Callable[..., Path]]:
    """Points the connector at a temp `SNOWFLAKE_HOME` and yields a writer of its toml files."""
    from snowflake.connector.config_manager import CONFIG_MANAGER

    home = Path(get_test_storage_root()) / ("sf_home_" + uniq_id())
    # the connector resolves its files at import, so swap them like `connections_file_path` does
    saved_path, saved_slices = CONFIG_MANAGER.file_path, list(CONFIG_MANAGER._slices)
    idx = next(i for i, s in enumerate(saved_slices) if s.section == "connections")
    CONFIG_MANAGER.file_path = home / "config.toml"
    CONFIG_MANAGER._slices[idx] = saved_slices[idx]._replace(path=home / "connections.toml")
    CONFIG_MANAGER.conf_file_cache = None
    env_keys = ("SNOWFLAKE_HOME", "SNOWFLAKE_CONNECTIONS", "SNOWFLAKE_DEFAULT_CONNECTION_NAME")
    saved_env = {k: os.environ.pop(k) for k in env_keys if k in os.environ}
    os.environ["SNOWFLAKE_HOME"] = str(home)

    def _write(connections: str, config: str = "") -> Path:
        # other fixtures may wipe the test storage after this fixture started
        home.mkdir(parents=True, exist_ok=True)
        for name, content in (("connections.toml", connections), ("config.toml", config)):
            if content:
                path = home / name
                path.write_text(content, encoding="utf-8")
                # connector 4.x refuses files readable by others, 3.x warns
                os.chmod(path, 0o600)
        CONFIG_MANAGER.conf_file_cache = None
        return home

    try:
        yield _write
    finally:
        CONFIG_MANAGER.file_path = saved_path
        CONFIG_MANAGER._slices[:] = saved_slices
        CONFIG_MANAGER.conf_file_cache = None
        os.environ.pop("SNOWFLAKE_HOME", None)
        os.environ.update(saved_env)


@pytest.fixture
def test_table() -> str:
    """Fixture for test table name."""
    return "test_schema.test_table"


@pytest.fixture
def stage_bucket_url() -> str:
    """Fixture for stage bucket URL."""
    return "s3://test-stage-bucket/"


@pytest.fixture
def stage_bucket_url_with_prefix() -> str:
    """Fixture for stage bucket URL."""
    return "s3://test-stage-bucket/with/prefix"


@pytest.fixture
def local_file_path() -> str:
    """Fixture for local file path."""
    return "/tmp/data.csv"


@pytest.fixture
def stage_name() -> str:
    """Fixture for stage name."""
    return "test_stage"


@pytest.fixture
def local_stage_path() -> str:
    """Fixture for local stage file path."""
    return "@%temp_stage/data.csv"


@pytest.fixture
def aws_credentials() -> AwsCredentialsWithoutDefaults:
    """Fixture for AWS credentials."""
    return AwsCredentialsWithoutDefaults(
        aws_access_key_id="test_aws_key", aws_secret_access_key="test_aws_secret"
    )


@pytest.fixture
def azure_credentials() -> AzureCredentialsWithoutDefaults:
    """Fixture for Azure credentials."""
    return AzureCredentialsWithoutDefaults(
        azure_storage_account_name="teststorage",
        azure_storage_sas_token="test_sas_token",
        azure_account_host="teststorage.blob.core.windows.net",
    )


@pytest.fixture
def default_csv_format() -> CsvFormatConfiguration:
    """Fixture for default CSV format."""
    return CsvFormatConfiguration(
        include_header=True, delimiter=",", encoding="UTF-8", on_error_continue=False
    )
