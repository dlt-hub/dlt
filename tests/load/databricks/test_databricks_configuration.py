import pytest
import os

pytest.importorskip("databricks")

from dlt.common.exceptions import TerminalValueError
from dlt.destinations.impl.databricks.databricks import DatabricksLoadJob
from dlt.common.configuration import resolve_configuration

from dlt.destinations import databricks
from dlt.destinations.impl.databricks.configuration import DatabricksClientConfiguration

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


def test_databricks_credentials_to_connector_params():
    os.environ["CREDENTIALS__SERVER_HOSTNAME"] = "my-databricks.example.com"
    os.environ["CREDENTIALS__HTTP_PATH"] = "/sql/1.0/warehouses/asdfe"
    os.environ["CREDENTIALS__ACCESS_TOKEN"] = "my-token"
    os.environ["CREDENTIALS__CATALOG"] = "my-catalog"
    # JSON encoded dict of extra args
    os.environ["CREDENTIALS__CONNECTION_PARAMETERS"] = '{"extra_a": "a", "extra_b": "b"}'

    config = resolve_configuration(
        DatabricksClientConfiguration()._bind_dataset_name(dataset_name="my-dataset")
    )

    credentials = config.credentials

    params = credentials.to_connector_params()

    assert params["server_hostname"] == "my-databricks.example.com"
    assert params["http_path"] == "/sql/1.0/warehouses/asdfe"
    assert params["access_token"] == "my-token"
    assert params["catalog"] == "my-catalog"
    assert params["extra_a"] == "a"
    assert params["extra_b"] == "b"
    assert params["_socket_timeout"] == credentials.socket_timeout


def test_databricks_configuration() -> None:
    bricks = databricks()
    config = bricks.configuration(None, accept_partial=True)
    assert config.is_staging_external_location is False
    assert config.staging_credentials_name is None

    os.environ["IS_STAGING_EXTERNAL_LOCATION"] = "true"
    os.environ["STAGING_CREDENTIALS_NAME"] = "credential"
    config = bricks.configuration(None, accept_partial=True)
    assert config.is_staging_external_location is True
    assert config.staging_credentials_name == "credential"

    # explicit params
    bricks = databricks(is_staging_external_location=None, staging_credentials_name="credential2")
    config = bricks.configuration(None, accept_partial=True)
    assert config.staging_credentials_name == "credential2"
    assert config.is_staging_external_location is None


def test_databricks_abfss_converter() -> None:
    with pytest.raises(TerminalValueError):
        DatabricksLoadJob.ensure_databricks_abfss_url("az://dlt-ci-test-bucket")

    abfss_url = DatabricksLoadJob.ensure_databricks_abfss_url(
        "az://dlt-ci-test-bucket", "my_account"
    )
    assert abfss_url == "abfss://dlt-ci-test-bucket@my_account.dfs.core.windows.net"

    abfss_url = DatabricksLoadJob.ensure_databricks_abfss_url(
        "az://dlt-ci-test-bucket/path/to/file.parquet", "my_account"
    )
    assert (
        abfss_url
        == "abfss://dlt-ci-test-bucket@my_account.dfs.core.windows.net/path/to/file.parquet"
    )

    abfss_url = DatabricksLoadJob.ensure_databricks_abfss_url(
        "az://dlt-ci-test-bucket@my_account.dfs.core.windows.net/path/to/file.parquet"
    )
    assert (
        abfss_url
        == "abfss://dlt-ci-test-bucket@my_account.dfs.core.windows.net/path/to/file.parquet"
    )
