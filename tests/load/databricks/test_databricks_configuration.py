import pytest
import os

from dlt.common.schema.schema import Schema
from dlt.common.utils import digest128

pytest.importorskip("databricks")

import dlt
from dlt.common.exceptions import TerminalValueError
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.destinations.impl.databricks.databricks import DatabricksLoadJob
from dlt.common.configuration import resolve_configuration

from dlt.destinations import databricks
from dlt.destinations.impl.databricks.configuration import (
    DatabricksClientConfiguration,
    DATABRICKS_APPLICATION_ID,
    DatabricksCredentials,
)

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
    assert params["_user_agent_entry"] == DATABRICKS_APPLICATION_ID

    displayable_location = str(credentials)
    assert displayable_location.startswith(
        "databricks://my-databricks.example.com/sql/1.0/warehouses/asdfe/my-catalog"
    )


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


def test_databricks_auth_invalid() -> None:
    with pytest.raises(ConfigurationValueError, match="Authentication failed:*"):
        os.environ["DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_ID"] = ""
        os.environ["DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_SECRET"] = ""
        os.environ["DESTINATION__DATABRICKS__CREDENTIALS__ACCESS_TOKEN"] = ""
        bricks = databricks()
        bricks.configuration(None, accept_partial=True)


def test_databricks_missing_config_catalog() -> None:
    with pytest.raises(
        ConfigurationValueError, match="Configuration error: Missing required parameter `catalog`"
    ):
        os.environ["DESTINATION__DATABRICKS__CREDENTIALS__CATALOG"] = ""
        bricks = databricks()
        bricks.configuration(None, accept_partial=True)


def test_databricks_missing_config_http_path() -> None:
    with pytest.raises(
        ConfigurationValueError,
        match="Configuration error: Missing required parameter `http_path`",
    ):
        os.environ["DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH"] = ""
        bricks = databricks()
        bricks.configuration(None, accept_partial=True)


def test_databricks_missing_config_server_hostname() -> None:
    with pytest.raises(
        ConfigurationValueError,
        match="Configuration error: Missing required parameter `server_hostname`",
    ):
        os.environ["DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME"] = ""
        bricks = databricks()
        bricks.configuration(None, accept_partial=True)


@pytest.mark.parametrize("auth_type", ("pat", "oauth2"))
def test_default_credentials(auth_type: str) -> None:
    # create minimal default env
    os.environ["DATABRICKS_HOST"] = dlt.secrets[
        "destination.databricks.credentials.server_hostname"
    ]
    if auth_type == "pat":
        os.environ["DATABRICKS_TOKEN"] = dlt.secrets[
            "destination.databricks.credentials.access_token"
        ]
    else:
        os.environ["DATABRICKS_CLIENT_ID"] = dlt.secrets[
            "destination.databricks.credentials.client_id"
        ]
        os.environ["DATABRICKS_CLIENT_SECRET"] = dlt.secrets[
            "destination.databricks.credentials.client_secret"
        ]

    # will not pick up the credentials from "destination.databricks"
    config = resolve_configuration(
        DatabricksClientConfiguration(
            credentials=DatabricksCredentials(catalog="dlt_ci")
        )._bind_dataset_name(dataset_name="my-dataset-1234")
    )
    # we pass authenticator that will be used to make connection, that's why callable
    assert callable(config.credentials.access_token)
    # taken from a warehouse
    assert isinstance(config.credentials.http_path, str)

    bricks = databricks(credentials=config.credentials)
    # "my-dataset-1234" not present (we check SQL execution)
    with bricks.client(Schema("schema"), config) as client:
        assert not client.is_storage_initialized()

    # check fingerprint not default
    assert config.fingerprint() != digest128("")


def test_oauth2_credentials() -> None:
    dlt.secrets["destination.databricks.credentials.access_token"] = ""
    # we must prime the "destinations" for google secret manager config provider
    # because it retrieves catalog as first element and it is not secret. and vault providers
    # are secret only
    dlt.secrets.get("destination.credentials")
    config = resolve_configuration(
        DatabricksClientConfiguration()._bind_dataset_name(dataset_name="my-dataset-1234-oauth"),
        sections=("destination", "databricks"),
    )
    assert config.credentials.access_token == ""
    # will resolve to oauth token
    bricks = databricks(credentials=config.credentials)
    # "my-dataset-1234-oauth" not present (we check SQL execution)
    with bricks.client(Schema("schema"), config) as client:
        assert not client.is_storage_initialized()


def test_default_warehouse() -> None:
    os.environ["DATABRICKS_TOKEN"] = dlt.secrets["destination.databricks.credentials.access_token"]
    os.environ["DATABRICKS_HOST"] = dlt.secrets[
        "destination.databricks.credentials.server_hostname"
    ]
    # will force this warehouse
    os.environ["DATABRICKS_WAREHOUSE_ID"] = "588dbd71bd802f4d"

    config = resolve_configuration(
        DatabricksClientConfiguration(
            credentials=DatabricksCredentials(catalog="dlt_ci")
        )._bind_dataset_name(dataset_name="my-dataset-1234")
    )
    assert config.credentials.http_path == "/sql/1.0/warehouses/588dbd71bd802f4d"
