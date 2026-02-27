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


def test_databricks_auth_invalid(mocker) -> None:
    # Clear all relevant auth environment variables to ensure clean test state
    for key in list(os.environ.keys()):
        if "DATABRICKS" in key or "CREDENTIALS" in key:
            del os.environ[key]

    # Mock WorkspaceClient to prevent it from providing default auth
    mock_workspace_client = mocker.MagicMock()
    mock_workspace_client.dbutils.notebook.entry_point.getDbutils.side_effect = Exception("No context")
    mock_workspace_client.config.authenticate = None
    mocker.patch("databricks.sdk.WorkspaceClient", return_value=mock_workspace_client)

    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_ID"] = ""
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_SECRET"] = ""
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__ACCESS_TOKEN"] = ""
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__CATALOG"] = "test_catalog"
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME"] = "test.databricks.com"
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH"] = "/sql/1.0/test"

    with pytest.raises(ConfigurationValueError, match="Authentication failed:.*"):
        bricks = databricks()
        bricks.configuration(None, accept_partial=True)


def test_databricks_missing_config_catalog(mocker) -> None:
    # Clear all relevant environment variables to ensure clean test state
    for key in list(os.environ.keys()):
        if "DATABRICKS" in key or "CREDENTIALS" in key:
            del os.environ[key]

    # Mock WorkspaceClient to prevent it from providing default connection params
    mock_workspace_client = mocker.MagicMock()
    mock_workspace_client.dbutils.notebook.entry_point.getDbutils.side_effect = Exception("No context")
    mock_workspace_client.warehouses.list.side_effect = Exception("No warehouses")
    mocker.patch("databricks.sdk.WorkspaceClient", return_value=mock_workspace_client)

    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__ACCESS_TOKEN"] = "test_token"
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME"] = "test.databricks.com"
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH"] = "/sql/1.0/test"
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__CATALOG"] = ""

    with pytest.raises(
        ConfigurationValueError, match="Configuration error: Missing required parameter `catalog`"
    ):
        bricks = databricks()
        bricks.configuration(None, accept_partial=True)


def test_databricks_missing_config_http_path(mocker) -> None:
    # Clear all relevant environment variables to ensure clean test state
    for key in list(os.environ.keys()):
        if "DATABRICKS" in key or "CREDENTIALS" in key:
            del os.environ[key]

    # Mock WorkspaceClient to prevent it from providing default connection params
    mock_workspace_client = mocker.MagicMock()
    mock_workspace_client.dbutils.notebook.entry_point.getDbutils.side_effect = Exception("No context")
    mock_workspace_client.config.warehouse_id = None
    mock_workspace_client.warehouses.list.return_value = iter([])  # Empty iterator
    mocker.patch("databricks.sdk.WorkspaceClient", return_value=mock_workspace_client)

    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__ACCESS_TOKEN"] = "test_token"
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME"] = "test.databricks.com"
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__CATALOG"] = "test_catalog"
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH"] = ""

    with pytest.raises(
        ConfigurationValueError,
        match="Configuration error: Missing required parameter `http_path`",
    ):
        bricks = databricks()
        bricks.configuration(None, accept_partial=True)


def test_databricks_missing_config_server_hostname(mocker) -> None:
    # Clear all relevant environment variables to ensure clean test state
    for key in list(os.environ.keys()):
        if "DATABRICKS" in key or "CREDENTIALS" in key:
            del os.environ[key]

    # Mock WorkspaceClient to prevent it from providing default connection params
    mock_workspace_client = mocker.MagicMock()
    mock_workspace_client.dbutils.notebook.entry_point.getDbutils.side_effect = Exception("No context")
    mock_workspace_client.config.warehouse_id = None
    mock_workspace_client.config.host = None
    mock_workspace_client.warehouses.list.return_value = iter([])  # Empty iterator
    mocker.patch("databricks.sdk.WorkspaceClient", return_value=mock_workspace_client)

    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__ACCESS_TOKEN"] = "test_token"
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__CATALOG"] = "test_catalog"
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH"] = "/sql/1.0/test"
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME"] = ""

    with pytest.raises(
        ConfigurationValueError,
        match="Configuration error: Missing required parameter `server_hostname`",
    ):
        bricks = databricks()
        bricks.configuration(None, accept_partial=True)


def test_shared_cluster_authentication(mocker) -> None:
    """Test that credentials work correctly when running on a shared cluster.

    When running on a shared Databricks cluster without explicit credentials,
    the code should attempt to use WorkspaceClient's default authentication.
    """
    # Clear all relevant environment variables to ensure clean test state
    for key in list(os.environ.keys()):
        if "DATABRICKS" in key or "CREDENTIALS" in key:
            del os.environ[key]

    # Create a mock authenticator function that simulates shared cluster auth
    def mock_authenticator():
        return {"Authorization": "Bearer mock-token"}

    # Mock the WorkspaceClient to simulate shared cluster environment
    mock_workspace_client = mocker.MagicMock()
    mock_workspace_client.config.authenticate = mock_authenticator
    mock_workspace_client.config.auth_type = "azure-cli"
    # Make dbutils fail (simulating non-notebook context)
    mock_workspace_client.dbutils.notebook.entry_point.getDbutils.side_effect = Exception(
        "Not in notebook context"
    )

    mocker.patch(
        "databricks.sdk.WorkspaceClient",
        return_value=mock_workspace_client,
    )

    # Test credentials directly to avoid full configuration resolution complexity
    creds = DatabricksCredentials(
        catalog="test_catalog",
        server_hostname="test.databricks.com",
        http_path="/sql/1.0/test",
    )
    # Manually trigger on_resolved
    creds.on_resolved()

    # Verify the authenticator was picked up
    assert callable(creds.access_token)
    assert creds.access_token == mock_authenticator

    # Verify connector params use credentials_provider for callable access_token
    params = creds.to_connector_params()
    assert "credentials_provider" in params
    assert callable(params["credentials_provider"])

def test_cluster_context_connection_params(mocker) -> None:
    """Test that server_hostname and http_path are fetched from cluster context.

    When running in a Databricks notebook on user's compute, the code should
    get server_hostname from workspace URL and construct http_path from
    workspace ID and cluster ID.
    """
    # Clear all relevant environment variables to ensure clean test state
    for key in list(os.environ.keys()):
        if "DATABRICKS" in key or "CREDENTIALS" in key:
            del os.environ[key]

    # Mock the WorkspaceClient to simulate notebook environment
    mock_workspace_client = mocker.MagicMock()

    # Set up workspace config
    mock_workspace_client.config.host = "https://adb-1234567890123456.12.azuredatabricks.net"

    # Create mock chain for notebook context
    mock_cluster_id = mocker.MagicMock()
    mock_cluster_id.get.return_value = "0123-456789-abcdefgh"
    mock_workspace_id = mocker.MagicMock()
    mock_workspace_id.get.return_value = "1234567890123456"

    mock_context = mocker.MagicMock()
    mock_context.clusterId.return_value = mock_cluster_id
    mock_context.workspaceId.return_value = mock_workspace_id

    # For apiToken
    mock_option = mocker.MagicMock()
    mock_option.getOrElse.return_value = "notebook-context-token"
    mock_context.apiToken.return_value = mock_option

    mock_notebook = mocker.MagicMock()
    mock_notebook.getContext.return_value = mock_context
    mock_dbutils = mocker.MagicMock()
    mock_dbutils.notebook.return_value = mock_notebook
    mock_entry_point = mocker.MagicMock()
    mock_entry_point.getDbutils.return_value = mock_dbutils
    mock_workspace_client.dbutils.notebook.entry_point = mock_entry_point

    mocker.patch(
        "databricks.sdk.WorkspaceClient",
        return_value=mock_workspace_client,
    )

    # Test credentials with only catalog provided
    creds = DatabricksCredentials(
        catalog="test_catalog",
    )
    # Manually trigger on_resolved
    creds.on_resolved()

    # Verify connection params were fetched from cluster context
    assert creds.server_hostname == "adb-1234567890123456.12.azuredatabricks.net"
    assert creds.http_path == "/sql/protocolv1/o/1234567890123456/0123-456789-abcdefgh"


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
