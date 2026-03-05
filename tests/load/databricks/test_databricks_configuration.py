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


def _setup_sdk_auth(mock_ws):
    """Enable SDK default authentication on the mock WorkspaceClient."""
    mock_ws.config.authenticate = lambda: {"Authorization": "Bearer token"}
    mock_ws.config.auth_type = "pat"


def _setup_notebook_context(
    mock_ws,
    mocker,
    cluster_id="0123-456789-abcdefgh",
    workspace_id="1234567890123456",
    api_token="nb-token",
):
    """Set up the dbutils notebook context chain on the mock WorkspaceClient."""
    mock_cluster_id = mocker.MagicMock()
    mock_cluster_id.get.return_value = cluster_id
    mock_workspace_id = mocker.MagicMock()
    mock_workspace_id.get.return_value = workspace_id

    mock_context = mocker.MagicMock()
    mock_context.clusterId.return_value = mock_cluster_id
    mock_context.workspaceId.return_value = mock_workspace_id

    mock_option = mocker.MagicMock()
    mock_option.getOrElse.return_value = api_token
    mock_context.apiToken.return_value = mock_option

    mock_notebook = mocker.MagicMock()
    mock_notebook.getContext.return_value = mock_context
    mock_dbutils = mocker.MagicMock()
    mock_dbutils.notebook.return_value = mock_notebook
    mock_entry_point = mocker.MagicMock()
    mock_entry_point.getDbutils.return_value = mock_dbutils
    mock_ws.dbutils.notebook.entry_point = mock_entry_point


def _setup_warehouse(
    mock_ws,
    mocker,
    warehouse_id="wh-1",
    hostname="warehouse.databricks.com",
    path="/sql/1.0/warehouses/wh-1",
):
    """Set up warehouse mock. Uses get() if warehouse_id is set on config, list() otherwise."""
    mock_warehouse = mocker.MagicMock()
    mock_warehouse.id = warehouse_id
    mock_warehouse.odbc_params.hostname = hostname
    mock_warehouse.odbc_params.path = path
    if mock_ws.config.warehouse_id:
        mock_ws.warehouses.get.return_value = mock_warehouse
    else:
        mock_ws.warehouses.list.return_value = iter([mock_warehouse])


@pytest.fixture
def mock_databricks_env(mocker):
    """Clears Databricks-related env vars and provides a mock WorkspaceClient.

    Returns a mock WorkspaceClient instance. By default, notebook context and
    warehouse discovery are disabled (raise exceptions). Tests can override
    specific attributes on the returned mock as needed.
    """
    # clear only DESTINATION__DATABRICKS__ prefixed env vars to avoid
    # interfering with other destination tests
    for key in list(os.environ.keys()):
        if key.startswith("DESTINATION__DATABRICKS__") or key.startswith("DATABRICKS_"):
            del os.environ[key]

    mock_ws = mocker.MagicMock()
    mock_ws.dbutils.notebook.entry_point.getDbutils.side_effect = Exception("No notebook context")
    mock_ws.config.authenticate = None
    mock_ws.config.warehouse_id = None
    mock_ws.config.host = None
    mock_ws.warehouses.list.return_value = iter([])
    mocker.patch("databricks.sdk.WorkspaceClient", return_value=mock_ws)
    return mock_ws


def test_databricks_auth_invalid(mock_databricks_env) -> None:
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_ID"] = ""
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__CLIENT_SECRET"] = ""
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__ACCESS_TOKEN"] = ""
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__CATALOG"] = "test_catalog"
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME"] = "test.databricks.com"
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH"] = "/sql/1.0/test"

    with pytest.raises(ConfigurationValueError, match="Authentication failed:.*"):
        bricks = databricks()
        bricks.configuration(None, accept_partial=True)


def test_databricks_missing_config_catalog(mock_databricks_env) -> None:
    mock_databricks_env.warehouses.list.side_effect = Exception("No warehouses")

    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__ACCESS_TOKEN"] = "test_token"
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME"] = "test.databricks.com"
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH"] = "/sql/1.0/test"
    os.environ["DESTINATION__DATABRICKS__CREDENTIALS__CATALOG"] = ""

    with pytest.raises(
        ConfigurationValueError, match="Configuration error: Missing required parameter `catalog`"
    ):
        bricks = databricks()
        bricks.configuration(None, accept_partial=True)


def test_databricks_missing_config_http_path(mock_databricks_env) -> None:
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


def test_databricks_missing_config_server_hostname(mock_databricks_env) -> None:
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


def test_shared_cluster_authentication(mock_databricks_env) -> None:
    """Test that default SDK authentication is used when no explicit credentials
    are provided and notebook context is unavailable."""

    def mock_authenticator():
        return {"Authorization": "Bearer mock-token"}

    mock_databricks_env.config.authenticate = mock_authenticator
    mock_databricks_env.config.auth_type = "azure-cli"

    creds = DatabricksCredentials(
        catalog="test_catalog",
        server_hostname="test.databricks.com",
        http_path="/sql/1.0/test",
    )
    creds.on_resolved()

    assert callable(creds.access_token)
    assert creds.access_token == mock_authenticator

    params = creds.to_connector_params()
    assert "credentials_provider" in params
    assert callable(params["credentials_provider"])


def test_cluster_context_connection_params(mock_databricks_env, mocker) -> None:
    """Test that server_hostname and http_path are derived from the notebook
    cluster context when not explicitly provided."""
    mock_ws = mock_databricks_env
    mock_ws.config.host = "https://adb-1234567890123456.12.azuredatabricks.net"
    _setup_sdk_auth(mock_ws)
    _setup_notebook_context(mock_ws, mocker)

    creds = DatabricksCredentials(catalog="test_catalog")
    creds.on_resolved()

    assert creds.server_hostname == "adb-1234567890123456.12.azuredatabricks.net"
    assert creds.http_path == "/sql/protocolv1/o/1234567890123456/0123-456789-abcdefgh"


def test_warehouse_provides_both_params(mock_databricks_env, mocker) -> None:
    """No workspace URL, no cluster context — both server_hostname and http_path
    come from the first warehouse on the list."""
    _setup_sdk_auth(mock_databricks_env)
    _setup_warehouse(mock_databricks_env, mocker)

    creds = DatabricksCredentials(catalog="test_catalog")
    creds.on_resolved()

    assert creds.server_hostname == "warehouse.databricks.com"
    assert creds.http_path == "/sql/1.0/warehouses/wh-1"


def test_hostname_from_workspace_http_path_from_warehouse(mock_databricks_env, mocker) -> None:
    """Workspace URL resolves server_hostname before warehouse fallback, so
    warehouse only fills in http_path."""
    mock_databricks_env.config.host = "https://adb-123.azuredatabricks.net"
    _setup_sdk_auth(mock_databricks_env)
    _setup_warehouse(mock_databricks_env, mocker)

    creds = DatabricksCredentials(catalog="test_catalog")
    creds.on_resolved()

    assert creds.server_hostname == "adb-123.azuredatabricks.net"
    assert creds.http_path == "/sql/1.0/warehouses/wh-1"


def test_warehouse_id_selects_specific_warehouse(mock_databricks_env, mocker) -> None:
    """When DATABRICKS_WAREHOUSE_ID is set, warehouses.get() is used instead
    of warehouses.list()."""
    _setup_sdk_auth(mock_databricks_env)
    mock_databricks_env.config.warehouse_id = "specific-wh"
    _setup_warehouse(
        mock_databricks_env,
        mocker,
        warehouse_id="specific-wh",
        path="/sql/1.0/warehouses/specific-wh",
    )

    creds = DatabricksCredentials(catalog="test_catalog")
    creds.on_resolved()

    mock_databricks_env.warehouses.get.assert_called_once_with("specific-wh")
    assert creds.server_hostname == "warehouse.databricks.com"
    assert creds.http_path == "/sql/1.0/warehouses/specific-wh"


def test_explicit_params_skip_auto_resolution(mock_databricks_env) -> None:
    """Test that explicitly provided server_hostname and http_path are not
    overwritten by auto-resolution."""
    mock_databricks_env.config.host = "https://should-not-overwrite.databricks.net"
    _setup_sdk_auth(mock_databricks_env)

    creds = DatabricksCredentials(
        catalog="test_catalog",
        server_hostname="explicit.databricks.com",
        http_path="/sql/1.0/explicit",
    )
    creds.on_resolved()

    assert creds.server_hostname == "explicit.databricks.com"
    assert creds.http_path == "/sql/1.0/explicit"


def test_notebook_token_overwritten_by_sdk_auth(mock_databricks_env, mocker) -> None:
    """Test that w.config.authenticate always takes precedence over notebook
    token — matches devel behavior where the second try block overwrites."""

    def sdk_authenticator():
        return {"Authorization": "Bearer sdk-token"}

    mock_databricks_env.config.authenticate = sdk_authenticator
    mock_databricks_env.config.auth_type = "azure-cli"
    _setup_notebook_context(mock_databricks_env, mocker)

    creds = DatabricksCredentials(
        catalog="test_catalog",
        server_hostname="test.databricks.com",
        http_path="/sql/1.0/test",
    )
    creds.on_resolved()

    # sdk authenticator overwrites notebook token (devel parity)
    assert creds.access_token is sdk_authenticator


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
