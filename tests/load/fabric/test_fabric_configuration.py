"""Tests for Microsoft Fabric Warehouse destination configuration"""

import base64
import copy
import pickle
import re
import sys
import time
from importlib.abc import MetaPathFinder
from types import ModuleType, SimpleNamespace
from typing import Any, Dict, List, Optional, cast

import pytest

from dlt.common import json
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.exceptions import (
    ConfigFieldMissingException,
    ConfigurationValueError,
)
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnSchema, TDataType
from dlt.common.utils import digest128
from dlt.destinations.impl.fabric.factory import fabric, FabricTypeMapper
from dlt.destinations.impl.fabric.configuration import (
    FabricCredentials,
    FabricClientConfiguration,
    FabricNotebookCredential,
)

from tests.common.configuration.utils import environment

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential

WAREHOUSE = {
    "CREDENTIALS__HOST": "test.datawarehouse.fabric.microsoft.com",
    "CREDENTIALS__DATABASE": "testdb",
}
SERVICE_PRINCIPAL = {
    "CREDENTIALS__AZURE_TENANT_ID": "test-tenant",
    "CREDENTIALS__AZURE_CLIENT_ID": "test-client",
    "CREDENTIALS__AZURE_CLIENT_SECRET": "test-secret",
}
INCOMPLETE_SERVICE_PRINCIPAL = {
    "CREDENTIALS__AZURE_TENANT_ID": "test-tenant",
    "CREDENTIALS__AZURE_CLIENT_ID": "test-client",
}
ACCESS_TOKEN = {"CREDENTIALS__ACCESS_TOKEN": "test-token"}
SQL_SCOPE = "https://database.windows.net/.default"


class FakeAzureCredential:
    def get_token(self, *scopes: str, **kwargs: Any) -> Any:
        return SimpleNamespace(token="azure-credential-token", expires_on=None)


def _jwt(expires_on: int) -> str:
    def _encode(part: Dict[str, Any]) -> str:
        return base64.urlsafe_b64encode(json.dumps(part).encode()).rstrip(b"=").decode()

    return f"{_encode({'alg': 'RS256'})}.{_encode({'exp': expires_on})}.signature"


@pytest.fixture
def notebook_tokens(monkeypatch: pytest.MonkeyPatch) -> List[str]:
    """Fakes NotebookUtils of a Fabric notebook. Tokens are served from the returned list and
    replaced with the audience they were requested for."""
    tokens: List[str] = []

    def _get_token(audience: str) -> str:
        token = tokens.pop(0)
        tokens.append(audience)
        return token

    notebookutils = ModuleType("notebookutils")
    notebookutils.credentials = SimpleNamespace(getToken=_get_token)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "notebookutils", notebookutils)
    return tokens


def test_fabric_factory() -> None:
    """Test Fabric destination factory with default settings"""
    dest = fabric()

    # Test destination properties without requiring credentials
    assert dest.destination_name == "fabric"
    assert dest.capabilities().has_case_sensitive_identifiers is True
    # Without staging configured, Fabric uses insert_values
    assert dest.capabilities().preferred_loader_file_format == "insert_values"
    assert dest.capabilities().sqlglot_dialect == "fabric"

    # arguments equal to the config defaults are dropped, others are passed to the config
    assert dest.config_params == {}
    dest = fabric(has_case_sensitive_identifiers=False, create_indexes=True)
    assert dest.config_params == {"has_case_sensitive_identifiers": False, "create_indexes": True}


def test_fabric_credentials_service_principal() -> None:
    """Test Fabric credentials with Service Principal configuration"""
    creds = FabricCredentials()
    creds.host = "abc12345-6789-def0-1234-56789abcdef0.datawarehouse.fabric.microsoft.com"
    creds.database = "mydb"
    creds.azure_tenant_id = "test-tenant-id"
    creds.azure_client_id = "test-client-id"
    creds.azure_client_secret = "test-client-secret"
    # Set driver to skip ODBC check

    # Call on_partial manually to trigger credential conversion
    creds.on_partial()

    # Check that username/password were auto-generated from Service Principal
    assert creds.azure_client_id
    assert creds.azure_client_secret


def test_fabric_credentials_odbc_dsn() -> None:
    """Test that Fabric credentials generate correct ODBC DSN with Fabric-specific parameters"""
    creds = FabricCredentials()
    creds.host = "abc12345-6789-def0-1234-56789abcdef0.datawarehouse.fabric.microsoft.com"
    creds.database = "mydb"
    creds.azure_tenant_id = "test-tenant-id"
    creds.azure_client_id = "test-client-id"
    creds.azure_client_secret = "test-client-secret"

    # Resolve to trigger on_partial and on_resolved
    # Get ODBC DSN parameters
    dsn_dict = creds.get_odbc_dsn_dict()

    # Verify Fabric-specific parameters are added
    assert dsn_dict["AUTHENTICATION"] == "ActiveDirectoryServicePrincipal"
    assert dsn_dict["UID"] == "test-client-id@test-tenant-id"
    assert dsn_dict["PWD"] == "test-client-secret"
    # mssql-python rejects both keywords
    assert "DRIVER" not in dsn_dict
    assert "LongAsMax" not in dsn_dict
    assert (
        dsn_dict["SERVER"]
        == "abc12345-6789-def0-1234-56789abcdef0.datawarehouse.fabric.microsoft.com,1433"
    )
    assert dsn_dict["DATABASE"] == "mydb"


def test_fabric_configuration_defaults() -> None:
    """Test Fabric configuration with default collation"""
    config = FabricClientConfiguration()

    # Fabric should default to UTF-8 collation
    assert config.collation == "Latin1_General_100_BIN2_UTF8"
    assert config.destination_type == "fabric"


@pytest.mark.parametrize(
    "host,port,expected_fingerprint",
    [
        pytest.param(None, None, "", id="empty"),
        pytest.param("host1", 1433, digest128("host1:1433"), id="host_default_port"),
        pytest.param("host1", 1444, digest128("host1:1444"), id="host_custom_port"),
    ],
)
def test_fabric_fingerprint(
    host: Optional[str], port: Optional[int], expected_fingerprint: str
) -> None:
    if host:
        credentials = FabricCredentials()
        credentials.host = host
        if port is not None:
            credentials.port = port
    else:
        credentials = None

    config = FabricClientConfiguration(credentials=credentials)

    assert config.fingerprint() == expected_fingerprint


def test_fabric_configuration_custom_collation() -> None:
    """Test Fabric configuration with custom collation"""
    config = FabricClientConfiguration()
    config.collation = "Latin1_General_100_CI_AS_KS_WS_SC_UTF8"

    assert config.collation == "Latin1_General_100_CI_AS_KS_WS_SC_UTF8"


def test_fabric_type_mapper() -> None:
    """Test Fabric type mapper converts nvarchar to varchar and datetimeoffset to datetime2"""
    # Create a mock table for testing
    table = cast(PreparedTableSchema, {"name": "test_table", "columns": {}})

    caps = DestinationCapabilitiesContext.generic_capabilities("parquet")
    mapper = FabricTypeMapper(caps)

    # Test that text type gets converted to varchar (not nvarchar)
    text_col = cast(TColumnSchema, {"name": "test", "data_type": "text", "nullable": True})
    result = mapper.to_destination_type(text_col, table)
    assert "varchar" in result.lower()
    assert "nvarchar" not in result.lower()

    # Test that timestamp uses datetime2 with precision 6 (not datetimeoffset)
    timestamp_col = cast(
        TColumnSchema, {"name": "test", "data_type": "timestamp", "nullable": True}
    )
    result = mapper.to_destination_type(timestamp_col, table)
    assert "datetime2" in result.lower()
    assert "datetimeoffset" not in result.lower()


def test_fabric_credentials_drivername() -> None:
    """Test that Fabric credentials use mssql+pyodbc drivername"""
    creds = FabricCredentials()
    # FabricCredentials uses mssql+pyodbc for SQLAlchemy compatibility
    assert creds.drivername == "mssql+pyodbc"


@pytest.mark.parametrize(
    "config,expected_provider,expected_token",
    [
        pytest.param(SERVICE_PRINCIPAL, None, None, id="service-principal"),
        pytest.param({}, "DefaultAzureCredential", None, id="default-credentials"),
        pytest.param(ACCESS_TOKEN, "_StaticTokenCredential", "test-token", id="access-token"),
        pytest.param(
            {**SERVICE_PRINCIPAL, **ACCESS_TOKEN},
            "_StaticTokenCredential",
            "test-token",
            id="access-token-over-service-principal",
        ),
        pytest.param(
            {**INCOMPLETE_SERVICE_PRINCIPAL, **ACCESS_TOKEN},
            "_StaticTokenCredential",
            "test-token",
            id="access-token-over-incomplete-service-principal",
        ),
    ],
)
def test_fabric_credentials_authentication(
    environment: Dict[str, str],
    config: Dict[str, str],
    expected_provider: Optional[str],
    expected_token: Optional[str],
) -> None:
    environment.update(WAREHOUSE)
    environment.update(config)
    creds = resolve_configuration(FabricCredentials())

    provider = creds.get_token_provider()
    dsn_dict = creds.get_odbc_dsn_dict()
    if expected_provider is None:
        assert provider is None
        assert dsn_dict["AUTHENTICATION"] == "ActiveDirectoryServicePrincipal"
        assert dsn_dict["UID"] == "test-client@test-tenant"
        assert dsn_dict["PWD"] == "test-secret"
    else:
        assert type(provider).__name__ == expected_provider
        # the driver refuses those keywords together with a token provider
        assert not {"AUTHENTICATION", "UID", "PWD"} & set(dsn_dict)
    if expected_token:
        assert provider.get_token(SQL_SCOPE).token == expected_token
    # the dbt runner picks the default credentials profile from this flag
    assert creds.has_default_credentials() is (expected_provider == "DefaultAzureCredential")


@pytest.mark.parametrize(
    "config,expected_message",
    [
        pytest.param(INCOMPLETE_SERVICE_PRINCIPAL, "['azure_client_secret']", id="no-secret"),
        pytest.param(
            {**SERVICE_PRINCIPAL, "CREDENTIALS__AZURE_CLIENT_SECRET": ""},
            "['azure_client_secret']",
            id="empty-secret",
        ),
        pytest.param(
            {"CREDENTIALS__AZURE_CLIENT_ID": "test-client"},
            "['azure_tenant_id', 'azure_client_secret']",
            id="client-id-only",
        ),
        pytest.param(
            {"CREDENTIALS__AZURE_CLIENT_ID": ""},
            "['azure_tenant_id', 'azure_client_id', 'azure_client_secret']",
            id="empty-client-id-only",
        ),
        pytest.param(
            {name: "" for name in SERVICE_PRINCIPAL},
            "['azure_tenant_id', 'azure_client_id', 'azure_client_secret']",
            id="all-empty",
        ),
        pytest.param(
            {**INCOMPLETE_SERVICE_PRINCIPAL, "CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME": "account"},
            "['azure_client_secret']",
            id="no-secret-with-storage-account",
        ),
        pytest.param({"CREDENTIALS__ACCESS_TOKEN": ""}, "`access_token`", id="empty-access-token"),
        pytest.param(
            {**SERVICE_PRINCIPAL, "CREDENTIALS__ACCESS_TOKEN": ""},
            "`access_token`",
            id="empty-access-token-over-service-principal",
        ),
    ],
)
def test_fabric_credentials_incomplete_authentication(
    environment: Dict[str, str],
    request: pytest.FixtureRequest,
    config: Dict[str, str],
    expected_message: str,
) -> None:
    environment.update(WAREHOUSE)
    environment.update(config)
    with pytest.raises(ConfigurationValueError, match=re.escape(expected_message)):
        resolve_configuration(FabricCredentials())

    # the notebook identity does not take over either
    request.getfixturevalue("notebook_tokens")
    with pytest.raises(ConfigurationValueError, match=re.escape(expected_message)):
        resolve_configuration(FabricCredentials())


@pytest.mark.parametrize(
    "config",
    [
        pytest.param({}, id="alone"),
        pytest.param({**SERVICE_PRINCIPAL, **ACCESS_TOKEN}, id="over-config"),
        pytest.param(INCOMPLETE_SERVICE_PRINCIPAL, id="over-incomplete-service-principal"),
    ],
)
def test_fabric_credentials_azure_credential(
    environment: Dict[str, str], notebook_tokens: List[str], config: Dict[str, str]
) -> None:
    environment.update(WAREHOUSE)
    environment.update(config)
    azure_credential = FakeAzureCredential()

    # host and database are resolved from config
    creds = resolve_configuration(FabricCredentials(), explicit_value=azure_credential)
    assert creds.host == "test.datawarehouse.fabric.microsoft.com"
    assert creds.database == "testdb"
    assert creds.is_external_session()
    assert creds.get_token_provider() is azure_credential
    assert not {"AUTHENTICATION", "UID", "PWD"} & set(creds.get_odbc_dsn_dict())

    # credentials created in code keep the credential when passed to the destination
    creds = FabricCredentials.from_credential(azure_credential)
    creds.host = "code.datawarehouse.fabric.microsoft.com"
    creds.database = "codedb"
    config_ = fabric(credentials=creds).configuration(
        FabricClientConfiguration()._bind_dataset_name("dataset")
    )
    assert config_.credentials.host == "code.datawarehouse.fabric.microsoft.com"
    assert config_.credentials.database == "codedb"
    assert config_.credentials.get_token_provider() is azure_credential


@pytest.mark.parametrize(
    "config",
    [
        pytest.param({}, id="default-credentials"),
        pytest.param(SERVICE_PRINCIPAL, id="service-principal"),
        pytest.param(ACCESS_TOKEN, id="access-token"),
    ],
)
def test_fabric_credentials_missing_warehouse(
    environment: Dict[str, str], config: Dict[str, str]
) -> None:
    environment.update(config)
    # only the warehouse is reported as missing, with every authentication method
    with pytest.raises(ConfigFieldMissingException) as ex:
        resolve_configuration(FabricCredentials())
    assert set(ex.value.fields) == {"host", "database"}

    with pytest.raises(ConfigFieldMissingException) as ex:
        resolve_configuration(FabricCredentials(), explicit_value=FakeAzureCredential())
    assert set(ex.value.fields) == {"host", "database"}

    # the destination reports the incomplete credentials and does not drop them
    # NOTE: a named destination does not read the warehouse of the test configuration
    with pytest.raises(ConfigFieldMissingException) as ex:
        fabric(destination_name="fabric_no_warehouse").configuration(
            FabricClientConfiguration()._bind_dataset_name("dataset")
        )
    assert ex.value.fields == ["credentials"]
    assert "`host`, `database`" in str(ex.value)


def test_fabric_credentials_notebook_identity(
    environment: Dict[str, str], notebook_tokens: List[str]
) -> None:
    environment.update(WAREHOUSE)
    creds = resolve_configuration(FabricCredentials())
    provider = creds.get_token_provider()
    assert isinstance(provider, FabricNotebookCredential)
    assert not {"AUTHENTICATION", "UID", "PWD"} & set(creds.get_odbc_dsn_dict())

    # NotebookUtils gets the resource, tokens are reused until shortly before they expire
    valid_token = _jwt(int(time.time()) + 3600)
    expiring_token = _jwt(int(time.time()) + 60)
    notebook_tokens.extend([valid_token, expiring_token, "not-a-jwt", valid_token])
    token = provider.get_token(SQL_SCOPE)
    assert token.token == valid_token
    assert token.expires_on is not None and token.expires_on > time.time()
    assert provider.get_token(SQL_SCOPE) is token
    assert notebook_tokens.pop() == "https://database.windows.net/"
    # copies keep the tokens, the destination is copied ie. by `pipeline.drop`
    for provider_copy in (copy.deepcopy(provider), pickle.loads(pickle.dumps(provider))):
        assert provider_copy.get_token(SQL_SCOPE) == token
    copy.deepcopy(fabric(credentials=provider))
    copy.deepcopy(creds)
    # a token is requested for each resource
    assert provider.get_token("https://storage.azure.com/.default").token == expiring_token
    assert notebook_tokens.pop() == "https://storage.azure.com/"
    # expiring tokens and tokens without known expiration are not reused
    assert provider.get_token("https://storage.azure.com/.default").token == "not-a-jwt"
    assert provider.get_token("https://storage.azure.com/.default").token == valid_token

    # explicit configuration wins over the notebook identity
    environment.update(SERVICE_PRINCIPAL)
    assert resolve_configuration(FabricCredentials()).get_token_provider() is None


def test_fabric_notebook_credential_outside_notebook() -> None:
    assert FabricNotebookCredential.is_available() is False
    with pytest.raises(ConfigurationValueError):
        FabricNotebookCredential()


def test_fabric_notebook_credential_broken_notebookutils(
    environment: Dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    class BrokenNotebookutilsFinder(MetaPathFinder):
        def find_spec(self, fullname: str, path: Any, target: Any = None) -> Any:
            if fullname == "notebookutils":
                raise ModuleNotFoundError("No module named 'py4j'", name="py4j")
            return None

    monkeypatch.setattr(sys, "meta_path", [BrokenNotebookutilsFinder(), *sys.meta_path])

    # a notebook runtime that does not import is not taken for running outside a notebook
    with pytest.raises(ModuleNotFoundError, match="py4j"):
        FabricNotebookCredential.is_available()
    environment.update(WAREHOUSE)
    with pytest.raises(ModuleNotFoundError, match="py4j"):
        resolve_configuration(FabricCredentials())


def test_fabric_credentials_service_principal_auto_conversion() -> None:
    """Test that Service Principal credentials are automatically converted to username/password"""
    creds = FabricCredentials()
    creds.host = "test.datawarehouse.fabric.microsoft.com"
    creds.database = "testdb"

    creds.azure_tenant_id = "test-tenant"
    creds.azure_client_id = "test-client"
    creds.azure_client_secret = "test-secret"

    creds = resolve_configuration(creds)
    # Verify automatic conversion happened
    assert creds.azure_client_id
    assert creds.azure_client_secret


def test_fabric_credentials_no_driver_validation() -> None:
    """Test that Fabric credentials don't enforce ODBC driver restrictions at config time"""
    # Fabric requires ODBC Driver 18, but we allow configuration with driver parameter
    # The actual driver validation happens at connection time, not during config parsing
    creds = FabricCredentials()
    creds.host = "test.datawarehouse.fabric.microsoft.com"
    creds.database = "test_db"
    creds.azure_tenant_id = "test-tenant-id"
    creds.azure_client_id = "test-client-id"
    creds.azure_client_secret = "test-client-secret"

    # Verify credentials can be created (driver validation is not enforced at this stage)
    assert creds.host == "test.datawarehouse.fabric.microsoft.com"
    assert creds.database == "test_db"


def test_fabric_credentials_odbc_dsn_escapes_secret() -> None:
    creds = FabricCredentials()
    creds.host = "test.datawarehouse.fabric.microsoft.com"
    creds.database = "testdb"
    creds.azure_tenant_id = "test-tenant"
    creds.azure_client_id = "test-client"
    creds.azure_client_secret = "se;cr}et"

    dsn = creds.to_odbc_dsn()
    assert dsn.endswith(";PWD={se;cr}}et}")


def test_fabric_credentials_authentication_method() -> None:
    """Test that Service Principal authentication method is correctly set"""
    creds = FabricCredentials()
    creds.host = "test.datawarehouse.fabric.microsoft.com"
    creds.database = "testdb"
    creds.azure_tenant_id = "test-tenant"
    creds.azure_client_id = "test-client"
    creds.azure_client_secret = "test-secret"

    # Verify ActiveDirectoryServicePrincipal is set
    dsn_dict = creds.get_odbc_dsn_dict()
    assert dsn_dict["AUTHENTICATION"] == "ActiveDirectoryServicePrincipal"


@pytest.mark.parametrize("data_type", ["text", "json"])
def test_fabric_type_mapper_scales_varchar_precision(data_type: TDataType) -> None:
    """Fabric varchar uses UTF-8 byte semantics; character precision must be scaled."""
    table = cast(PreparedTableSchema, {"name": "test_table", "columns": {}})
    caps = DestinationCapabilitiesContext.generic_capabilities("parquet")
    mapper = FabricTypeMapper(caps)

    col = cast(
        TColumnSchema, {"name": "c", "data_type": data_type, "precision": 100, "nullable": True}
    )
    assert mapper.to_destination_type(col, table) == "varchar(400)"

    # 2000*4=8000 is the longest length Fabric accepts
    col = cast(
        TColumnSchema,
        {"name": "c", "data_type": data_type, "precision": 2000, "nullable": True},
    )
    assert mapper.to_destination_type(col, table) == "varchar(8000)"

    # 2001*4=8004 > 8000 → varchar(max)
    col = cast(
        TColumnSchema,
        {"name": "c", "data_type": data_type, "precision": 2001, "nullable": True},
    )
    assert mapper.to_destination_type(col, table) == "varchar(max)"

    col = cast(TColumnSchema, {"name": "c", "data_type": data_type, "nullable": True})
    assert mapper.to_destination_type(col, table) == "varchar(max)"

    col = cast(
        TColumnSchema, {"name": "c", "data_type": data_type, "precision": 10, "nullable": True}
    )
    assert mapper.to_destination_type(col, table) == "varchar(40)"
