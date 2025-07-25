from typing import Any, Dict, Optional
from urllib.parse import parse_qs
from uuid import uuid4

import pytest
from pytest_mock import MockerFixture

import dlt
from dlt.common import pendulum
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.configuration import resolve_configuration, ConfigFieldMissingException
from dlt.common.configuration.specs import (
    AzureCredentials,
    AzureServicePrincipalCredentials,
    AzureServicePrincipalCredentialsWithoutDefaults,
    AzureCredentialsWithoutDefaults,
)
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.storages.fsspec_filesystem import fsspec_from_config

from tests.load.filesystem.utils import can_connect_pyiceberg_fileio_config, fs_creds
from tests.load.utils import ABFS_BUCKET, ALL_FILESYSTEM_DRIVERS, AZ_BUCKET
from tests.common.configuration.utils import environment


# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential

if "az" not in ALL_FILESYSTEM_DRIVERS:
    pytest.skip("az filesystem driver not configured", allow_module_level=True)


@pytest.fixture
def az_service_principal_config() -> Optional[FilesystemConfiguration]:
    """FS config with alternate azure credentials format if available in environment

    Working credentials of this type may be created as an app in Entra, which has
    R/W/E access to the bucket (via ACL of particular container)

    """
    credentials = AzureServicePrincipalCredentialsWithoutDefaults(
        azure_tenant_id=dlt.config.get("tests.az_sp_tenant_id", str),
        azure_client_id=dlt.config.get("tests.az_sp_client_id", str),
        azure_client_secret=dlt.config.get("tests.az_sp_client_secret", str),
        azure_storage_account_name=dlt.config.get("tests.az_sp_storage_account_name", str),
    )
    #
    credentials = resolve_configuration(credentials, sections=("destination", "fsazureprincipal"))
    cfg = FilesystemConfiguration(bucket_url=AZ_BUCKET, credentials=credentials)

    return resolve_configuration(cfg)


def test_azure_credentials_from_account_key(environment: Dict[str, str]) -> None:
    environment["CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"] = "fake_account_name"
    environment["CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY"] = "QWERTYUIOPASDFGHJKLZXCVBNM1234567890"

    config = resolve_configuration(AzureCredentials())

    # Verify sas token is generated with correct permissions and expiry time
    sas_params = parse_qs(config.azure_storage_sas_token)

    permissions = set(sas_params["sp"][0])
    assert permissions == {"r", "w", "d", "l", "a", "c"}

    exp = ensure_pendulum_datetime(sas_params["se"][0])
    assert exp > pendulum.now().add(hours=23)


def test_create_azure_sas_token_with_permissions(environment: Dict[str, str]) -> None:
    environment["CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"] = "fake_account_name"
    environment["CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY"] = "QWERTYUIOPASDFGHJKLZXCVBNM1234567890"
    environment["CREDENTIALS__AZURE_SAS_TOKEN_PERMISSIONS"] = "rl"

    config = resolve_configuration(AzureCredentials())

    sas_params = parse_qs(config.azure_storage_sas_token)

    permissions = set(sas_params["sp"][0])
    assert permissions == {"r", "l"}


def test_azure_credentials_from_sas_token(environment: Dict[str, str]) -> None:
    environment["CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"] = "fake_account_name"
    environment["CREDENTIALS__AZURE_STORAGE_SAS_TOKEN"] = (
        "sp=rwdlacx&se=2021-01-01T00:00:00Z&sv=2019-12-12&sr=c&sig=1234567890"
    )
    environment["CREDENTIALS__AZURE_ACCOUNT_HOST"] = "blob.core.usgovcloudapi.net"

    config = resolve_configuration(AzureCredentials())

    assert config.azure_storage_sas_token == environment["CREDENTIALS__AZURE_STORAGE_SAS_TOKEN"]
    assert (
        config.azure_storage_account_name == environment["CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"]
    )
    assert config.azure_storage_account_key is None

    assert config.to_adlfs_credentials() == {
        "account_name": environment["CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"],
        "account_key": None,
        "sas_token": environment["CREDENTIALS__AZURE_STORAGE_SAS_TOKEN"],
        "account_host": "blob.core.usgovcloudapi.net",
    }


def test_azure_credentials_missing_account_name(environment: Dict[str, str]) -> None:
    with pytest.raises(ConfigFieldMissingException) as excinfo:
        resolve_configuration(AzureCredentials())

    ex = excinfo.value

    assert "azure_storage_account_name" in ex.fields


def test_azure_credentials_from_default(environment: Dict[str, str]) -> None:
    environment["CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"] = "fake_account_name"

    config = resolve_configuration(AzureCredentials())

    assert (
        config.azure_storage_account_name == environment["CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"]
    )
    assert config.azure_storage_account_key is None
    assert config.azure_storage_sas_token is None

    # fsspec args should have anon=True when using system credentials
    assert config.to_adlfs_credentials() == {
        "account_name": environment["CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"],
        "account_key": None,
        "sas_token": None,
        "anon": False,
        "account_host": None,
    }


def test_azure_service_principal_credentials(environment: Dict[str, str]) -> None:
    environment["CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"] = "fake_account_name"
    environment["CREDENTIALS__AZURE_CLIENT_ID"] = "fake_client_id"
    environment["CREDENTIALS__AZURE_CLIENT_SECRET"] = "fake_client_secret"
    environment["CREDENTIALS__AZURE_TENANT_ID"] = "fake_tenant_id"

    config = resolve_configuration(AzureServicePrincipalCredentials())

    assert config.azure_client_id == environment["CREDENTIALS__AZURE_CLIENT_ID"]
    assert config.azure_client_secret == environment["CREDENTIALS__AZURE_CLIENT_SECRET"]
    assert config.azure_tenant_id == environment["CREDENTIALS__AZURE_TENANT_ID"]

    assert config.to_adlfs_credentials() == {
        "account_name": environment["CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"],
        "account_host": None,
        "client_id": environment["CREDENTIALS__AZURE_CLIENT_ID"],
        "client_secret": environment["CREDENTIALS__AZURE_CLIENT_SECRET"],
        "tenant_id": environment["CREDENTIALS__AZURE_TENANT_ID"],
    }


def test_azure_filesystem_configuration_service_principal(environment: Dict[str, str]) -> None:
    """Filesystem config resolves correct credentials type"""
    environment["CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"] = "fake_account_name"
    environment["CREDENTIALS__AZURE_CLIENT_ID"] = "fake_client_id"
    environment["CREDENTIALS__AZURE_CLIENT_SECRET"] = "asdsadas"
    environment["CREDENTIALS__AZURE_TENANT_ID"] = str(uuid4())

    config = FilesystemConfiguration(bucket_url="az://my-bucket")

    resolved_config = resolve_configuration(config)

    assert isinstance(resolved_config.credentials, AzureServicePrincipalCredentialsWithoutDefaults)

    fs, bucket = fsspec_from_config(resolved_config)

    assert fs.tenant_id == environment["CREDENTIALS__AZURE_TENANT_ID"]
    assert fs.client_id == environment["CREDENTIALS__AZURE_CLIENT_ID"]
    assert fs.client_secret == environment["CREDENTIALS__AZURE_CLIENT_SECRET"]


def test_azure_filesystem_configuration_sas_token(environment: Dict[str, str]) -> None:
    environment["CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"] = "fake_account_name"
    environment["CREDENTIALS__AZURE_STORAGE_SAS_TOKEN"] = (
        "sp=rwdlacx&se=2021-01-01T00:00:00Z&sv=2019-12-12&sr=c&sig=1234567890"
    )

    config = FilesystemConfiguration(bucket_url="az://my-bucket")

    resolved_config = resolve_configuration(config)

    assert isinstance(resolved_config.credentials, AzureCredentialsWithoutDefaults)

    fs, bucket = fsspec_from_config(resolved_config)

    assert fs.sas_token == "?" + environment["CREDENTIALS__AZURE_STORAGE_SAS_TOKEN"]
    assert fs.account_name == environment["CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"]


def test_azure_service_principal_fs_operations(
    az_service_principal_config: Optional[FilesystemConfiguration],
) -> None:
    """Test connecting to azure filesystem with service principal credentials"""
    config = az_service_principal_config
    fs, bucket = fsspec_from_config(config)

    fn = uuid4().hex
    # Try some file ops to see if the credentials work
    fs.touch(f"{bucket}/{fn}/{fn}")
    files = fs.ls(f"{bucket}/{fn}")
    assert f"{bucket}/{fn}/{fn}" in files
    fs.delete(f"{bucket}/{fn}/{fn}")
    fs.rmdir(f"{bucket}/{fn}")


def test_account_host_kwargs(environment: Dict[str, str], mocker: MockerFixture) -> None:
    environment["CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"] = "fake_account_name"
    environment["CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY"] = "QWERTYUIOPASDFGHJKLZXCVBNM1234567890"
    environment["CREDENTIALS__AZURE_SAS_TOKEN_PERMISSIONS"] = "rl"

    # [destination.filesystem]
    # bucket_url="..."
    # [destination.filesystem.kwargs]
    # account_host="blob.core.usgovcloudapi.net"
    # [destination.filesystem.credentials]
    # ...

    config = resolve_configuration(FilesystemConfiguration(bucket_url="az://dlt-ci-test-bucket"))
    config.kwargs = {"account_host": "dlt_ci.blob.core.usgovcloudapi.net"}

    from adlfs import AzureBlobFileSystem

    connect_mock = mocker.spy(AzureBlobFileSystem, "do_connect")
    fsspec_from_config(config)

    connect_mock.assert_called_once()
    assert connect_mock.call_args[0][0].account_host == "dlt_ci.blob.core.usgovcloudapi.net"

    config = resolve_configuration(
        FilesystemConfiguration(
            bucket_url="abfss://dlt-ci-test-bucket@dlt_ci.blob.core.usgovcloudapi.net"
        )
    )
    connect_mock.reset_mock()

    assert isinstance(config.credentials, AzureCredentialsWithoutDefaults)
    assert config.credentials.azure_storage_account_name == "fake_account_name"
    # ignores the url from the bucket_url 🤷
    fs, _ = fsspec_from_config(config)
    connect_mock.assert_called_once()
    assert connect_mock.call_args[0][0].account_url.endswith(
        "fake_account_name.blob.core.windows.net"
    )

    # use host
    environment["KWARGS"] = '{"account_host": "fake_account_name.blob.core.usgovcloudapi.net"}'
    config = resolve_configuration(
        FilesystemConfiguration(
            bucket_url="abfss://dlt-ci-test-bucket@fake_account_name.blob.core.usgovcloudapi.net"
        )
    )
    connect_mock.reset_mock()

    # NOTE: fsspec is caching instances created in the same thread: skip_instance_cache
    fs, _ = fsspec_from_config(config)
    connect_mock.assert_called_once()
    # assert connect_mock.call_args[0][0].account_url.endswith("fake_account_name.blob.core.usgovcloudapi.net")
    assert fs.account_url.endswith("fake_account_name.blob.core.usgovcloudapi.net")


def test_azure_account_host(environment: Dict[str, str]) -> None:
    environment["CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"] = "fake_account_name"
    environment["CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY"] = "QWERTYUIOPASDFGHJKLZXCVBNM1234567890"
    environment["CREDENTIALS__AZURE_SAS_TOKEN_PERMISSIONS"] = "rl"
    environment["CREDENTIALS__AZURE_ACCOUNT_HOST"] = "dlt_ci.blob.core.usgovcloudapi.net"

    config = resolve_configuration(FilesystemConfiguration(bucket_url="az://dlt-ci-test-bucket"))
    fs, _ = fsspec_from_config(config)
    assert fs.account_host == "dlt_ci.blob.core.usgovcloudapi.net"


def test_azure_credentials_pyiceberg_export_import(fs_creds: Dict[str, Any]) -> None:
    """test that Azure credentials can be exported to PyIceberg config and imported back."""
    # test AzureCredentialsWithoutDefaults
    original_creds = AzureCredentialsWithoutDefaults(
        azure_storage_account_name=fs_creds["azure_storage_account_name"],
        azure_storage_account_key=fs_creds["azure_storage_account_key"],
    )
    # export to PyIceberg config
    pyiceberg_config = original_creds.to_pyiceberg_fileio_config()

    # config should contain required fields with correct values
    assert "adls.account-name" in pyiceberg_config
    assert pyiceberg_config["adls.account-name"] == fs_creds["azure_storage_account_name"]
    assert "adls.account-key" in pyiceberg_config
    assert pyiceberg_config["adls.account-key"] == fs_creds["azure_storage_account_key"]

    # import back from PyIceberg config
    imported_creds = AzureCredentialsWithoutDefaults.from_pyiceberg_fileio_config(pyiceberg_config)

    # verify credentials were restored correctly
    assert imported_creds.azure_storage_account_name == original_creds.azure_storage_account_name
    assert imported_creds.azure_storage_account_key == original_creds.azure_storage_account_key

    # test connection using imported credentials
    assert can_connect_pyiceberg_fileio_config(ABFS_BUCKET, pyiceberg_config)


def test_azure_service_principal_pyiceberg_export_import() -> None:
    """test that Azure Service Principal credentials can be exported to PyIceberg config and imported back."""
    import dlt

    # get Azure principal credentials from secrets
    principal_config: Dict[str, Any] = dlt.secrets.get("destination.fsazureprincipal.credentials")
    if not principal_config:
        pytest.skip("Azure Service Principal credentials not configured")

    # test AzureServicePrincipalCredentialsWithoutDefaults
    original_creds = AzureServicePrincipalCredentialsWithoutDefaults(
        azure_storage_account_name=principal_config["azure_storage_account_name"],
        azure_tenant_id=principal_config["azure_tenant_id"],
        azure_client_id=principal_config["azure_client_id"],
        azure_client_secret=principal_config["azure_client_secret"],
    )

    # export to PyIceberg config
    pyiceberg_config = original_creds.to_pyiceberg_fileio_config()

    # config should contain required fields with correct values
    assert "adls.account-name" in pyiceberg_config
    assert pyiceberg_config["adls.account-name"] == principal_config["azure_storage_account_name"]
    assert "adls.tenant-id" in pyiceberg_config
    assert pyiceberg_config["adls.tenant-id"] == principal_config["azure_tenant_id"]
    assert "adls.client-id" in pyiceberg_config
    assert pyiceberg_config["adls.client-id"] == principal_config["azure_client_id"]
    assert "adls.client-secret" in pyiceberg_config
    assert pyiceberg_config["adls.client-secret"] == principal_config["azure_client_secret"]

    # import back from PyIceberg config
    imported_creds = AzureServicePrincipalCredentialsWithoutDefaults.from_pyiceberg_fileio_config(
        pyiceberg_config
    )

    # verify credentials were restored correctly
    assert imported_creds.azure_storage_account_name == original_creds.azure_storage_account_name
    assert imported_creds.azure_tenant_id == original_creds.azure_tenant_id
    assert imported_creds.azure_client_id == original_creds.azure_client_id
    assert imported_creds.azure_client_secret == original_creds.azure_client_secret

    # test connection using imported credentials
    assert can_connect_pyiceberg_fileio_config(ABFS_BUCKET, pyiceberg_config)
