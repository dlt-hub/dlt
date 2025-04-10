from typing import Any, Dict

import pytest

from dlt.common.configuration.specs import (
    AnyAzureCredentials,
    AzureCredentialsWithoutDefaults,
    AzureServicePrincipalCredentialsWithoutDefaults,
    AwsCredentials,
    AwsCredentialsWithoutDefaults,
    GcpOAuthCredentialsWithoutDefaults,
    GcpServiceAccountCredentialsWithoutDefaults,
)
from dlt.common.configuration.specs.exceptions import UnsupportedAuthenticationMethodException

from tests.load.utils import (
    ABFS_BUCKET,
    AWS_BUCKET,
    R2_BUCKET_CONFIG,
)
from tests.load.filesystem.test_credentials_mixins import can_connect_pyiceberg_fileio_config


pytestmark = pytest.mark.essential


@pytest.fixture
def fs_creds() -> Dict[str, Any]:
    import dlt

    creds: Dict[str, Any] = dlt.secrets.get("destination.filesystem.credentials")
    if creds is None:
        pytest.skip(
            msg="`destination.filesystem.credentials` must be configured for these tests.",
        )
    return creds


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


def test_aws_credentials_pyiceberg_export_import(fs_creds: Dict[str, Any]) -> None:
    """test that AWS credentials can be exported to PyIceberg config and imported back."""
    # test AwsCredentialsWithoutDefaults
    original_creds = AwsCredentialsWithoutDefaults(
        aws_access_key_id=fs_creds["aws_access_key_id"],
        aws_secret_access_key=fs_creds["aws_secret_access_key"],
        region_name=fs_creds.get("region_name"),
    )

    # export to PyIceberg config
    pyiceberg_config = original_creds.to_pyiceberg_fileio_config()

    # config should contain required fields with correct values
    assert "s3.access-key-id" in pyiceberg_config
    assert pyiceberg_config["s3.access-key-id"] == fs_creds["aws_access_key_id"]
    assert "s3.secret-access-key" in pyiceberg_config
    assert pyiceberg_config["s3.secret-access-key"] == fs_creds["aws_secret_access_key"]
    assert "s3.region" in pyiceberg_config
    assert pyiceberg_config["s3.region"] == fs_creds.get("region_name")

    # import back from PyIceberg config
    imported_creds = AwsCredentialsWithoutDefaults.from_pyiceberg_fileio_config(pyiceberg_config)

    # verify credentials were restored correctly
    assert imported_creds.aws_access_key_id == original_creds.aws_access_key_id
    assert imported_creds.aws_secret_access_key == original_creds.aws_secret_access_key
    assert imported_creds.region_name == original_creds.region_name

    # test connection using imported credentials
    assert can_connect_pyiceberg_fileio_config(AWS_BUCKET, pyiceberg_config)


def test_aws_r2_credentials_pyiceberg_export_import() -> None:
    """test that AWS R2 credentials can be exported to PyIceberg config and imported back."""
    # skip if R2 bucket config is not available
    if not R2_BUCKET_CONFIG:
        pytest.skip("R2 bucket configuration not available")

    fs_creds: Dict[str, Any] = R2_BUCKET_CONFIG["credentials"]  # type: ignore[assignment]

    # test AwsCredentialsWithoutDefaults for R2
    original_creds = AwsCredentialsWithoutDefaults(
        aws_access_key_id=fs_creds["aws_access_key_id"],
        aws_secret_access_key=fs_creds["aws_secret_access_key"],
        endpoint_url=fs_creds.get("endpoint_url"),
    )

    # export to PyIceberg config
    pyiceberg_config = original_creds.to_pyiceberg_fileio_config()

    # config should contain required fields with correct values
    assert "s3.access-key-id" in pyiceberg_config
    assert pyiceberg_config["s3.access-key-id"] == fs_creds["aws_access_key_id"]
    assert "s3.secret-access-key" in pyiceberg_config
    assert pyiceberg_config["s3.secret-access-key"] == fs_creds["aws_secret_access_key"]
    assert "s3.endpoint" in pyiceberg_config
    assert pyiceberg_config["s3.endpoint"] == fs_creds.get("endpoint_url")

    # import back from PyIceberg config
    imported_creds = AwsCredentialsWithoutDefaults.from_pyiceberg_fileio_config(pyiceberg_config)

    # verify credentials were restored correctly
    assert imported_creds.aws_access_key_id == original_creds.aws_access_key_id
    assert imported_creds.aws_secret_access_key == original_creds.aws_secret_access_key
    assert imported_creds.endpoint_url == original_creds.endpoint_url


def test_gcp_oauth_credentials_pyiceberg_export_import() -> None:
    """test that GCP OAuth credentials can be exported to PyIceberg config and imported back."""
    import dlt

    # get GCP OAuth credentials
    oauth_config: Dict[str, Any] = dlt.secrets.get("destination.fsgcpoauth.credentials")
    if not oauth_config:
        pytest.skip("GCP OAuth credentials not configured")

    # create original credentials
    original_creds = GcpOAuthCredentialsWithoutDefaults(
        project_id=oauth_config.get("project_id", "test-project"),
        client_id=oauth_config.get("client_id"),
        client_secret=oauth_config.get("client_secret"),
        refresh_token=oauth_config.get("refresh_token"),
    )

    # set a test token value to bypass auth flow in tests
    original_creds.token = "test-token"

    # export to PyIceberg config
    pyiceberg_config = original_creds.to_pyiceberg_fileio_config()

    # config should contain required fields with correct values
    assert "gcs.project-id" in pyiceberg_config
    assert pyiceberg_config["gcs.project-id"] == original_creds.project_id
    assert "gcs.oauth2.token" in pyiceberg_config
    assert pyiceberg_config["gcs.oauth2.token"] == original_creds.token
    assert "gcs.oauth2.token-expires-at" in pyiceberg_config

    # import back from PyIceberg config
    imported_creds = GcpOAuthCredentialsWithoutDefaults.from_pyiceberg_fileio_config(
        pyiceberg_config
    )

    # verify credentials were restored correctly
    assert imported_creds.project_id == original_creds.project_id
    assert imported_creds.token == original_creds.token


def test_gcp_service_account_credentials_pyiceberg_not_supported() -> None:
    """test that GCP Service Account credentials raise the expected exception with PyIceberg."""
    import dlt

    # get GCP Service Account credentials
    sa_config: Dict[str, Any] = dlt.secrets.get("destination.filesystem.credentials")
    if not sa_config.get("private_key"):
        pytest.skip("GCP Service Account credentials not configured")

    # create service account credentials
    sa_creds = GcpServiceAccountCredentialsWithoutDefaults(
        project_id=sa_config["project_id"],
        private_key=sa_config["private_key"],
        private_key_id=sa_config["private_key_id"],
        client_email=sa_config["client_email"],
    )

    # should raise exception when calling to_pyiceberg_fileio_config
    with pytest.raises(UnsupportedAuthenticationMethodException):
        sa_creds.to_pyiceberg_fileio_config()

    # should also raise exception when calling from_pyiceberg_fileio_config
    with pytest.raises(UnsupportedAuthenticationMethodException):
        GcpServiceAccountCredentialsWithoutDefaults.from_pyiceberg_fileio_config({})
