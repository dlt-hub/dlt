from typing import Any, Dict, cast

import pytest
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

import dlt
from dlt.common.typing import TSecretStrValue
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import (
    CredentialsConfiguration,
    AzureServicePrincipalCredentialsWithoutDefaults,
    AzureCredentialsWithoutDefaults,
    AwsCredentials,
    AwsCredentialsWithoutDefaults,
    GcpServiceAccountCredentialsWithoutDefaults,
    GcpOAuthCredentialsWithoutDefaults,
)


def test_object_store_rs_credentials() -> None:
    """Tests translation of `dlt` credentials into `object_store` Rust crate credentials."""

    def can_connect(bucket_url: str, object_store_rs_credentials: Dict[str, str]) -> bool:
        """Returns True if client can connect to object store, False otherwise.

        Uses `deltatable` library as Python interface to `object_store` Rust crate.
        """
        try:
            DeltaTable(
                bucket_url,
                storage_options=object_store_rs_credentials,
            )
        except TableNotFoundError:
            # this error implies the connection was succesful
            # there is no Delta table at `bucket_url`
            return True
        return False

    fs_creds: Dict[str, Any] = dlt.secrets.get("destination.filesystem.credentials")
    creds: CredentialsConfiguration

    ### Azure
    bucket_url = dlt.config.get("tests.bucket_url_az", str)

    creds = AzureServicePrincipalCredentialsWithoutDefaults(
        **dlt.secrets.get("destination.fsazureprincipal.credentials")
    )
    assert can_connect(bucket_url, creds.to_object_store_rs_credentials())

    # without SAS token
    creds = AzureCredentialsWithoutDefaults(
        azure_storage_account_name=fs_creds["azure_storage_account_name"],
        azure_storage_account_key=fs_creds["azure_storage_account_key"],
    )
    assert creds.azure_storage_sas_token is None
    assert can_connect(bucket_url, creds.to_object_store_rs_credentials())

    # with SAS token
    creds = resolve_configuration(creds)
    assert creds.azure_storage_sas_token is not None
    assert can_connect(bucket_url, creds.to_object_store_rs_credentials())

    ### AWS
    bucket_url = dlt.config.get("tests.bucket_url_s3", str)

    # AwsCredentials: no user-provided session token
    creds = AwsCredentials(
        aws_access_key_id=fs_creds["aws_access_key_id"],
        aws_secret_access_key=fs_creds["aws_secret_access_key"],
        region_name=fs_creds["region_name"],
    )
    assert creds.aws_session_token is None
    object_store_rs_creds = creds.to_object_store_rs_credentials()
    assert object_store_rs_creds["aws_session_token"] is not None  # auto-generated token
    assert can_connect(bucket_url, object_store_rs_creds)

    # AwsCredentials: user-provided session token
    # use previous credentials to create session token for new credentials
    sess_creds = creds.to_session_credentials()
    creds = AwsCredentials(
        aws_access_key_id=sess_creds["aws_access_key_id"],
        aws_secret_access_key=cast(TSecretStrValue, sess_creds["aws_secret_access_key"]),
        aws_session_token=cast(TSecretStrValue, sess_creds["aws_session_token"]),
        region_name=fs_creds["region_name"],
    )
    assert creds.aws_session_token is not None
    object_store_rs_creds = creds.to_object_store_rs_credentials()
    assert object_store_rs_creds["aws_session_token"] is not None
    assert can_connect(bucket_url, object_store_rs_creds)

    # AwsCredentialsWithoutDefaults: no user-provided session token
    creds = AwsCredentialsWithoutDefaults(
        aws_access_key_id=fs_creds["aws_access_key_id"],
        aws_secret_access_key=fs_creds["aws_secret_access_key"],
        region_name=fs_creds["region_name"],
    )
    assert creds.aws_session_token is None
    object_store_rs_creds = creds.to_object_store_rs_credentials()
    assert "aws_session_token" not in object_store_rs_creds  # no auto-generated token
    assert can_connect(bucket_url, object_store_rs_creds)

    # AwsCredentialsWithoutDefaults: user-provided session token
    creds = AwsCredentialsWithoutDefaults(
        aws_access_key_id=sess_creds["aws_access_key_id"],
        aws_secret_access_key=cast(TSecretStrValue, sess_creds["aws_secret_access_key"]),
        aws_session_token=cast(TSecretStrValue, sess_creds["aws_session_token"]),
        region_name=fs_creds["region_name"],
    )
    assert creds.aws_session_token is not None
    object_store_rs_creds = creds.to_object_store_rs_credentials()
    assert object_store_rs_creds["aws_session_token"] is not None
    assert can_connect(bucket_url, object_store_rs_creds)

    ### GCP
    bucket_url = dlt.config.get("tests.bucket_url_gs", str)

    creds = GcpServiceAccountCredentialsWithoutDefaults(
        project_id=fs_creds["project_id"],
        private_key=fs_creds["private_key"],
        private_key_id=fs_creds["private_key_id"],
        client_email=fs_creds["client_email"],
    )
    assert can_connect(bucket_url, creds.to_object_store_rs_credentials())

    # GcpOAuthCredentialsWithoutDefaults is currently not supported
    with pytest.raises(NotImplementedError):
        GcpOAuthCredentialsWithoutDefaults().to_object_store_rs_credentials()
