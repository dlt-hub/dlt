"""Tests translation of `dlt` credentials into `object_store` Rust crate credentials."""

from typing import Any, Dict, cast

import pytest
from deltalake import DeltaTable  # type: ignore[import-not-found]
from deltalake.exceptions import TableNotFoundError  # type: ignore[import-not-found]

import dlt
from dlt.common.typing import TSecretStrValue
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import (
    AnyAzureCredentials,
    AzureServicePrincipalCredentialsWithoutDefaults,
    AzureCredentialsWithoutDefaults,
    AwsCredentials,
    AwsCredentialsWithoutDefaults,
    GcpServiceAccountCredentialsWithoutDefaults,
    GcpOAuthCredentialsWithoutDefaults,
)

from tests.load.utils import AZ_BUCKET, AWS_BUCKET, GCS_BUCKET, ALL_FILESYSTEM_DRIVERS

if all(driver not in ALL_FILESYSTEM_DRIVERS for driver in ("az", "s3", "gs")):
    pytest.skip(
        "Requires at least one of `az`, `s3`, `gs` in `ALL_FILESYSTEM_DRIVERS`.",
        allow_module_level=True,
    )


FS_CREDS: Dict[str, Any] = dlt.secrets.get("destination.filesystem.credentials")
assert (
    FS_CREDS is not None
), "`destination.filesystem.credentials` must be configured for these tests."


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


@pytest.mark.skipif(
    "az" not in ALL_FILESYSTEM_DRIVERS, reason="`az` not in `ALL_FILESYSTEM_DRIVERS`"
)
def test_azure_object_store_rs_credentials() -> None:
    creds: AnyAzureCredentials

    creds = AzureServicePrincipalCredentialsWithoutDefaults(
        **dlt.secrets.get("destination.fsazureprincipal.credentials")
    )
    assert can_connect(AZ_BUCKET, creds.to_object_store_rs_credentials())

    # without SAS token
    creds = AzureCredentialsWithoutDefaults(
        azure_storage_account_name=FS_CREDS["azure_storage_account_name"],
        azure_storage_account_key=FS_CREDS["azure_storage_account_key"],
    )
    assert creds.azure_storage_sas_token is None
    assert can_connect(AZ_BUCKET, creds.to_object_store_rs_credentials())

    # with SAS token
    creds = resolve_configuration(creds)
    assert creds.azure_storage_sas_token is not None
    assert can_connect(AZ_BUCKET, creds.to_object_store_rs_credentials())


@pytest.mark.skipif(
    "s3" not in ALL_FILESYSTEM_DRIVERS, reason="`s3` not in `ALL_FILESYSTEM_DRIVERS`"
)
def test_aws_object_store_rs_credentials() -> None:
    creds: AwsCredentialsWithoutDefaults

    # AwsCredentials: no user-provided session token
    creds = AwsCredentials(
        aws_access_key_id=FS_CREDS["aws_access_key_id"],
        aws_secret_access_key=FS_CREDS["aws_secret_access_key"],
        region_name=FS_CREDS["region_name"],
    )
    assert creds.aws_session_token is None
    object_store_rs_creds = creds.to_object_store_rs_credentials()
    assert object_store_rs_creds["aws_session_token"] is not None  # auto-generated token
    assert can_connect(AWS_BUCKET, object_store_rs_creds)

    # AwsCredentials: user-provided session token
    # use previous credentials to create session token for new credentials
    sess_creds = creds.to_session_credentials()
    creds = AwsCredentials(
        aws_access_key_id=sess_creds["aws_access_key_id"],
        aws_secret_access_key=cast(TSecretStrValue, sess_creds["aws_secret_access_key"]),
        aws_session_token=cast(TSecretStrValue, sess_creds["aws_session_token"]),
        region_name=FS_CREDS["region_name"],
    )
    assert creds.aws_session_token is not None
    object_store_rs_creds = creds.to_object_store_rs_credentials()
    assert object_store_rs_creds["aws_session_token"] is not None
    assert can_connect(AWS_BUCKET, object_store_rs_creds)

    # AwsCredentialsWithoutDefaults: no user-provided session token
    creds = AwsCredentialsWithoutDefaults(
        aws_access_key_id=FS_CREDS["aws_access_key_id"],
        aws_secret_access_key=FS_CREDS["aws_secret_access_key"],
        region_name=FS_CREDS["region_name"],
    )
    assert creds.aws_session_token is None
    object_store_rs_creds = creds.to_object_store_rs_credentials()
    assert "aws_session_token" not in object_store_rs_creds  # no auto-generated token
    assert can_connect(AWS_BUCKET, object_store_rs_creds)

    # AwsCredentialsWithoutDefaults: user-provided session token
    creds = AwsCredentialsWithoutDefaults(
        aws_access_key_id=sess_creds["aws_access_key_id"],
        aws_secret_access_key=cast(TSecretStrValue, sess_creds["aws_secret_access_key"]),
        aws_session_token=cast(TSecretStrValue, sess_creds["aws_session_token"]),
        region_name=FS_CREDS["region_name"],
    )
    assert creds.aws_session_token is not None
    object_store_rs_creds = creds.to_object_store_rs_credentials()
    assert object_store_rs_creds["aws_session_token"] is not None
    assert can_connect(AWS_BUCKET, object_store_rs_creds)


@pytest.mark.skipif(
    "gs" not in ALL_FILESYSTEM_DRIVERS, reason="`gs` not in `ALL_FILESYSTEM_DRIVERS`"
)
def test_gcp_object_store_rs_credentials() -> None:
    creds = GcpServiceAccountCredentialsWithoutDefaults(
        project_id=FS_CREDS["project_id"],
        private_key=FS_CREDS["private_key"],
        private_key_id=FS_CREDS["private_key_id"],
        client_email=FS_CREDS["client_email"],
    )
    assert can_connect(GCS_BUCKET, creds.to_object_store_rs_credentials())

    # GcpOAuthCredentialsWithoutDefaults is currently not supported
    with pytest.raises(NotImplementedError):
        GcpOAuthCredentialsWithoutDefaults().to_object_store_rs_credentials()
