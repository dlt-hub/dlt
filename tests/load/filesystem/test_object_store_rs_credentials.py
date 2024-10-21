"""Tests translation of `dlt` credentials into `object_store` Rust crate credentials."""

from typing import Any, Dict

import pytest
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

import dlt
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
from dlt.common.configuration.specs.exceptions import ObjectStoreRsCredentialsException

from tests.load.utils import (
    AZ_BUCKET,
    AWS_BUCKET,
    GCS_BUCKET,
    R2_BUCKET_CONFIG,
    ALL_FILESYSTEM_DRIVERS,
)

if all(driver not in ALL_FILESYSTEM_DRIVERS for driver in ("az", "s3", "gs", "r2")):
    pytest.skip(
        "Requires at least one of `az`, `s3`, `gs`, `r2` in `ALL_FILESYSTEM_DRIVERS`.",
        allow_module_level=True,
    )


FS_CREDS: Dict[str, Any] = dlt.secrets.get("destination.filesystem.credentials")
if FS_CREDS is None:
    pytest.skip(
        msg="`destination.filesystem.credentials` must be configured for these tests.",
        allow_module_level=True,
    )


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


@pytest.mark.parametrize(
    "driver", [driver for driver in ALL_FILESYSTEM_DRIVERS if driver in ("az")]
)
def test_azure_object_store_rs_credentials(driver: str) -> None:
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


@pytest.mark.parametrize(
    "driver", [driver for driver in ALL_FILESYSTEM_DRIVERS if driver in ("s3", "r2")]
)
def test_aws_object_store_rs_credentials(driver: str) -> None:
    creds: AwsCredentialsWithoutDefaults

    fs_creds = FS_CREDS
    if driver == "r2":
        fs_creds = R2_BUCKET_CONFIG["credentials"]  # type: ignore[assignment]

    # AwsCredentialsWithoutDefaults: no user-provided session token
    creds = AwsCredentialsWithoutDefaults(
        aws_access_key_id=fs_creds["aws_access_key_id"],
        aws_secret_access_key=fs_creds["aws_secret_access_key"],
        region_name=fs_creds.get("region_name"),
        endpoint_url=fs_creds.get("endpoint_url"),
    )
    assert creds.aws_session_token is None
    object_store_rs_creds = creds.to_object_store_rs_credentials()
    assert "aws_session_token" not in object_store_rs_creds  # no auto-generated token
    assert can_connect(AWS_BUCKET, object_store_rs_creds)

    # AwsCredentials: no user-provided session token
    creds = AwsCredentials(
        aws_access_key_id=fs_creds["aws_access_key_id"],
        aws_secret_access_key=fs_creds["aws_secret_access_key"],
        region_name=fs_creds.get("region_name"),
        endpoint_url=fs_creds.get("endpoint_url"),
    )
    assert creds.aws_session_token is None
    object_store_rs_creds = creds.to_object_store_rs_credentials()
    assert "aws_session_token" not in object_store_rs_creds  # no auto-generated token
    assert can_connect(AWS_BUCKET, object_store_rs_creds)

    # exception should be raised if both `endpoint_url` and `region_name` are
    # not provided
    with pytest.raises(ObjectStoreRsCredentialsException):
        AwsCredentials(
            aws_access_key_id=fs_creds["aws_access_key_id"],
            aws_secret_access_key=fs_creds["aws_secret_access_key"],
        ).to_object_store_rs_credentials()

    if "endpoint_url" in object_store_rs_creds:
        # TODO: make sure this case is tested on GitHub CI, e.g. by adding
        # a local MinIO bucket to the set of tested buckets
        if object_store_rs_creds["endpoint_url"].startswith("http://"):
            assert object_store_rs_creds["aws_allow_http"] == "true"

        # remainder of tests use session tokens
        # we don't run them on S3 compatible storage because session tokens
        # may not be available
        return

    # AwsCredentials: user-provided session token
    # use previous credentials to create session token for new credentials
    assert isinstance(creds, AwsCredentials)
    sess_creds = creds.to_session_credentials()
    creds = AwsCredentials(
        aws_access_key_id=sess_creds["aws_access_key_id"],
        aws_secret_access_key=sess_creds["aws_secret_access_key"],
        aws_session_token=sess_creds["aws_session_token"],
        region_name=fs_creds["region_name"],
    )
    assert creds.aws_session_token is not None
    object_store_rs_creds = creds.to_object_store_rs_credentials()
    assert object_store_rs_creds["aws_session_token"] is not None
    assert can_connect(AWS_BUCKET, object_store_rs_creds)

    # AwsCredentialsWithoutDefaults: user-provided session token
    creds = AwsCredentialsWithoutDefaults(
        aws_access_key_id=sess_creds["aws_access_key_id"],
        aws_secret_access_key=sess_creds["aws_secret_access_key"],
        aws_session_token=sess_creds["aws_session_token"],
        region_name=fs_creds["region_name"],
    )
    assert creds.aws_session_token is not None
    object_store_rs_creds = creds.to_object_store_rs_credentials()
    assert object_store_rs_creds["aws_session_token"] is not None
    assert can_connect(AWS_BUCKET, object_store_rs_creds)


@pytest.mark.parametrize(
    "driver", [driver for driver in ALL_FILESYSTEM_DRIVERS if driver in ("gs")]
)
def test_gcp_object_store_rs_credentials(driver) -> None:
    creds = GcpServiceAccountCredentialsWithoutDefaults(
        project_id=FS_CREDS["project_id"],
        private_key=FS_CREDS["private_key"],
        # private_key_id must be configured in order for data lake to work
        private_key_id=FS_CREDS["private_key_id"],
        client_email=FS_CREDS["client_email"],
    )
    assert can_connect(GCS_BUCKET, creds.to_object_store_rs_credentials())

    # GcpOAuthCredentialsWithoutDefaults is currently not supported
    with pytest.raises(NotImplementedError):
        GcpOAuthCredentialsWithoutDefaults().to_object_store_rs_credentials()
