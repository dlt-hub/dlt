from typing import Any, Dict, Union, Type, get_args, cast

import os
import json  # noqa: I251
import pytest

import dlt
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import (
    AnyAzureCredentials,
    AzureServicePrincipalCredentialsWithoutDefaults,
    AzureCredentialsWithoutDefaults,
    AwsCredentials,
    AwsCredentialsWithoutDefaults,
    GcpCredentials,
    GcpServiceAccountCredentialsWithoutDefaults,
    GcpOAuthCredentialsWithoutDefaults,
)
from dlt.common.utils import custom_environ
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs.gcp_credentials import GcpDefaultCredentials
from dlt.common.configuration.specs.exceptions import (
    ObjectStoreRsCredentialsException,
    UnsupportedAuthenticationMethodException,
)
from dlt.common.configuration.specs.mixins import WithObjectStoreRsCredentials, WithPyicebergConfig

from tests.load.utils import (
    AZ_BUCKET,
    ABFS_BUCKET,
    AWS_BUCKET,
    GCS_BUCKET,
    R2_BUCKET_CONFIG,
    ALL_FILESYSTEM_DRIVERS,
)


TCredentialsMixin = Union[WithObjectStoreRsCredentials, WithPyicebergConfig]
ALL_CREDENTIALS_MIXINS = get_args(TCredentialsMixin)

pytestmark = pytest.mark.essential

if all(driver not in ALL_FILESYSTEM_DRIVERS for driver in ("az", "s3", "gs", "r2")):
    pytest.skip(
        "Requires at least one of `az`, `s3`, `gs`, `r2` in `ALL_FILESYSTEM_DRIVERS`.",
        allow_module_level=True,
    )


@pytest.fixture
def fs_creds() -> Dict[str, Any]:
    creds: Dict[str, Any] = dlt.secrets.get("destination.filesystem.credentials")
    if creds is None:
        pytest.skip(
            msg="`destination.filesystem.credentials` must be configured for these tests.",
        )
    return creds


def can_connect(bucket_url: str, credentials: TCredentialsMixin, mixin: Type[TCredentialsMixin]) -> bool:  # type: ignore[return]
    """Returns True if client can connect to object store, False otherwise."""
    if mixin == WithObjectStoreRsCredentials:
        credentials = cast(WithObjectStoreRsCredentials, credentials)
        return can_connect_object_store_rs_credentials(
            bucket_url, credentials.to_object_store_rs_credentials()
        )
    elif mixin == WithPyicebergConfig:
        credentials = cast(WithPyicebergConfig, credentials)
        return can_connect_pyiceberg_fileio_config(
            bucket_url, credentials.to_pyiceberg_fileio_config()
        )


def can_connect_object_store_rs_credentials(
    bucket_url: str, object_store_rs_credentials: Dict[str, str]
) -> bool:
    # uses `deltatable` library as Python interface to `object_store` Rust crate
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError

    try:
        DeltaTable(
            bucket_url,
            storage_options=object_store_rs_credentials,
        )
    except TableNotFoundError:
        # this error implies the connection was successful
        # there is no Delta table at `bucket_url`
        return True
    return False


def can_connect_pyiceberg_fileio_config(
    bucket_url: str, pyiceberg_fileio_config: Dict[str, str]
) -> bool:
    from pyiceberg.table import StaticTable

    try:
        StaticTable.from_metadata(
            f"{bucket_url}/non_existing_metadata_file.json",
            properties=pyiceberg_fileio_config,
        )
    except FileNotFoundError:
        # this error implies the connection was successful
        # there is no Iceberg metadata file at the specified path
        return True
    return False


@pytest.mark.parametrize(
    "driver", [driver for driver in ALL_FILESYSTEM_DRIVERS if driver in ("az", "abfss")]
)
@pytest.mark.parametrize("mixin", ALL_CREDENTIALS_MIXINS)
def test_azure_credentials_mixins(
    driver: str, fs_creds: Dict[str, Any], mixin: Type[TCredentialsMixin]
) -> None:
    if mixin == WithPyicebergConfig and driver == "az":
        pytest.skip("`pyiceberg` does not support `az` scheme")

    buckets = {"az": AZ_BUCKET, "abfss": ABFS_BUCKET}
    creds: AnyAzureCredentials

    creds = AzureServicePrincipalCredentialsWithoutDefaults(
        **dlt.secrets.get("destination.fsazureprincipal.credentials")
    )
    assert can_connect(buckets[driver], creds, mixin)

    # without SAS token
    creds = AzureCredentialsWithoutDefaults(
        azure_storage_account_name=fs_creds["azure_storage_account_name"],
        azure_storage_account_key=fs_creds["azure_storage_account_key"],
    )
    assert creds.azure_storage_sas_token is None
    assert can_connect(buckets[driver], creds, mixin)

    # with SAS token
    creds = resolve_configuration(creds)
    assert creds.azure_storage_sas_token is not None
    assert can_connect(buckets[driver], creds, mixin)


@pytest.mark.parametrize(
    "driver", [driver for driver in ALL_FILESYSTEM_DRIVERS if driver in ("s3", "r2")]
)
@pytest.mark.parametrize("mixin", ALL_CREDENTIALS_MIXINS)
def test_aws_credentials_mixins(
    driver: str, fs_creds: Dict[str, Any], mixin: Type[TCredentialsMixin]
) -> None:
    creds: AwsCredentialsWithoutDefaults

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
    if mixin == WithObjectStoreRsCredentials:
        assert (
            "aws_session_token" not in creds.to_object_store_rs_credentials()
        )  # no auto-generated token
    assert can_connect(AWS_BUCKET, creds, mixin)

    # AwsCredentials: no user-provided session token
    creds = AwsCredentials(
        aws_access_key_id=fs_creds["aws_access_key_id"],
        aws_secret_access_key=fs_creds["aws_secret_access_key"],
        region_name=fs_creds.get("region_name"),
        endpoint_url=fs_creds.get("endpoint_url"),
    )
    assert creds.aws_session_token is None
    assert can_connect(AWS_BUCKET, creds, mixin)
    if mixin == WithObjectStoreRsCredentials:
        object_store_rs_creds = creds.to_object_store_rs_credentials()
        assert "aws_session_token" not in object_store_rs_creds  # no auto-generated token

        # exception should be raised if both `endpoint_url` and `region_name` are
        # not provided
        with pytest.raises(ObjectStoreRsCredentialsException):
            AwsCredentials(
                aws_access_key_id=fs_creds["aws_access_key_id"],
                aws_secret_access_key=fs_creds["aws_secret_access_key"],
            ).to_object_store_rs_credentials()

        if "endpoint_url" in object_store_rs_creds and object_store_rs_creds[
            "endpoint_url"
        ].startswith("http://"):
            # TODO: make sure this case is tested on GitHub CI, e.g. by adding
            # a local MinIO bucket to the set of tested buckets
            assert object_store_rs_creds["aws_allow_http"] == "true"

    if creds.endpoint_url is not None:
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
    assert can_connect(AWS_BUCKET, creds, mixin)
    if mixin == WithObjectStoreRsCredentials:
        object_store_rs_creds = creds.to_object_store_rs_credentials()
        assert object_store_rs_creds["aws_session_token"] is not None

    # AwsCredentialsWithoutDefaults: user-provided session token
    creds = AwsCredentialsWithoutDefaults(
        aws_access_key_id=sess_creds["aws_access_key_id"],
        aws_secret_access_key=sess_creds["aws_secret_access_key"],
        aws_session_token=sess_creds["aws_session_token"],
        region_name=fs_creds["region_name"],
    )
    assert creds.aws_session_token is not None
    assert can_connect(AWS_BUCKET, creds, mixin)
    if mixin == WithObjectStoreRsCredentials:
        object_store_rs_creds = creds.to_object_store_rs_credentials()
        assert object_store_rs_creds["aws_session_token"] is not None


@pytest.mark.parametrize(
    "driver", [driver for driver in ALL_FILESYSTEM_DRIVERS if driver in ("gs")]
)
@pytest.mark.parametrize("mixin", ALL_CREDENTIALS_MIXINS)
def test_gcp_credentials_mixins(
    driver, fs_creds: Dict[str, Any], mixin: Type[TCredentialsMixin]
) -> None:
    creds: GcpCredentials

    # GcpServiceAccountCredentialsWithoutDefaults
    creds = GcpServiceAccountCredentialsWithoutDefaults(
        project_id=fs_creds["project_id"],
        private_key=fs_creds["private_key"],
        # private_key_id must be configured in order for data lake to work
        private_key_id=fs_creds["private_key_id"],
        client_email=fs_creds["client_email"],
    )
    if mixin == WithPyicebergConfig:
        with pytest.raises(UnsupportedAuthenticationMethodException):
            assert can_connect(GCS_BUCKET, creds, mixin)
    elif mixin == WithObjectStoreRsCredentials:
        assert can_connect(GCS_BUCKET, creds, mixin)

    # GcpDefaultCredentials

    # reset failed default credentials timeout so we resolve below
    GcpDefaultCredentials._LAST_FAILED_DEFAULT = 0

    # write service account key to JSON file
    service_json = json.loads(creds.to_native_representation())
    path = "_secrets/service.json"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(service_json, f)

    with custom_environ({"GOOGLE_APPLICATION_CREDENTIALS": path}):
        creds = GcpDefaultCredentials()
        resolve_configuration(creds)
        if mixin == WithPyicebergConfig:
            with pytest.raises(UnsupportedAuthenticationMethodException):
                assert can_connect(GCS_BUCKET, creds, mixin)
        elif mixin == WithObjectStoreRsCredentials:
            assert can_connect(GCS_BUCKET, creds, mixin)

    # GcpOAuthCredentialsWithoutDefaults
    creds = resolve_configuration(
        GcpOAuthCredentialsWithoutDefaults(), sections=("destination", "fsgcpoauth")
    )
    if mixin == WithPyicebergConfig:
        assert can_connect(GCS_BUCKET, creds, mixin)
    elif mixin == WithObjectStoreRsCredentials:
        with pytest.raises(UnsupportedAuthenticationMethodException):
            assert can_connect(GCS_BUCKET, creds, mixin)
