import pytest
from typing import Any, Dict

from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration
from dlt.common.utils import digest128
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs.aws_credentials import (
    AwsCredentials,
    AwsCredentialsWithoutDefaults,
)
from dlt.common.configuration.specs.exceptions import InvalidBoto3Session

from tests.common.configuration.utils import environment
from tests.load.filesystem.utils import can_connect_pyiceberg_fileio_config, fs_creds
from tests.load.utils import ALL_FILESYSTEM_DRIVERS, AWS_BUCKET, R2_BUCKET_CONFIG

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential

if "s3" not in ALL_FILESYSTEM_DRIVERS:
    pytest.skip("s3 filesystem driver not configured", allow_module_level=True)


def test_aws_credentials_resolved_from_default(environment: Dict[str, str]) -> None:
    set_aws_credentials_env(environment)

    config = resolve_configuration(AwsCredentials())

    assert config.aws_access_key_id == "fake_access_key"
    assert config.aws_secret_access_key == "fake_secret_key"
    assert config.aws_session_token == "fake_session_token"
    # we do not set the profile
    assert config.profile_name is None

    # use profile name other than default
    import botocore

    environment["CREDENTIALS__PROFILE_NAME"] = "fake_profile"
    with pytest.raises(botocore.exceptions.ProfileNotFound):
        resolve_configuration(AwsCredentials())

    # environment["CREDENTIALS__PROFILE_NAME"] = "default"
    # config = resolve_configuration(AwsCredentials())
    # assert config.profile_name == "default"


def test_aws_credentials_from_botocore(environment: Dict[str, str]) -> None:
    set_aws_credentials_env(environment)

    import botocore.session

    session = botocore.session.get_session()
    region_name = "eu-central-1"  # session.get_config_variable('region')

    c = AwsCredentials.from_session(session)
    assert c.profile_name is None
    assert c.aws_access_key_id == "fake_access_key"
    assert c.region_name == region_name
    assert c.profile_name is None
    assert c.is_resolved()
    assert not c.is_partial()

    s3_cred = c.to_s3fs_credentials()
    assert s3_cred == {
        "key": "fake_access_key",
        "secret": "fake_secret_key",
        "token": "fake_session_token",
        "profile": None,
        "endpoint_url": None,
        "client_kwargs": {"region_name": region_name},
    }

    c = AwsCredentials()
    c.parse_native_representation(botocore.session.get_session())
    assert c.is_resolved()
    assert not c.is_partial()
    assert c.aws_access_key_id == "fake_access_key"

    with pytest.raises(InvalidBoto3Session):
        c.parse_native_representation("boto3")


def test_aws_credentials_from_boto3(environment: Dict[str, str]) -> None:
    try:
        import boto3
    except ModuleNotFoundError:
        pytest.skip("Cannot import boto3 - skipping test")

    set_aws_credentials_env(environment)

    session = boto3.Session()

    c = AwsCredentials.from_session(session)
    assert c.profile_name is None
    assert c.aws_access_key_id == "fake_access_key"
    assert c.region_name == session.region_name
    assert c.profile_name is None
    assert c.is_resolved()
    assert not c.is_partial()

    c = AwsCredentials()
    c.parse_native_representation(boto3.Session())
    assert c.is_resolved()
    assert not c.is_partial()
    assert c.aws_access_key_id == "fake_access_key"


def test_aws_credentials_from_unknown_object() -> None:
    with pytest.raises(InvalidBoto3Session):
        AwsCredentials().parse_native_representation(CredentialsConfiguration())


def test_aws_credentials_for_profile(environment: Dict[str, str]) -> None:
    import botocore.exceptions

    c = AwsCredentials()
    c.profile_name = "dlt-ci-user2"
    with pytest.raises(botocore.exceptions.ProfileNotFound):
        c = resolve_configuration(c)

    c = AwsCredentials()
    c.profile_name = "dlt-ci-user"
    try:
        c = resolve_configuration(c)
        assert digest128(c.aws_access_key_id) == "S3r3CtEf074HjqVeHKj/"
    except botocore.exceptions.ProfileNotFound:
        pytest.skip("This test requires dlt-ci-user aws profile to be present")


def test_aws_credentials_with_endpoint_url(environment: Dict[str, str]) -> None:
    set_aws_credentials_env(environment)
    environment["CREDENTIALS__ENDPOINT_URL"] = "https://123.r2.cloudflarestorage.com"

    config = resolve_configuration(AwsCredentials())

    assert config.endpoint_url == "https://123.r2.cloudflarestorage.com"

    assert config.to_s3fs_credentials() == {
        "key": "fake_access_key",
        "secret": "fake_secret_key",
        "token": "fake_session_token",
        "profile": None,
        "endpoint_url": "https://123.r2.cloudflarestorage.com",
        "client_kwargs": {"region_name": "eu-central-1"},
        "config_kwargs": {
            "request_checksum_calculation": "when_required",
            "response_checksum_validation": "when_required",
        },
    }


def test_explicit_filesystem_credentials() -> None:
    import dlt
    from dlt.destinations import filesystem

    # try filesystem which uses union of credentials that requires bucket_url to resolve
    p = dlt.pipeline(
        pipeline_name="postgres_pipeline",
        destination=filesystem(
            bucket_url="s3://test",
            destination_name="uniq_s3_bucket",
            credentials={"aws_access_key_id": "key_id", "aws_secret_access_key": "key"},
        ),
    )
    config = p.destination_client().config
    assert isinstance(config.credentials, AwsCredentials)
    assert config.credentials.is_resolved()


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


def set_aws_credentials_env(environment: Dict[str, str]) -> None:
    environment["AWS_ACCESS_KEY_ID"] = "fake_access_key"
    environment["AWS_SECRET_ACCESS_KEY"] = "fake_secret_key"
    environment["AWS_SESSION_TOKEN"] = "fake_session_token"
    environment["AWS_DEFAULT_REGION"] = environment["REGION_NAME"] = "eu-central-1"
