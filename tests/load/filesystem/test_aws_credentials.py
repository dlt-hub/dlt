import pytest
from typing import Dict

from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs.aws_credentials import AwsCredentials
from dlt.common.configuration.specs.exceptions import InvalidBoto3Session
from tests.common.configuration.utils import environment

from tests.load.utils import ALL_FILESYSTEM_DRIVERS

@pytest.mark.skipif('s3' not in ALL_FILESYSTEM_DRIVERS, reason='s3 filesystem driver not configured')
def test_aws_credentials_resolved_from_default(environment: Dict[str, str]) -> None:
    environment['AWS_ACCESS_KEY_ID'] = 'fake_access_key'
    environment['AWS_SECRET_ACCESS_KEY'] = 'fake_secret_key'
    environment['AWS_SESSION_TOKEN'] = 'fake_session_token'

    config = resolve_configuration(AwsCredentials())

    assert config.aws_access_key_id == 'fake_access_key'
    assert config.aws_secret_access_key == 'fake_secret_key'
    assert config.aws_session_token == 'fake_session_token'
    assert config.profile_name == 'default'

    # use profile name other than default
    import botocore

    environment["CREDENTIALS__PROFILE_NAME"] = "fake_profile"
    with pytest.raises(botocore.exceptions.ProfileNotFound):
        resolve_configuration(AwsCredentials())

    environment["CREDENTIALS__PROFILE_NAME"] = "default"
    config = resolve_configuration(AwsCredentials())
    assert config.profile_name == "default"


@pytest.mark.skipif('s3' not in ALL_FILESYSTEM_DRIVERS, reason='s3 filesystem driver not configured')
def test_aws_credentials_from_boto3(environment: Dict[str, str]) -> None:
    environment['AWS_ACCESS_KEY_ID'] = 'fake_access_key'
    environment['AWS_SECRET_ACCESS_KEY'] = 'fake_secret_key'
    environment['AWS_SESSION_TOKEN'] = 'fake_session_token'

    import boto3

    session = boto3.Session()

    c = AwsCredentials(session)
    assert c.profile_name == "default"
    assert c.aws_access_key_id == "fake_access_key"
    assert c.region_name == session.region_name
    assert c.profile_name == session.profile_name
    assert c.is_resolved()
    assert not c.is_partial()

    c = AwsCredentials()
    c.parse_native_representation(boto3.Session())
    assert c.is_resolved()
    assert not c.is_partial()
    assert c.aws_access_key_id == "fake_access_key"

    with pytest.raises(InvalidBoto3Session):
        c.parse_native_representation("boto3")
