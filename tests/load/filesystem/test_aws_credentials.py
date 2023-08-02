import pytest
from typing import Any, Dict

from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs.aws_credentials import AwsCredentials
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
    assert config.aws_profile == 'default'
