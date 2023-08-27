from typing import Dict
from urllib.parse import parse_qs

import pytest

from dlt.common import pendulum
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.configuration import resolve_configuration, ConfigFieldMissingException
from dlt.common.configuration.specs import AzureCredentials
from tests.load.utils import ALL_FILESYSTEM_DRIVERS
from tests.common.configuration.utils import environment
from tests.utils import preserve_environ, autouse_test_storage


@pytest.mark.skipif('az' not in ALL_FILESYSTEM_DRIVERS, reason='az filesystem driver not configured')
def test_azure_credentials_from_account_key(environment: Dict[str, str]) -> None:
    environment['CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME'] = 'fake_account_name'
    environment['CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY'] = "QWERTYUIOPASDFGHJKLZXCVBNM1234567890"

    config = resolve_configuration(AzureCredentials())

    # Verify sas token is generated with correct permissions and expiry time
    sas_params = parse_qs(config.azure_storage_sas_token)

    permissions = set(sas_params['sp'][0])
    assert permissions == {'r', 'w', 'd', 'l', 'a', 'c'}

    exp = ensure_pendulum_datetime(sas_params['se'][0])
    assert exp > pendulum.now().add(hours=23)


@pytest.mark.skipif('az' not in ALL_FILESYSTEM_DRIVERS, reason='az filesystem driver not configured')
def test_create_azure_sas_token_with_permissions(environment: Dict[str, str]) -> None:
    environment['CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME'] = 'fake_account_name'
    environment['CREDENTIALS__AZURE_STORAGE_ACCOUNT_KEY'] = "QWERTYUIOPASDFGHJKLZXCVBNM1234567890"
    environment['CREDENTIALS__AZURE_SAS_TOKEN_PERMISSIONS'] = "rl"

    config = resolve_configuration(AzureCredentials())

    sas_params = parse_qs(config.azure_storage_sas_token)

    permissions = set(sas_params['sp'][0])
    assert permissions == {'r', 'l'}



@pytest.mark.skipif('az' not in ALL_FILESYSTEM_DRIVERS, reason='az filesystem driver not configured')
def test_azure_credentials_from_sas_token(environment: Dict[str, str]) -> None:
    environment['CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME'] = 'fake_account_name'
    environment['CREDENTIALS__AZURE_STORAGE_SAS_TOKEN'] = "sp=rwdlacx&se=2021-01-01T00:00:00Z&sv=2019-12-12&sr=c&sig=1234567890"

    config = resolve_configuration(AzureCredentials())

    assert config.azure_storage_sas_token == environment['CREDENTIALS__AZURE_STORAGE_SAS_TOKEN']
    assert config.azure_storage_account_name == environment['CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME']
    assert config.azure_storage_account_key is None

    assert config.to_adlfs_credentials() == {
        'account_name': environment['CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME'],
        'account_key': None,
        'sas_token': environment['CREDENTIALS__AZURE_STORAGE_SAS_TOKEN'],
    }


@pytest.mark.skipif('az' not in ALL_FILESYSTEM_DRIVERS, reason='az filesystem driver not configured')
def test_azure_credentials_missing_account_name(environment: Dict[str, str]) -> None:
    with pytest.raises(ConfigFieldMissingException) as excinfo:
        resolve_configuration(AzureCredentials())

    ex = excinfo.value

    assert 'azure_storage_account_name' in ex.fields


@pytest.mark.skipif('az' not in ALL_FILESYSTEM_DRIVERS, reason='az filesystem driver not configured')
def test_azure_credentials_from_default(environment: Dict[str, str]) -> None:
    environment['CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME'] = 'fake_account_name'

    config = resolve_configuration(AzureCredentials())

    assert config.azure_storage_account_name == environment['CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME']
    assert config.azure_storage_account_key is None
    assert config.azure_storage_sas_token is None

    # fsspec args should have anon=True when using system credentials
    assert config.to_adlfs_credentials() == {
        'account_name': environment['CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME'],
        'account_key': None,
        'sas_token': None,
        'anon': False
    }
