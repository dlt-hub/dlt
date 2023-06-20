from pathlib import Path

from sqlalchemy.engine import URL, make_url

from dlt.destinations.snowflake.configuration import SnowflakeClientConfiguration, SnowflakeCredentials


def test_connection_string_with_all_params() -> None:
    url = "snowflake://user1:pass1@host1/db1?warehouse=warehouse1&role=role1"

    creds = SnowflakeCredentials()
    creds.parse_native_representation(
        "snowflake://user1:pass1@host1/db1?warehouse=warehouse1&role=role1"
    )

    assert creds.database == "db1"
    assert creds.username == "user1"
    assert creds.password == "pass1"
    assert creds.host == "host1"
    assert creds.warehouse == "warehouse1"
    assert creds.role == "role1"

    expected = make_url(url)

    # Test URL components regardless of query param order
    assert make_url(creds.to_native_representation()) == expected


def test_to_connector_params() -> None:
    pkey_str = Path('./tests/common/cases/secrets/encrypted-private-key').read_text('utf8')

    creds = SnowflakeCredentials()
    creds.private_key = pkey_str  # type: ignore[assignment]
    creds.private_key_passphrase = '12345'  # type: ignore[assignment]
    creds.username = 'user1'
    creds.database = 'db1'
    creds.host = 'host1'
    creds.warehouse = 'warehouse1'
    creds.role = 'role1'

    params = creds.to_connector_params()

    assert isinstance(params['private_key'], bytes)
    params.pop('private_key')

    assert params == dict(
        user='user1',
        database='db1',
        account='host1',
        password=None,
        warehouse='warehouse1',
        role='role1',
    )
