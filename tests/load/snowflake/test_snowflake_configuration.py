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
