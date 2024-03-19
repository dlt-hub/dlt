import os
from unittest.mock import patch
import pytest
from pathlib import Path
from dlt.common.libs.sql_alchemy import make_url
from dlt.common.storages.file_storage import FileStorage

pytest.importorskip("snowflake")

from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.exceptions import ConfigFieldMissingException, ConfigurationValueError
from dlt.common.utils import digest128

from dlt.destinations.impl.snowflake.configuration import (
    SnowflakeClientConfiguration,
    SnowflakeCredentials,
)

from tests.utils import autouse_test_storage, TEST_STORAGE_ROOT, test_storage
from tests.common.configuration.utils import environment


def test_connection_string_with_all_params() -> None:
    url = "snowflake://user1:pass1@host1/db1?warehouse=warehouse1&role=role1&private_key=cGs%3D&private_key_passphrase=paphr"

    creds = SnowflakeCredentials()
    creds.parse_native_representation(url)

    assert creds.database == "db1"
    assert creds.username == "user1"
    assert creds.password == "pass1"
    assert creds.host == "host1"
    assert creds.warehouse == "warehouse1"
    assert creds.role == "role1"
    assert creds.private_key == "cGs="
    assert creds.private_key_passphrase == "paphr"

    expected = make_url(url)

    # Test URL components regardless of query param order
    assert make_url(creds.to_native_representation()) == expected


def test_setting_authenticator() -> None:
    # no password and user are allowed
    url = "snowflake://host1/db1?authenticator=oauth&role=role1&token=A&warehouse=warehouse1"
    creds = SnowflakeCredentials()
    creds.parse_native_representation(url)
    assert creds.authenticator == "oauth"
    assert creds.token == "A"
    assert creds.to_native_representation() == url

    params = creds.to_connector_params()
    assert params["authenticator"] == "oauth"
    assert params["token"] == "A"

    # change token
    creds.token = "B"  # type: ignore[assignment]
    params = creds.to_connector_params()
    assert params["token"] == "B"


def test_connection_value_errors() -> None:
    # user must be present when password/key are present
    url = "snowflake://:pass1@host1/db1?warehouse=warehouse1&role=role1&private_key=cGs%3D&private_key_passphrase=paphr"
    creds = SnowflakeCredentials()
    with pytest.raises(ConfigurationValueError) as c_ex:
        creds.parse_native_representation(url)
    assert "user" in str(c_ex)

    # password / key / token must be present
    url = "snowflake://user@host1/db1?warehouse=warehouse1&role=role1"
    creds = SnowflakeCredentials()
    creds.parse_native_representation(url)
    # this is partial
    assert creds.is_partial()


def test_to_connector_params() -> None:
    # PEM key
    pkey_str = Path("./tests/common/cases/secrets/encrypted-private-key").read_text("utf8")

    creds = SnowflakeCredentials()
    creds.private_key = pkey_str  # type: ignore[assignment]
    creds.private_key_passphrase = "12345"  # type: ignore[assignment]
    creds.username = "user1"
    creds.database = "db1"
    creds.host = "host1"
    creds.warehouse = "warehouse1"
    creds.role = "role1"

    params = creds.to_connector_params()

    assert isinstance(params["private_key"], bytes)
    params.pop("private_key")

    assert params == dict(
        user="user1",
        database="db1",
        account="host1",
        password=None,
        warehouse="warehouse1",
        role="role1",
    )

    # base64 encoded DER key
    pkey_str = Path("./tests/common/cases/secrets/encrypted-private-key-base64").read_text("utf8")

    creds = SnowflakeCredentials()
    creds.private_key = pkey_str  # type: ignore[assignment]
    creds.private_key_passphrase = "12345"  # type: ignore[assignment]
    creds.username = "user1"
    creds.database = "db1"
    creds.host = "host1"
    creds.warehouse = "warehouse1"
    creds.role = "role1"

    params = creds.to_connector_params()

    assert isinstance(params["private_key"], bytes)
    params.pop("private_key")

    assert params == dict(
        user="user1",
        database="db1",
        account="host1",
        password=None,
        warehouse="warehouse1",
        role="role1",
    )


def test_snowflake_credentials_native_value(environment) -> None:
    with pytest.raises(ConfigurationValueError):
        resolve_configuration(
            SnowflakeCredentials(),
            explicit_value="snowflake://user1@host1/db1?warehouse=warehouse1&role=role1",
        )
    # set password via env
    os.environ["CREDENTIALS__PASSWORD"] = "pass"
    c = resolve_configuration(
        SnowflakeCredentials(),
        explicit_value="snowflake://user1@host1/db1?warehouse=warehouse1&role=role1",
    )
    assert c.is_resolved()
    assert c.password == "pass"
    # # but if password is specified - it is final
    c = resolve_configuration(
        SnowflakeCredentials(),
        explicit_value="snowflake://user1:pass1@host1/db1?warehouse=warehouse1&role=role1",
    )
    assert c.is_resolved()
    assert c.password == "pass1"

    # set PK via env
    del os.environ["CREDENTIALS__PASSWORD"]
    os.environ["CREDENTIALS__PRIVATE_KEY"] = "pk"
    c = resolve_configuration(
        SnowflakeCredentials(),
        explicit_value="snowflake://user1@host1/db1?warehouse=warehouse1&role=role1",
    )
    assert c.is_resolved()
    assert c.private_key == "pk"


def test_snowflake_default_credentials(environment, test_storage: FileStorage) -> None:
    with pytest.raises(ConfigFieldMissingException):
        resolve_configuration(SnowflakeCredentials(), explicit_value="snowflake:///db1")

    token_path = os.path.join(TEST_STORAGE_ROOT, "token")
    test_storage.save("token", "TOK1")

    with patch.object(SnowflakeCredentials, "LOGIN_TOKEN_PATH", token_path):
        with pytest.raises(ConfigFieldMissingException) as mi_ex:
            resolve_configuration(SnowflakeCredentials(), explicit_value="snowflake:///db1")
        # only host missing
        assert mi_ex.value.fields == ["host"]

        # set missing env
        os.environ["SNOWFLAKE_ACCOUNT"] = "accnt"
        os.environ["SNOWFLAKE_HOST"] = "hostname"
        c = resolve_configuration(SnowflakeCredentials(), explicit_value={"database": "db1"})
        assert c.host == "accnt"
        assert c._hostname == "hostname"
        assert c.database == "db1"
        assert c.token == "TOK1"
        assert c.authenticator == "oauth"
        assert not c.is_partial()
        assert c.has_default_credentials()

        # check conn url
        params = c.to_connector_params()
        # no password, user etc. are set
        assert params == {
            "user": None,
            "password": None,
            "account": "accnt",
            "database": "db1",
            "warehouse": None,
            "role": None,
            "private_key": None,
            "authenticator": "oauth",
            "token": "TOK1",
            "host": "hostname",
        }

        # change token file
        test_storage.save("token", "TOK2")
        params = c.to_connector_params()
        # token updated
        assert params["token"] == "TOK2"


def test_snowflake_configuration() -> None:
    # def empty fingerprint
    assert SnowflakeClientConfiguration().fingerprint() == ""
    # based on host
    c = resolve_configuration(
        SnowflakeCredentials(),
        explicit_value="snowflake://user1:pass@host1/db1?warehouse=warehouse1&role=role1",
    )
    assert SnowflakeClientConfiguration(credentials=c).fingerprint() == digest128("host1")
