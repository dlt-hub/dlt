import base64
from tests.utils import skip_if_not_active

skip_if_not_active("snowflake")

import os
import pytest
from pathlib import Path
from unittest.mock import patch

from dlt.common.configuration.utils import add_config_to_env
from tests.utils import TEST_DICT_CONFIG_PROVIDER, TEST_STORAGE_ROOT, test_storage

from dlt.common.libs.sql_alchemy_compat import make_url
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.storages.file_storage import FileStorage
from dlt.common.utils import custom_environ, digest128

from dlt.destinations.impl.snowflake import utils as snowflake_utils
from dlt.destinations.impl.snowflake.configuration import (
    SNOWFLAKE_APPLICATION_ID,
    SnowflakeClientConfiguration,
    SnowflakeCredentials,
    SnowflakeCredentialsWithoutDefaults,
)
from dlt.destinations.impl.snowflake.utils import snowflake_session_token_available

from tests.common.configuration.utils import environment


# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential

# PEM key
PKEY_PEM_PATH = "./tests/common/cases/secrets/encrypted-private-key"
PKEY_PEM_STR = Path(PKEY_PEM_PATH).read_text("utf8")
# base64 PEM key
PKEY_PEM_BASE64_STR = base64.b64encode(PKEY_PEM_STR.encode(encoding="ascii")).decode(
    encoding="ascii"
)
# base64 encoded DER key
PKEY_DER_PATH = "./tests/common/cases/secrets/encrypted-private-key-base64"
PKEY_DER_STR = Path(PKEY_DER_PATH).read_text("utf8")

PKEY_PASSPHRASE = "12345"


def test_connection_string_with_all_params() -> None:
    url = "snowflake://user1:pass1@host1/db1?warehouse=warehouse1&role=role1&private_key=cGs%3D&private_key_passphrase=paphr&authenticator=oauth&token=TOK"

    creds = SnowflakeCredentialsWithoutDefaults()
    creds.parse_native_representation(url)
    assert not creds.is_resolved()

    assert creds.database == "db1"
    assert creds.username == "user1"
    assert creds.password == "pass1"
    assert creds.host == "host1"
    assert creds.warehouse == "warehouse1"
    assert creds.role == "role1"
    assert creds.private_key == "cGs="
    assert creds.private_key_passphrase == "paphr"
    assert creds.authenticator == "oauth"
    assert creds.token == "TOK"

    expected = make_url(url)
    to_url_value = str(creds.to_url())

    # Test URL components regardless of query param order
    assert make_url(creds.to_native_representation()) == expected
    assert to_url_value == str(expected)


def test_custom_application():
    creds = SnowflakeCredentialsWithoutDefaults()
    creds.application = "custom"
    url = "snowflake://user1:pass1@host1/db1?authenticator=oauth&warehouse=warehouse1&role=role1&private_key=cGs%3D&private_key_passphrase=paphr&token=TOK"
    creds.parse_native_representation(url)
    assert not creds.is_resolved()
    expected = make_url(url)
    to_url_value = str(creds.to_url())
    assert make_url(creds.to_native_representation()) == expected
    assert to_url_value == str(expected)
    assert "application=custom" not in str(expected)


def test_set_all_from_env(environment) -> None:
    url = "snowflake://user1:pass1@host1/db1?authenticator=oauth&warehouse=warehouse1&role=role1&private_key=cGs%3D&private_key_passphrase=paphr&token=TOK"
    c = SnowflakeCredentialsWithoutDefaults(url)
    add_config_to_env(c)
    # resolve from environments
    creds = resolve_configuration(SnowflakeCredentialsWithoutDefaults())
    assert creds.is_resolved()
    assert creds.database == "db1"
    assert creds.username == "user1"
    assert creds.password == "pass1"
    assert creds.host == "host1"
    assert creds.warehouse == "warehouse1"
    assert creds.role == "role1"
    assert creds.private_key == "cGs="
    assert creds.private_key_passphrase == "paphr"
    assert creds.authenticator == "oauth"
    assert creds.token == "TOK"


def test_conditionally_required_params() -> None:
    # if using `snowflake` auth, username and password are required
    snowflake_auth_urls = (
        # URLs that implicitly use `snowflake` auth
        "snowflake://host1/db1",  # missing username + password
        "snowflake://user1@host1/db1",  # missing password
        "snowflake://:pass1@host1/db1",  # missing username
        # URLs that explicitly use `snowflake` auth
        "snowflake://host1/db1?authenticator=snowflake",  # missing username + password
    )
    for url in snowflake_auth_urls:
        with pytest.raises(ConfigurationValueError):
            resolve_configuration(SnowflakeCredentialsWithoutDefaults(url))


def test_only_authenticator() -> None:
    c = resolve_configuration(
        SnowflakeCredentialsWithoutDefaults("snowflake://host1/db1?authenticator=uri")
    )
    assert c.authenticator == "uri"
    assert c.token is None
    # token not present
    assert c.to_connector_params() == {
        "authenticator": "uri",
        "user": None,
        "password": None,
        "account": "host1",
        "database": "db1",
        "application": "dltHub_dlt",
    }
    c = resolve_configuration(
        SnowflakeCredentialsWithoutDefaults("snowflake://host1/db1?authenticator=oauth&token=TOK")
    )
    assert c.to_connector_params() == {
        "authenticator": "oauth",
        "token": "TOK",
        "user": None,
        "password": None,
        "account": "host1",
        "database": "db1",
        "application": "dltHub_dlt",
    }


# def test_no_query(environment) -> None:
#     c = SnowflakeCredentialsWithoutDefaults("snowflake://user1:pass1@host1/db1")
#     assert str(c.to_url()) == "snowflake://user1:pass1@host1/db1"
#     print(c.to_url())


def test_query_additional_params() -> None:
    c = SnowflakeCredentialsWithoutDefaults("snowflake://user1:pass1@host1/db1?keep_alive=true")
    assert c.to_connector_params()["keep_alive"] == "true"

    # try a typed param
    with TEST_DICT_CONFIG_PROVIDER().values({"credentials": {"query": {"keep_alive": True}}}):
        c = SnowflakeCredentialsWithoutDefaults("snowflake://user1:pass1@host1/db1")
        print(c.__is_resolved__)
        assert c.is_resolved() is False
        c = resolve_configuration(c)
        assert c.to_connector_params()["keep_alive"] is True
        # serialize to str
        assert c.to_url().query["keep_alive"] == "True"


def test_overwrite_query_value_from_explicit() -> None:
    # value specified in the query is preserved over the value set in config
    c = SnowflakeCredentialsWithoutDefaults("snowflake://user1@host1/db1?authenticator=uri")
    c.authenticator = "oauth"
    assert c.to_url().query["authenticator"] == "oauth"
    assert c.to_connector_params()["authenticator"] == "oauth"


@pytest.mark.parametrize(
    "private_key",
    (PKEY_DER_STR, PKEY_PEM_BASE64_STR, PKEY_PEM_STR),
    ids=["PKEY_DER_STR", "PKEY_PEM_BASE64_STR", "PKEY_PEM_STR"],
)
def test_to_connector_params_private_key(private_key: str) -> None:
    creds = SnowflakeCredentialsWithoutDefaults()
    creds.private_key = private_key
    creds.private_key_passphrase = PKEY_PASSPHRASE
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
        application="dltHub_dlt",
    )


@pytest.mark.parametrize(
    "private_key_path",
    (PKEY_DER_PATH, PKEY_PEM_PATH),
    ids=["PKEY_DER_PATH", "PKEY_PEM_PATH"],
)
def test_to_connector_params_private_path(private_key_path: str) -> None:
    from urllib.parse import quote

    c = resolve_configuration(
        SnowflakeCredentialsWithoutDefaults(),
        explicit_value=f"snowflake://user1@host1/db1?warehouse=warehouse1&role=role1&private_key_path={quote(private_key_path)}&private_key_passphrase={quote(PKEY_PASSPHRASE)}",
    )
    assert c.is_resolved()
    assert c.private_key_path == private_key_path
    assert c.private_key_passphrase == PKEY_PASSPHRASE
    assert c.password is None

    conn_params = c.to_connector_params()
    assert isinstance(conn_params["private_key"], bytes)


def test_snowflake_application_id() -> None:
    creds = SnowflakeCredentialsWithoutDefaults()
    creds.private_key = PKEY_PEM_STR
    creds.private_key_passphrase = PKEY_PASSPHRASE
    creds.username = "user1"
    creds.database = "db1"
    creds.host = "host1"
    creds.warehouse = "warehouse1"
    creds.role = "role1"

    params = creds.to_connector_params()
    assert params["application"] == SNOWFLAKE_APPLICATION_ID

    # set application identifier and check it
    creds.application = "custom_app_id"
    params = creds.to_connector_params()
    assert params["application"] == "custom_app_id"


@pytest.mark.parametrize(
    "private_key",
    ("not base!!", "TWFu", PKEY_PEM_STR),
    ids=["not_base64", "not_DER", "wrong_pass"],
)
def test_mangled_private_keys(environment, private_key: str) -> None:
    from urllib.parse import quote

    c = resolve_configuration(
        SnowflakeCredentialsWithoutDefaults(),
        explicit_value=f"snowflake://user1@host1/db1?warehouse=warehouse1&role=role1&private_key={quote(private_key)}&private_key_passphrase=qwe",
    )
    assert c.is_resolved()
    assert c.private_key == private_key
    assert c.private_key_passphrase == "qwe"
    assert c.password is None

    with pytest.raises(ValueError):
        c.to_connector_params()


def test_snowflake_credentials_native_value(environment) -> None:
    with pytest.raises(ConfigurationValueError):
        resolve_configuration(
            SnowflakeCredentialsWithoutDefaults(),
            explicit_value="snowflake://user1@host1/db1?warehouse=warehouse1&role=role1",
        )
    # set password via env
    os.environ["CREDENTIALS__PASSWORD"] = "pass"
    os.environ["CREDENTIALS__APPLICATION"] = "dlt"
    c = resolve_configuration(
        SnowflakeCredentialsWithoutDefaults(),
        explicit_value="snowflake://user1@host1/db1?warehouse=warehouse1&role=role1",
    )
    assert c.is_resolved()
    assert c.password == "pass"
    assert c.application == "dlt"
    assert "application=dlt" not in str(c.to_url())
    # # but if password is specified - it is final
    c = resolve_configuration(
        SnowflakeCredentialsWithoutDefaults(),
        explicit_value="snowflake://user1:pass1@host1/db1?warehouse=warehouse1&role=role1",
    )
    assert c.is_resolved()
    assert c.password == "pass1"

    # check with application = "" it should not be in connection string
    os.environ["CREDENTIALS__APPLICATION"] = ""
    c = resolve_configuration(
        SnowflakeCredentialsWithoutDefaults(),
        explicit_value="snowflake://user1@host1/db1?warehouse=warehouse1&role=role1",
    )
    assert c.is_resolved()
    assert c.application == ""
    assert "application=" not in str(c.to_url())
    conn_params = c.to_connector_params()
    assert conn_params == {
        "warehouse": "warehouse1",
        "role": "role1",
        "user": "user1",
        "password": "pass",
        "account": "host1",
        "database": "db1",
    }


@pytest.mark.parametrize(
    "private_key",
    (PKEY_DER_STR, PKEY_PEM_BASE64_STR, PKEY_PEM_STR),
    ids=["PKEY_DER_STR", "PKEY_PEM_BASE64_STR", "PKEY_PEM_STR"],
)
def test_snowflake_credentials_via_query_str(environment, private_key: str) -> None:
    from urllib.parse import quote

    c = resolve_configuration(
        SnowflakeCredentialsWithoutDefaults(),
        explicit_value=f"snowflake://user1@host1/db1?warehouse=warehouse1&role=role1&private_key={quote(private_key)}&private_key_passphrase={quote(PKEY_PASSPHRASE)}",
    )
    assert c.is_resolved()
    assert c.private_key == private_key
    assert c.private_key_passphrase == PKEY_PASSPHRASE
    assert c.password is None

    conn_params = c.to_connector_params()
    assert isinstance(conn_params["private_key"], bytes)


@pytest.mark.parametrize(
    "private_key",
    (PKEY_DER_STR, PKEY_PEM_BASE64_STR, PKEY_PEM_STR),
    ids=["PKEY_DER_STR", "PKEY_PEM_BASE64_STR", "PKEY_PEM_STR"],
)
def test_snowflake_credentials_key_via_env(environment, private_key: str) -> None:
    # set PK via env
    os.environ["CREDENTIALS__PRIVATE_KEY"] = private_key
    os.environ["CREDENTIALS__PRIVATE_KEY_PASSPHRASE"] = PKEY_PASSPHRASE
    c = resolve_configuration(
        SnowflakeCredentialsWithoutDefaults(),
        explicit_value="snowflake://user1@host1/db1?warehouse=warehouse1&role=role1",
    )
    assert c.is_resolved()
    assert c.private_key == private_key
    assert c.private_key_passphrase == PKEY_PASSPHRASE
    assert c.password is None

    conn_params = c.to_connector_params()
    assert isinstance(conn_params["private_key"], bytes)


def test_snowflake_provided_oauth_token(test_storage: FileStorage) -> None:
    # create mocked token file
    token_file_name = "token"
    test_storage.save(token_file_name, "SNOW_TOK")

    # define context managers constants
    SNOWFLAKE_SESSION_TOKEN_PATH_ATTR = "SNOWFLAKE_SESSION_TOKEN_PATH"
    PATCHED_TOKEN_PATH = os.path.join(TEST_STORAGE_ROOT, token_file_name)
    SNOWFLAKE_ENV_VARS = {
        "SNOWFLAKE_ACCOUNT": "host1",
        "SNOWFLAKE_HOST": "host1.snowflakecomputing.com",
    }

    # URLs with `authenticator=oauth` and missing `host` or `token` should trigger attempt to use
    # Snowflake-provided OAuth token
    trigger_urls = (
        "snowflake:///db1?authenticator=oauth",  # both host and token missing
        "snowflake://host1/db1?authenticator=oauth",  # host present, but token missing
        "snowflake:///db1?authenticator=oauth&token=USER_TOK",  # token present, but host missing
    )

    # if Snowflake-provided env vars and token present, should resolve correctly
    with patch.object(snowflake_utils, SNOWFLAKE_SESSION_TOKEN_PATH_ATTR, PATCHED_TOKEN_PATH):
        with custom_environ(SNOWFLAKE_ENV_VARS):
            for url in trigger_urls:
                creds = SnowflakeCredentials(url)
                assert snowflake_session_token_available()
                creds = resolve_configuration(creds)
                assert creds.is_resolved()
                assert creds.host == "host1"
                assert creds.authenticator == "oauth"
                conn_params = creds.to_connector_params()
                assert conn_params["account"] == "host1"
                assert conn_params["host"] == "host1.snowflakecomputing.com"
                assert conn_params["database"] == "db1"
                assert conn_params["authenticator"] == "oauth"
                assert conn_params["token"] == "SNOW_TOK"
                assert conn_params["user"] is None
                assert conn_params["password"] is None

    # if Snowflake-provided token not present, should raise
    with custom_environ(SNOWFLAKE_ENV_VARS):
        for url in trigger_urls:
            creds = SnowflakeCredentials(url)
            assert not snowflake_session_token_available()
            with pytest.raises(ConfigurationValueError):
                creds = resolve_configuration(creds)

    # if Snowflake-provided env vars not present, should raise
    with patch.object(snowflake_utils, SNOWFLAKE_SESSION_TOKEN_PATH_ATTR, PATCHED_TOKEN_PATH):
        for url in trigger_urls:
            creds = SnowflakeCredentials(url)
            assert not snowflake_session_token_available()
            with pytest.raises(ConfigurationValueError):
                creds = resolve_configuration(creds)

    # URLs without `authenticator=oauth` or both `host` and `token` present should NOT trigger attempt
    # to use Snowflake-provided OAuth token
    not_trigger_urls = (
        "snowflake://host1/db1?authenticator=password",  # not oauth
        "snowflake://host1/db1?authenticator=oauth&token=USER_TOK",  # both host and token present
    )
    with patch.object(snowflake_utils, SNOWFLAKE_SESSION_TOKEN_PATH_ATTR, PATCHED_TOKEN_PATH):
        with custom_environ(SNOWFLAKE_ENV_VARS):
            for url in not_trigger_urls:
                creds = SnowflakeCredentials(url)
                assert snowflake_session_token_available()
                creds = resolve_configuration(creds)
                assert creds.is_resolved()
                assert creds.token != "SNOW_TOK"


def test_snowflake_configuration() -> None:
    # def empty fingerprint
    assert SnowflakeClientConfiguration().fingerprint() == ""
    # based on host
    c = resolve_configuration(
        SnowflakeCredentials(),
        explicit_value="snowflake://user1:pass@host1/db1?warehouse=warehouse1&role=role1",
    )
    assert SnowflakeClientConfiguration(credentials=c).fingerprint() == digest128("host1")
