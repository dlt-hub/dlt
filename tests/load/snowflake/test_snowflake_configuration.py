import base64
from tests.utils import skip_if_not_active

skip_if_not_active("snowflake")

import os
import sys
from pathlib import Path
from typing import Any, Callable, Optional
from unittest.mock import patch

import pytest

from dlt.common.configuration.utils import add_config_to_env
from tests.utils import TEST_DICT_CONFIG_PROVIDER, get_test_storage_root, test_storage

from dlt.common.libs.sql_alchemy_compat import make_url
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.exceptions import (
    ConfigFieldMissingException,
    ConfigurationValueError,
)
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import DictStrAny
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
        "timezone": "UTC",
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
        "timezone": "UTC",
        "authenticator": "oauth",
        "token": "TOK",
        "user": None,
        "password": None,
        "account": "host1",
        "database": "db1",
        "application": "dltHub_dlt",
    }


def test_workload_identity_provider() -> None:
    c = resolve_configuration(
        SnowflakeCredentialsWithoutDefaults(
            "snowflake://host1/db1?authenticator=WORKLOAD_IDENTITY"
            "&workload_identity_provider=AWS&warehouse=wh1"
        )
    )
    assert c.authenticator == "WORKLOAD_IDENTITY"
    assert c.workload_identity_provider == "AWS"
    assert c.warehouse == "wh1"
    assert c.to_connector_params() == {
        "timezone": "UTC",
        "authenticator": "WORKLOAD_IDENTITY",
        "workload_identity_provider": "AWS",
        "warehouse": "wh1",
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


def test_session_timezone() -> None:
    # UTC is pinned so the offset `TIMESTAMP_LTZ` renders does not follow the account
    c = SnowflakeCredentialsWithoutDefaults("snowflake://user1:pass1@host1/db1")
    assert c.to_connector_params()["timezone"] == "UTC"

    c.session_timezone = "Europe/Berlin"
    assert c.to_connector_params()["timezone"] == "Europe/Berlin"

    # `None` hands the session back to the account default
    c.session_timezone = None
    assert "timezone" not in c.to_connector_params()

    # a timezone passed in the query takes precedence
    c = SnowflakeCredentialsWithoutDefaults("snowflake://user1:pass1@host1/db1?timezone=Asia/Tokyo")
    assert c.to_connector_params()["timezone"] == "Asia/Tokyo"


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
        timezone="UTC",
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
        "timezone": "UTC",
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
    PATCHED_TOKEN_PATH = os.path.join(get_test_storage_root(), token_file_name)
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


SNOWFLAKE_CONNECTIONS_TOML = f"""
[dev]
account = "acc1"
user = "user1"
password = "pass1"
database = "db1"
warehouse = "warehouse1"
role = "role1"
client_session_keep_alive = true

[pkey]
account = "acc1"
user = "user1"
authenticator = "SNOWFLAKE_JWT"
private_key_file = "{PKEY_PEM_PATH}"
private_key_file_pwd = "{PKEY_PASSPHRASE}"

[no_account]
user = "user1"
password = "pass1"

[host_only]
host = "acc9.snowflakecomputing.com"
"""

DEFAULT_CONNECTION_TOML = """
[default]
account = "acc1"
user = "user1"
password = "pass1"
"""

CONNECTION_PARAMS = {
    "connection_name": "dev",
    "application": SNOWFLAKE_APPLICATION_ID,
    "timezone": "UTC",
}

TSnowflakeHome = Callable[..., Path]


@pytest.mark.parametrize(
    "explicit_value",
    [
        pytest.param({"connection_name": "dev"}, id="name_only"),
        pytest.param("snowflake:///?connection_name=dev", id="connection_string"),
        pytest.param(None, id="env"),
    ],
)
def test_snowflake_connector_loads_connection(
    snowflake_home: TSnowflakeHome, environment: Any, explicit_value: Any
) -> None:
    snowflake_home(SNOWFLAKE_CONNECTIONS_TOML)
    if explicit_value is None:
        environment["CREDENTIALS__CONNECTION_NAME"] = "dev"
    c = resolve_configuration(SnowflakeCredentials(), explicit_value=explicit_value)
    assert c.is_resolved()
    assert c.has_default_credentials()
    assert c.default_credentials() == "dev"
    # dlt loads the connection parameters for tools that cannot use `connections.toml`
    assert c.host == "acc1"
    assert c.database == "db1"
    assert c.username == "user1"
    assert c.password == "pass1"
    assert c.warehouse == "warehouse1"
    assert dict(c)["password"] == "pass1"
    # but the connector loads the connection itself
    assert c.to_connector_params() == {
        **CONNECTION_PARAMS,
        "database": "db1",
        "warehouse": "warehouse1",
        "role": "role1",
    }
    assert str(c) == "snowflake://user1:***@acc1/db1"

    # the connection name and the loaded values survive the round trip
    native = c.to_native_representation()
    assert "connection_name=dev" in native
    parsed = SnowflakeCredentials(native)
    assert parsed.connection_name == "dev"
    assert parsed.database == "db1"


@pytest.mark.parametrize(
    "explicit_value,expected",
    [
        pytest.param(
            {"connection_name": "dev", "database": "db2", "warehouse": "wh2", "role": "r2"},
            {**CONNECTION_PARAMS, "database": "db2", "warehouse": "wh2", "role": "r2"},
            id="dict_overrides",
        ),
        pytest.param(
            "snowflake:///db2?connection_name=dev&warehouse=wh2&role=r2",
            {**CONNECTION_PARAMS, "database": "db2", "warehouse": "wh2", "role": "r2"},
            id="url_overrides",
        ),
        pytest.param(
            {"connection_name": "dev", "query": {"client_session_keep_alive": True}},
            {
                **CONNECTION_PARAMS,
                "client_session_keep_alive": True,
                "database": "db1",
                "warehouse": "warehouse1",
                "role": "role1",
            },
            id="query_extras",
        ),
    ],
)
def test_snowflake_connection_overrides(
    snowflake_home: TSnowflakeHome, explicit_value: Any, expected: DictStrAny
) -> None:
    snowflake_home(SNOWFLAKE_CONNECTIONS_TOML)
    c = resolve_configuration(SnowflakeCredentials(), explicit_value=explicit_value)
    assert c.to_connector_params() == expected

    # connector 3.x and 4.x blank connection values with `None` or empty params
    c.application = ""
    c.session_timezone = None
    assert all(v not in (None, "") for v in c.to_connector_params().values())
    assert c.to_connector_params().keys() == expected.keys() - {"application", "timezone"}


@pytest.mark.parametrize(
    "field,value",
    [
        pytest.param("host", "acc2", id="host"),
        pytest.param("username", "user2", id="username"),
        pytest.param("password", "pass2", id="password"),
        pytest.param("private_key", PKEY_DER_STR, id="private_key"),
        pytest.param("private_key_path", PKEY_PEM_PATH, id="private_key_path"),
        pytest.param("private_key_passphrase", PKEY_PASSPHRASE, id="private_key_passphrase"),
        pytest.param("token", "TOK", id="token"),
        pytest.param("authenticator", "oauth", id="authenticator"),
        pytest.param("workload_identity_provider", "AWS", id="workload_identity_provider"),
    ],
)
def test_snowflake_connection_rejects_credentials(
    snowflake_home: TSnowflakeHome, field: str, value: str
) -> None:
    snowflake_home(SNOWFLAKE_CONNECTIONS_TOML)
    with pytest.raises(ConfigurationValueError) as py_ex:
        resolve_configuration(
            SnowflakeCredentials(), explicit_value={"connection_name": "dev", field: value}
        )
    assert field in str(py_ex.value)
    assert "`dev`" in str(py_ex.value)


def test_snowflake_connection_loads_private_key(snowflake_home: TSnowflakeHome) -> None:
    snowflake_home(SNOWFLAKE_CONNECTIONS_TOML)
    c = resolve_configuration(SnowflakeCredentials(), explicit_value={"connection_name": "pkey"})
    assert c.private_key_path == PKEY_PEM_PATH
    assert c.private_key == PKEY_PEM_STR
    assert c.private_key_passphrase == PKEY_PASSPHRASE
    assert c.authenticator == "SNOWFLAKE_JWT"
    # dlt does not send the key because the connector reads the key file of the connection
    conn_params = c.to_connector_params()
    assert conn_params.keys() == {"connection_name", "application", "timezone"}


def test_snowflake_connection_loads_token_file(snowflake_home: TSnowflakeHome) -> None:
    home = snowflake_home("")
    token_path = home / "token"
    token_path.write_text("FILE_TOK\n", encoding="utf-8")
    snowflake_home(
        '[spcs]\naccount = "acc1"\nauthenticator = "oauth"\ntoken_file_path ='
        f' "{token_path.as_posix()}"\n'
    )
    c = resolve_configuration(SnowflakeCredentials(), explicit_value={"connection_name": "spcs"})
    assert c.token == "FILE_TOK"
    assert c.authenticator == "oauth"
    assert "token" not in c.to_connector_params()


def test_snowflake_connection_rejects_url_host(snowflake_home: TSnowflakeHome) -> None:
    snowflake_home(SNOWFLAKE_CONNECTIONS_TOML)
    with pytest.raises(ConfigurationValueError, match="host"):
        resolve_configuration(
            SnowflakeCredentials(), explicit_value="snowflake://acc2/db2?connection_name=dev"
        )
    # credentials without defaults validate the same way
    with pytest.raises(ConfigurationValueError, match="host"):
        SnowflakeCredentialsWithoutDefaults("snowflake://acc2/db2?connection_name=dev").resolve()


def test_snowflake_connection_unknown_name(snowflake_home: TSnowflakeHome) -> None:
    snowflake_home(SNOWFLAKE_CONNECTIONS_TOML)
    with pytest.raises(ConfigurationValueError) as py_ex:
        resolve_configuration(SnowflakeCredentials(), explicit_value={"connection_name": "prod"})
    assert "`prod`" in str(py_ex.value)
    for name in ("dev", "pkey", "no_account", "host_only"):
        assert name in str(py_ex.value)


def test_snowflake_connection_missing_connector(snowflake_home: TSnowflakeHome) -> None:
    snowflake_home(SNOWFLAKE_CONNECTIONS_TOML)
    with patch.dict(sys.modules, {"snowflake.connector.config_manager": None}):
        with pytest.raises(MissingDependencyException):
            resolve_configuration(SnowflakeCredentials(), explicit_value={"connection_name": "dev"})
        # dlt does not probe the default connection without the connector
        with pytest.raises(ConfigFieldMissingException):
            resolve_configuration(SnowflakeCredentials())


@pytest.mark.parametrize(
    "config_toml,env_name,expected_name",
    [
        pytest.param('default_connection_name = "dev"', None, "dev", id="config_toml"),
        pytest.param('default_connection_name = "dev"', "pkey", "pkey", id="env_name"),
        pytest.param("", None, "default", id="default_section"),
    ],
)
def test_snowflake_default_connection(
    snowflake_home: TSnowflakeHome, config_toml: str, env_name: Optional[str], expected_name: str
) -> None:
    snowflake_home(SNOWFLAKE_CONNECTIONS_TOML + DEFAULT_CONNECTION_TOML, config_toml)
    env = {"SNOWFLAKE_DEFAULT_CONNECTION_NAME": env_name} if env_name else {}
    with custom_environ(env):
        c = resolve_configuration(SnowflakeCredentials())
        assert c.connection_name == expected_name
        assert c.has_default_credentials()
        assert c.to_connector_params()["connection_name"] == expected_name
        assert SnowflakeClientConfiguration(credentials=c).fingerprint() == digest128("acc1")

        # dlt keeps the session parameters as overrides
        c = resolve_configuration(SnowflakeCredentials(), explicit_value={"database": "db2"})
        assert c.connection_name == expected_name
        assert c.to_connector_params()["database"] == "db2"


def test_snowflake_default_connection_not_used(snowflake_home: TSnowflakeHome) -> None:
    # no connections at all
    with pytest.raises(ConfigFieldMissingException) as missing_ex:
        resolve_configuration(SnowflakeCredentials())
    assert set(missing_ex.value.fields) == {"host", "database"}

    # default name points to a connection that is not defined
    snowflake_home(SNOWFLAKE_CONNECTIONS_TOML, 'default_connection_name = "prod"')
    with pytest.raises(ConfigFieldMissingException):
        resolve_configuration(SnowflakeCredentials())

    # any explicit credential disables the default connection
    snowflake_home(SNOWFLAKE_CONNECTIONS_TOML, 'default_connection_name = "dev"')
    with pytest.raises(ConfigFieldMissingException):
        resolve_configuration(SnowflakeCredentials(), explicit_value={"username": "user2"})
    with pytest.raises(ConfigurationValueError, match="username"):
        resolve_configuration(SnowflakeCredentials(), explicit_value="snowflake://acc2/db2")


def test_snowflake_container_connection(
    snowflake_home: TSnowflakeHome, test_storage: FileStorage
) -> None:
    """dlt uses the connection that a container mounts like any other default connection."""
    test_storage.save("token", "SNOW_TOK")
    token_path = os.path.join(get_test_storage_root(), "token")
    spcs_env = {"SNOWFLAKE_ACCOUNT": "host1", "SNOWFLAKE_HOST": "host1.snowflakecomputing.com"}
    with patch.object(snowflake_utils, "SNOWFLAKE_SESSION_TOKEN_PATH", token_path):
        with custom_environ(spcs_env):
            # the session token alone is not a default
            with pytest.raises(ConfigFieldMissingException):
                resolve_configuration(SnowflakeCredentials())

            snowflake_home(SNOWFLAKE_CONNECTIONS_TOML + DEFAULT_CONNECTION_TOML)
            c = resolve_configuration(SnowflakeCredentials())
            assert c.connection_name == "default"
            assert c.has_default_credentials()
            assert "token" not in c.to_connector_params()

            # explicit oauth still uses the session token next to a mounted connection
            c = resolve_configuration(
                SnowflakeCredentials(), explicit_value="snowflake:///db1?authenticator=oauth"
            )
            assert not c.has_default_credentials()
            conn_params = c.to_connector_params()
            assert conn_params["account"] == "host1"
            assert conn_params["token"] == "SNOW_TOK"
            assert "connection_name" not in conn_params


@pytest.mark.parametrize(
    "name,identity,display",
    [
        pytest.param("dev", "acc1", "snowflake://user1:***@acc1/db1", id="account"),
        # a host is not an account, so it does not display
        pytest.param("host_only", "acc9.snowflakecomputing.com", "snowflake://", id="host_only"),
        pytest.param("no_account", None, "snowflake://user1:***@", id="no_account"),
    ],
)
def test_snowflake_connection_identity(
    snowflake_home: TSnowflakeHome, name: str, identity: Optional[str], display: str
) -> None:
    snowflake_home(SNOWFLAKE_CONNECTIONS_TOML)
    config = SnowflakeClientConfiguration(
        credentials=resolve_configuration(
            SnowflakeCredentials(), explicit_value={"connection_name": name}
        )
    )
    explicit = SnowflakeClientConfiguration(
        credentials=resolve_configuration(
            SnowflakeCredentials(), explicit_value="snowflake://user1:pass1@acc1/db1"
        )
    )
    assert str(config.credentials) == display
    if identity:
        assert config.fingerprint() == digest128(identity)
        assert config.data_location() == identity
        # explicit host and a connection with the same account are the same location
        assert config.is_same_location(explicit) == (identity == "acc1")
    else:
        assert config.fingerprint() == ""
        with pytest.raises(ConfigurationValueError, match="data location"):
            config.data_location()


def test_snowflake_connection_without_defaults_stays_partial(
    snowflake_home: TSnowflakeHome,
) -> None:
    snowflake_home(SNOWFLAKE_CONNECTIONS_TOML)
    with pytest.raises(ConfigFieldMissingException):
        resolve_configuration(
            SnowflakeCredentialsWithoutDefaults(), explicit_value={"connection_name": "dev"}
        )
    # credentials without defaults still build the connector parameters when used directly
    c = SnowflakeCredentialsWithoutDefaults()
    c.connection_name = "dev"
    assert c.to_connector_params() == CONNECTION_PARAMS


@pytest.mark.parametrize(
    "connection_string,expected_fingerprint",
    [
        pytest.param("", "", id="empty"),
        pytest.param(
            "snowflake://user1:pass@host1/db1?warehouse=warehouse1&role=role1",
            digest128("host1"),
            id="legacy_host_only",
        ),
    ],
)
def test_snowflake_fingerprint(connection_string: str, expected_fingerprint: str) -> None:
    credentials: Optional[SnowflakeCredentials]
    if connection_string:
        credentials = resolve_configuration(
            SnowflakeCredentials(),
            explicit_value=connection_string,
        )
    else:
        credentials = None

    config = SnowflakeClientConfiguration(credentials=credentials)

    assert config.fingerprint() == expected_fingerprint
