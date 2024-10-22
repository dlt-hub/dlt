import itertools
import os
import pytest
from typing import Optional, Union, Any

import dlt
from dlt.common.configuration.exceptions import InvalidNativeValue, ConfigFieldMissingException
from dlt.common.configuration.providers import EnvironProvider
from dlt.common.configuration.specs import CredentialsConfiguration, BaseConfiguration
from dlt.common.configuration import configspec, resolve_configuration
from dlt.common.configuration.specs.gcp_credentials import GcpServiceAccountCredentials
from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.specs.connection_string_credentials import ConnectionStringCredentials
from dlt.common.configuration.resolve import initialize_credentials
from dlt.common.configuration.specs.exceptions import NativeValueError

from tests.common.configuration.utils import environment
from tests.utils import preserve_environ


@configspec
class ZenCredentials(CredentialsConfiguration):
    def auth(self):
        pass


@configspec
class ZenEmailCredentials(ZenCredentials):
    email: str = None
    password: TSecretStrValue = None

    def parse_native_representation(self, native_value: Any) -> None:
        assert isinstance(native_value, str)
        if native_value.startswith("email:"):
            parts = native_value.split(":")
            self.email = parts[-2]
            self.password = parts[-1]
        else:
            raise NativeValueError(self.__class__, native_value, "invalid email NV")

    def auth(self):
        return "email-cookie"


@configspec
class ZenApiKeyCredentials(ZenCredentials):
    api_key: str = None
    api_secret: TSecretStrValue = None

    def parse_native_representation(self, native_value: Any) -> None:
        assert isinstance(native_value, str)
        if native_value.startswith("secret:"):
            parts = native_value.split(":")
            self.api_key = parts[-2]
            self.api_secret = parts[-1]
        else:
            raise NativeValueError(self.__class__, native_value, "invalid secret NV")

    def auth(self):
        return "api-cookie"


@configspec
class ZenConfig(BaseConfiguration):
    credentials: Union[ZenApiKeyCredentials, ZenEmailCredentials] = None
    some_option: bool = False


@configspec
class ZenConfigOptCredentials:
    # add none to union to make it optional
    credentials: Union[ZenApiKeyCredentials, ZenEmailCredentials, None] = None
    some_option: bool = False


def test_resolve_union() -> None:
    # provide values to first spec in union
    os.environ["CREDENTIALS__API_KEY"] = "api key"
    os.environ["CREDENTIALS__API_SECRET"] = "api secret"
    c = resolve_configuration(ZenConfig())
    # right credentials instance was created
    assert isinstance(c.credentials, ZenApiKeyCredentials)
    assert c.credentials.api_key == "api key"
    assert c.credentials.api_secret == "api secret"

    # if values for both spec in union exist, first one is returned
    os.environ["CREDENTIALS__EMAIL"] = "email"
    os.environ["CREDENTIALS__PASSWORD"] = "password"
    c = resolve_configuration(ZenConfig())
    assert isinstance(c.credentials, ZenApiKeyCredentials)

    # drop values for first spec
    del os.environ["CREDENTIALS__API_KEY"]
    del os.environ["CREDENTIALS__API_SECRET"]
    c = resolve_configuration(ZenConfig())
    assert isinstance(c.credentials, ZenEmailCredentials)
    assert c.credentials.email == "email"
    assert c.credentials.password == "password"

    # allow partial
    c = resolve_configuration(ZenConfig(), accept_partial=True)
    assert isinstance(c.credentials, ZenApiKeyCredentials)
    assert c.is_partial
    assert c.is_resolved
    assert c.credentials.is_partial
    assert c.credentials.api_secret is None


def test_resolve_optional_union() -> None:
    c = resolve_configuration(ZenConfigOptCredentials())  # type: ignore[type-var]
    assert c.is_partial  # type: ignore[attr-defined]
    # assert c.is
    assert c.credentials is None

    # if we provide values for second union, it will be tried and resolved
    os.environ["CREDENTIALS__EMAIL"] = "email"
    os.environ["CREDENTIALS__PASSWORD"] = "password"
    c = resolve_configuration(ZenConfigOptCredentials())  # type: ignore[type-var]
    assert isinstance(c.credentials, ZenEmailCredentials)


def test_resolve_union_from_native_value() -> None:
    # this is native value for e-mail auth, the api key raises on it
    os.environ["CREDENTIALS"] = "email:mx:pwd"
    c = resolve_configuration(ZenConfig())
    assert isinstance(c.credentials, ZenEmailCredentials)
    assert c.credentials.email == "mx"
    assert c.credentials.password == "pwd"

    # api secret resolves first
    os.environ["CREDENTIALS"] = "secret:🔑:secret"
    c = resolve_configuration(ZenConfig())
    assert isinstance(c.credentials, ZenApiKeyCredentials)
    assert c.credentials.api_key == "🔑"
    assert c.credentials.api_secret == "secret"

    os.environ["CREDENTIALS"] = "wrong"
    with pytest.raises(InvalidNativeValue):
        c = resolve_configuration(ZenConfig())


def test_unresolved_union() -> None:
    with pytest.raises(ConfigFieldMissingException) as cfm_ex:
        resolve_configuration(ZenConfig())
    assert cfm_ex.value.fields == ["credentials"]
    # all the missing fields from all the union elements are present
    checked_keys = set(
        t.key
        for t in itertools.chain(*cfm_ex.value.traces.values())
        if t.provider == EnvironProvider().name
    )
    assert checked_keys == {
        "CREDENTIALS__EMAIL",
        "CREDENTIALS__PASSWORD",
        "CREDENTIALS__API_KEY",
        "CREDENTIALS__API_SECRET",
    }


def test_union_decorator() -> None:
    import dlt

    # this will generate equivalent of ZenConfig
    @dlt.source
    def zen_source(
        credentials: Union[ZenApiKeyCredentials, ZenEmailCredentials, str] = dlt.secrets.value,
        some_option: bool = False,
    ):
        # depending on what the user provides in config, ZenApiKeyCredentials or ZenEmailCredentials will be injected in credentials
        # both classes implement `auth` so you can always call it
        credentials.auth()  # type: ignore[union-attr]
        return dlt.resource([credentials], name="credentials")

    # pass native value
    os.environ["CREDENTIALS"] = "email:mx:pwd"
    assert list(zen_source())[0].email == "mx"

    # pass explicit native value
    assert list(zen_source("secret:🔑:secret"))[0].api_secret == "secret"

    # pass explicit dict
    assert list(zen_source(credentials={"email": "emx", "password": "pass"}))[0].email == "emx"  # type: ignore[arg-type]
    assert list(zen_source(credentials={"api_key": "🔑", "api_secret": ":secret:"}))[0].api_key == "🔑"  # type: ignore[arg-type]
    # mixed credentials will not work
    with pytest.raises(ConfigFieldMissingException):
        assert list(zen_source(credentials={"api_key": "🔑", "password": "pass"}))[0].api_key == "🔑"  # type: ignore[arg-type]


class GoogleAnalyticsCredentialsBase(CredentialsConfiguration):
    """
    The Base version of all the GoogleAnalyticsCredentials classes.
    """

    pass


@configspec
class GoogleAnalyticsCredentialsOAuth(GoogleAnalyticsCredentialsBase):
    """
    This class is used to store credentials Google Analytics
    """

    client_id: str = None
    client_secret: TSecretStrValue = None
    project_id: TSecretStrValue = None
    refresh_token: TSecretStrValue = None
    access_token: Optional[TSecretStrValue] = None


@dlt.source(max_table_nesting=2)
def google_analytics(
    credentials: Union[
        GoogleAnalyticsCredentialsOAuth, GcpServiceAccountCredentials
    ] = dlt.secrets.value
):
    yield dlt.resource([credentials], name="creds")


def test_google_auth_union(environment: Any) -> None:
    info = {
        "type": "service_account",
        "project_id": "dlthub-analytics",
        "private_key_id": "45cbe97fbd3d756d55d4633a5a72d8530a05b993",
        "private_key": "-----BEGIN PRIVATE KEY-----\n\n-----END PRIVATE KEY-----\n",
        "client_email": "105150287833-compute@developer.gserviceaccount.com",
        "client_id": "106404499083406128146",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/105150287833-compute%40developer.gserviceaccount.com",
    }

    credentials = list(google_analytics(credentials=info))[0]  # type: ignore[arg-type]
    print(dict(credentials))
    assert isinstance(credentials, GcpServiceAccountCredentials)


class Engine:
    pass


@dlt.source
def sql_database(credentials: Union[ConnectionStringCredentials, Engine, str] = dlt.secrets.value):
    yield dlt.resource([credentials], name="creds")


def test_union_concrete_type(environment: Any) -> None:
    # we can pass engine explicitly
    engine = Engine()
    db = sql_database(credentials=engine)
    creds = list(db)[0]
    assert isinstance(creds, Engine)
    # we can pass valid connection string explicitly
    db = sql_database(credentials="sqlite://user@/:memory:")
    creds = list(db)[0]
    # but it is used as native value
    assert isinstance(creds, ConnectionStringCredentials)
    # pass instance of credentials
    cn = ConnectionStringCredentials("sqlite://user@/:memory:")
    db = sql_database(credentials=cn)
    # exactly that instance is returned
    assert list(db)[0] is cn
    # invalid cn
    with pytest.raises(InvalidNativeValue):
        db = sql_database(credentials="?")
    with pytest.raises(InvalidNativeValue):
        db = sql_database(credentials=123)  # type: ignore[arg-type]


def test_initialize_credentials(environment: Any) -> None:
    # test single credentials
    zen_cred = initialize_credentials(ZenEmailCredentials, None)
    assert isinstance(zen_cred, ZenEmailCredentials)
    assert not zen_cred.is_resolved()
    zen_cred = initialize_credentials(ZenEmailCredentials, "email:rfix:pass")
    assert zen_cred.is_resolved()
    zen_cred = initialize_credentials(ZenEmailCredentials, {"email": "rfix", "password": "pass"})
    assert zen_cred.is_resolved()
    with pytest.raises(NativeValueError):
        initialize_credentials(ZenEmailCredentials, "email")

    ZenUnion = Union[ZenApiKeyCredentials, ZenEmailCredentials]
    # if initial value does not fully resolve any of the credentials, the first one is instantiated
    zen_cred = initialize_credentials(ZenUnion, None)
    assert isinstance(zen_cred, ZenApiKeyCredentials)
    assert not zen_cred.is_resolved()
    zen_cred = initialize_credentials(ZenUnion, "email:rfix:pass")
    assert isinstance(zen_cred, ZenEmailCredentials)
    assert zen_cred.is_resolved()
    # resolve from dict
    zen_cred = initialize_credentials(ZenUnion, {"api_key": "key", "api_secret": "secret"})
    assert isinstance(zen_cred, ZenApiKeyCredentials)
    assert zen_cred.is_resolved()
    # does not fit any native format
    with pytest.raises(NativeValueError):
        initialize_credentials(ZenUnion, "email")
