import itertools
import os
import pytest
from typing import Union, Any

from dlt.common.configuration.exceptions import InvalidNativeValue, ConfigFieldMissingException
from dlt.common.configuration.providers import EnvironProvider
from dlt.common.configuration.specs import CredentialsConfiguration, BaseConfiguration
from dlt.common.configuration import configspec, resolve_configuration
from dlt.common.typing import TSecretValue

from tests.utils import preserve_environ


@configspec
class ZenCredentials(CredentialsConfiguration):
    def auth(self):
        pass


@configspec
class ZenEmailCredentials(ZenCredentials):
    email: str
    password: TSecretValue

    def parse_native_representation(self, native_value: Any) -> None:
        assert isinstance(native_value, str)
        if native_value.startswith("email:"):
            parts = native_value.split(":")
            self.email = parts[-2]
            self.password = parts[-1]
        else:
            raise InvalidNativeValue(self.__class__, type(native_value), ("credentials", ), ValueError(native_value))

    def auth(self):
        return "email-cookie"


@configspec
class ZenApiKeyCredentials(ZenCredentials):
    api_key: str
    api_secret: TSecretValue

    def parse_native_representation(self, native_value: Any) -> None:
        assert isinstance(native_value, str)
        if native_value.startswith("secret:"):
            parts = native_value.split(":")
            self.api_key = parts[-2]
            self.api_secret = parts[-1]
        else:
            raise InvalidNativeValue(self.__class__, type(native_value), ("credentials", ), ValueError(native_value))

    def auth(self):
        return "api-cookie"


@configspec
class ZenConfig(BaseConfiguration):
    credentials: Union[ZenApiKeyCredentials, ZenEmailCredentials]
    some_option: bool = False


@configspec
class ZenConfigOptCredentials:
    # add none to union to make it optional
    credentials: Union[ZenApiKeyCredentials, ZenEmailCredentials, None]
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
    c = resolve_configuration(ZenConfigOptCredentials())
    assert c.is_partial
    # assert c.is
    assert c.credentials is None

    # if we provide values for second union, it will be tried and resolved
    os.environ["CREDENTIALS__EMAIL"] = "email"
    os.environ["CREDENTIALS__PASSWORD"] = "password"
    c = resolve_configuration(ZenConfigOptCredentials())
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
    checked_keys = set(t.key for t in itertools.chain(*cfm_ex.value.traces.values()) if t.provider == EnvironProvider().name)
    assert checked_keys == {"CREDENTIALS__EMAIL", "CREDENTIALS__PASSWORD", "CREDENTIALS__API_KEY", "CREDENTIALS__API_SECRET"}


def test_union_decorator() -> None:
    import dlt

    # this will generate equivalent of ZenConfig
    @dlt.source
    def zen_source(credentials: Union[ZenApiKeyCredentials, ZenEmailCredentials, str] = dlt.secrets.value, some_option: bool = False):
        # depending on what the user provides in config, ZenApiKeyCredentials or ZenEmailCredentials will be injected in credentials
        # both classes implement `auth` so you can always call it
        credentials.auth()
        return dlt.resource([credentials], name="credentials")

    # pass native value
    os.environ["CREDENTIALS"] = "email:mx:pwd"
    assert list(zen_source())[0].email == "mx"

    # pass explicit native value
    assert list(zen_source("secret:🔑:secret"))[0].api_secret == "secret"

    # pass explicit dict
    assert list(zen_source(credentials={"email": "emx", "password": "pass"}))[0].email == "emx"
