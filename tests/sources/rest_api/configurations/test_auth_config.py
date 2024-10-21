import re
from typing import Any, Dict, List, Literal, NamedTuple, Optional, Union, cast, get_args

import pytest
from requests.auth import AuthBase

import dlt
import dlt.common
import dlt.common.exceptions
import dlt.extract
from dlt.common.configuration import inject_section
from dlt.common.configuration.specs import ConfigSectionContext
from dlt.common.typing import TSecretStrValue
from dlt.common.utils import custom_environ
from dlt.sources.rest_api import (
    _mask_secrets,
    rest_api_source,
)
from dlt.sources.rest_api.config_setup import (
    AUTH_MAP,
    create_auth,
)
from dlt.sources.rest_api.typing import (
    AuthConfigBase,
    AuthType,
    AuthTypeConfig,
    RESTAPIConfig,
)

try:
    from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
except ImportError:
    pass


from dlt.sources.helpers.rest_client.auth import (
    APIKeyAuth,
    BearerTokenAuth,
    HttpBasicAuth,
    OAuth2ClientCredentials,
)

from .source_configs import (
    AUTH_TYPE_CONFIGS,
)


@pytest.mark.parametrize("auth_type", get_args(AuthType))
@pytest.mark.parametrize(
    "section", ("SOURCES__REST_API__CREDENTIALS", "SOURCES__CREDENTIALS", "CREDENTIALS")
)
def test_auth_shorthands(auth_type: AuthType, section: str) -> None:
    # mock all required envs
    with custom_environ(
        {
            f"{section}__TOKEN": "token",
            f"{section}__API_KEY": "api_key",
            f"{section}__USERNAME": "username",
            f"{section}__PASSWORD": "password",
            f"{section}__ACCESS_TOKEN_URL": "https://example.com/oauth/token",
            f"{section}__CLIENT_ID": "a_client_id",
            f"{section}__CLIENT_SECRET": "a_client_secret",
        }
    ):
        # shorthands need to instantiate from config
        with inject_section(
            ConfigSectionContext(sections=("sources", "rest_api")), merge_existing=False
        ):
            import os

            print(os.environ)
            auth = create_auth(auth_type)
            assert isinstance(auth, AUTH_MAP[auth_type])
            if isinstance(auth, BearerTokenAuth):
                assert auth.token == "token"
            if isinstance(auth, APIKeyAuth):
                assert auth.api_key == "api_key"
                assert auth.location == "header"
                assert auth.name == "Authorization"
            if isinstance(auth, HttpBasicAuth):
                assert auth.username == "username"
                assert auth.password == "password"
            if isinstance(auth, OAuth2ClientCredentials):
                assert auth.access_token_url == "https://example.com/oauth/token"
                assert auth.client_id == "a_client_id"
                assert auth.client_secret == "a_client_secret"
                assert auth.default_token_expiration == 3600


@pytest.mark.parametrize("auth_type_config", AUTH_TYPE_CONFIGS)
@pytest.mark.parametrize(
    "section", ("SOURCES__REST_API__CREDENTIALS", "SOURCES__CREDENTIALS", "CREDENTIALS")
)
def test_auth_type_configs(auth_type_config: AuthTypeConfig, section: str) -> None:
    # mock all required envs
    with custom_environ(
        {
            f"{section}__API_KEY": "api_key",
            f"{section}__NAME": "session-cookie",
            f"{section}__PASSWORD": "password",
            f"{section}__CLIENT_SECRET": "a_client_secret",
            f"{section}__CLIENT_ID": "a_client_id",
        }
    ):
        # shorthands need to instantiate from config
        with inject_section(
            ConfigSectionContext(sections=("sources", "rest_api")), merge_existing=False
        ):
            auth = create_auth(auth_type_config)  # type: ignore
            assert isinstance(auth, AUTH_MAP[auth_type_config["type"]])
            if isinstance(auth, BearerTokenAuth):
                # from typed dict
                assert auth.token == "token"
            if isinstance(auth, APIKeyAuth):
                assert auth.location == "cookie"
                # injected
                assert auth.api_key == "api_key"
                assert auth.name == "session-cookie"
            if isinstance(auth, HttpBasicAuth):
                # typed dict
                assert auth.username == "username"
                # injected
                assert auth.password == "password"
            if isinstance(auth, OAuth2ClientCredentials):
                assert auth.access_token_url == "https://example.com/oauth/token"
                assert auth.default_token_expiration == 60
                # injected
                assert auth.client_id == "a_client_id"
                assert auth.client_secret == "a_client_secret"


@pytest.mark.parametrize(
    "section", ("SOURCES__REST_API__CREDENTIALS", "SOURCES__CREDENTIALS", "CREDENTIALS")
)
def test_auth_instance_config(section: str) -> None:
    auth = APIKeyAuth(location="param", name="token")
    with custom_environ(
        {
            f"{section}__API_KEY": "api_key",
            f"{section}__NAME": "session-cookie",
        }
    ):
        # shorthands need to instantiate from config
        with inject_section(
            ConfigSectionContext(sections=("sources", "rest_api")), merge_existing=False
        ):
            # this also resolved configuration
            resolved_auth = create_auth(auth)
            assert resolved_auth is auth
            # explicit
            assert auth.location == "param"
            # injected
            assert auth.api_key == "api_key"
            # config overrides explicit (TODO: reverse)
            assert auth.name == "session-cookie"


def test_bearer_token_fallback() -> None:
    auth = create_auth({"token": "secret"})
    assert isinstance(auth, BearerTokenAuth)
    assert auth.token == "secret"


def test_error_message_invalid_auth_type() -> None:
    with pytest.raises(ValueError) as e:
        create_auth("non_existing_method")  # type: ignore
    assert (
        str(e.value)
        == "Invalid authentication: non_existing_method."
        " Available options: bearer, api_key, http_basic, oauth2_client_credentials."
    )


class AuthConfigTest(NamedTuple):
    secret_keys: List[
        Literal[
            "token", "api_key", "password", "username", "client_id", "client_secret", "access_token"
        ]
    ]
    config: Union[Dict[str, Any], AuthConfigBase]
    masked_secrets: Optional[List[str]] = ["s*****t"]


SENSITIVE_SECRET = cast(TSecretStrValue, "sensitive-secret")

AUTH_CONFIGS = [
    AuthConfigTest(
        secret_keys=["token"],
        config={
            "type": "bearer",
            "token": "sensitive-secret",
        },
    ),
    AuthConfigTest(
        secret_keys=["api_key"],
        config={
            "type": "api_key",
            "api_key": "sensitive-secret",
        },
    ),
    AuthConfigTest(
        secret_keys=["username", "password"],
        config={
            "type": "http_basic",
            "username": "sensitive-secret",
            "password": "sensitive-secret",
        },
        masked_secrets=["s*****t", "s*****t"],
    ),
    AuthConfigTest(
        secret_keys=["username", "password"],
        config={
            "type": "http_basic",
            "username": "",
            "password": "sensitive-secret",
        },
        masked_secrets=["*****", "s*****t"],
    ),
    AuthConfigTest(
        secret_keys=["username", "password"],
        config={
            "type": "http_basic",
            "username": "sensitive-secret",
            "password": "",
        },
        masked_secrets=["s*****t", "*****"],
    ),
    AuthConfigTest(
        secret_keys=["token"],
        config=BearerTokenAuth(token=SENSITIVE_SECRET),
    ),
    AuthConfigTest(
        secret_keys=["api_key"],
        config=APIKeyAuth(api_key=SENSITIVE_SECRET),
    ),
    AuthConfigTest(
        secret_keys=["username", "password"],
        config=HttpBasicAuth("sensitive-secret", SENSITIVE_SECRET),
        masked_secrets=["s*****t", "s*****t"],
    ),
    AuthConfigTest(
        secret_keys=["username", "password"],
        config=HttpBasicAuth("sensitive-secret", cast(TSecretStrValue, "")),
        masked_secrets=["s*****t", "*****"],
    ),
    AuthConfigTest(
        secret_keys=["username", "password"],
        config=HttpBasicAuth("", SENSITIVE_SECRET),
        masked_secrets=["*****", "s*****t"],
    ),
    AuthConfigTest(
        secret_keys=["client_id", "client_secret", "access_token"],
        config=OAuth2ClientCredentials(
            access_token=SENSITIVE_SECRET,
            client_id=SENSITIVE_SECRET,
            client_secret=SENSITIVE_SECRET,
        ),
        masked_secrets=["s*****t", "s*****t", "s*****t"],
    ),
]


@pytest.mark.parametrize("secret_keys, config, masked_secrets", AUTH_CONFIGS)
def test_secret_masking_auth_config(secret_keys, config, masked_secrets):
    masked = _mask_secrets(config)
    for key, mask in zip(secret_keys, masked_secrets):
        assert masked[key] == mask  # type: ignore[literal-required]


def test_secret_masking_oauth() -> None:
    config = OAuth2ClientCredentials(
        access_token_url=cast(TSecretStrValue, ""),
        client_id=cast(TSecretStrValue, "sensitive-secret"),
        client_secret=cast(TSecretStrValue, "sensitive-secret"),
    )

    masked = _mask_secrets(config)
    assert "sensitive-secret" not in str(masked)

    # TODO
    # assert masked.access_token == "None"
    # assert masked.client_id == "s*****t"
    # assert masked.client_secret == "s*****t"


def test_secret_masking_custom_auth() -> None:
    class CustomAuthConfigBase(AuthConfigBase):
        def __init__(self, token: str = "sensitive-secret"):
            self.token = token

    class CustomAuthBase(AuthBase):
        def __init__(self, token: str = "sensitive-secret"):
            self.token = token

    auth = _mask_secrets(CustomAuthConfigBase())
    assert "s*****t" not in str(auth)
    # TODO
    # assert auth.token == "s*****t"

    auth_2 = _mask_secrets(CustomAuthBase())  # type: ignore[arg-type]
    assert "s*****t" not in str(auth_2)
    # TODO
    # assert auth_2.token == "s*****t"


def test_validation_masks_auth_secrets() -> None:
    incorrect_config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
            "auth": {  # type: ignore[typeddict-item]
                "type": "bearer",
                "location": "header",
                "token": "sensitive-secret",
            },
        },
        "resources": ["posts"],
    }
    with pytest.raises(dlt.common.exceptions.DictValidationException) as e:
        rest_api_source(incorrect_config)
    assert (
        re.search("sensitive-secret", str(e.value)) is None
    ), "unexpectedly printed 'sensitive-secret'"
    assert e.match(re.escape("'{'type': 'bearer', 'location': 'header', 'token': 's*****t'}'"))
