import math
import dataclasses
from abc import abstractmethod
from base64 import b64encode
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Final,
    Iterable,
    List,
    Literal,
    Optional,
    Union,
    cast,
)
from typing_extensions import Annotated
from requests.auth import AuthBase
from requests import PreparedRequest, Session as BaseSession  # noqa: I251

from dlt.common import logger
from dlt.common.exceptions import MissingDependencyException
from dlt.common.configuration.specs.base_configuration import configspec, NotResolved
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.configuration.specs.exceptions import NativeValueError
from dlt.common.pendulum import pendulum
from dlt.common.typing import TSecretStrValue
from dlt.sources.helpers import requests

if TYPE_CHECKING:
    from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes
else:
    PrivateKeyTypes = Any

TApiKeyLocation = Literal["header", "cookie", "query", "param"]  # Alias for scheme "in" field


class AuthConfigBase(AuthBase, CredentialsConfiguration):
    """Authenticator base which is both `requests` friendly AuthBase and dlt SPEC
    configurable via env variables or toml files
    """

    def __bool__(self) -> bool:
        # This is needed to avoid AuthConfigBase-derived classes
        # which do not implement CredentialsConfiguration interface
        # to be evaluated as False in requests.sessions.Session.prepare_request()
        return True


@configspec
class BearerTokenAuth(AuthConfigBase):
    """Uses `token` for Bearer authentication in "Authorization" header."""

    token: TSecretStrValue = None

    def parse_native_representation(self, value: Any) -> None:
        if isinstance(value, str):
            self.token = cast(TSecretStrValue, value)
        else:
            raise NativeValueError(
                type(self),
                value,
                f"BearerTokenAuth token must be a string, got {type(value)}",
            )

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        request.headers["Authorization"] = f"Bearer {self.token}"
        return request


@configspec
class APIKeyAuth(AuthConfigBase):
    """Uses provided `api_key` to create authorization data in the specified `location` (query, param, header, cookie) under specified `name`"""

    name: str = "Authorization"
    api_key: TSecretStrValue = None
    location: TApiKeyLocation = "header"

    def parse_native_representation(self, value: Any) -> None:
        if isinstance(value, str):
            self.api_key = cast(TSecretStrValue, value)
        else:
            raise NativeValueError(
                type(self),
                value,
                f"APIKeyAuth api_key must be a string, got {type(value)}",
            )

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        if self.location == "header":
            request.headers[self.name] = self.api_key
        elif self.location in ["query", "param"]:
            request.prepare_url(request.url, {self.name: self.api_key})
        elif self.location == "cookie":
            raise NotImplementedError()
        return request


@configspec
class HttpBasicAuth(AuthConfigBase):
    """Uses HTTP basic authentication"""

    username: str = None
    password: TSecretStrValue = None

    def parse_native_representation(self, value: Any) -> None:
        if isinstance(value, Iterable) and not isinstance(value, str):
            value = list(value)
            if len(value) == 2:
                self.username, self.password = value
                return
        raise NativeValueError(
            type(self),
            value,
            "HttpBasicAuth username and password must be a tuple of two strings, got"
            f" {type(value)}",
        )

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        encoded = b64encode(f"{self.username}:{self.password}".encode()).decode()
        request.headers["Authorization"] = f"Basic {encoded}"
        return request


@configspec
class OAuth2AuthBase(AuthConfigBase):
    """Base class for oauth2 authenticators. requires access_token"""

    # TODO: Separate class for flows (implicit, authorization_code, client_credentials, etc)
    access_token: TSecretStrValue = None

    def parse_native_representation(self, value: Any) -> None:
        if isinstance(value, str):
            self.access_token = cast(TSecretStrValue, value)
        else:
            raise NativeValueError(
                type(self),
                value,
                f"OAuth2AuthBase access_token must be a string, got {type(value)}",
            )

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        request.headers["Authorization"] = f"Bearer {self.access_token}"
        return request


@configspec
class OAuth2ClientCredentials(OAuth2AuthBase):
    """
    This class implements OAuth2 Client Credentials flow where the autorization service
    gives permission without the end user approving.
    This is often used for machine-to-machine authorization.
    The client sends its client ID and client secret to the authorization service which replies
    with a temporary access token.
    With the access token, the client can access resource services.
    """

    def __init__(
        self,
        access_token_url: TSecretStrValue,
        client_id: TSecretStrValue,
        client_secret: TSecretStrValue,
        access_token_request_data: Dict[str, Any] = None,
        default_token_expiration: int = 3600,
        session: Annotated[BaseSession, NotResolved()] = None,
    ) -> None:
        super().__init__()
        self.access_token_url = access_token_url
        self.client_id = client_id
        self.client_secret = client_secret
        if access_token_request_data is None:
            self.access_token_request_data = {}
        else:
            self.access_token_request_data = access_token_request_data
        self.default_token_expiration = default_token_expiration
        self.token_expiry: pendulum.DateTime = pendulum.now()

        self.session = session if session is not None else requests.client.session

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        if self.access_token is None or self.is_token_expired():
            self.obtain_token()
        request.headers["Authorization"] = f"Bearer {self.access_token}"
        return request

    def is_token_expired(self) -> bool:
        return pendulum.now() >= self.token_expiry

    def obtain_token(self) -> None:
        response = self.session.post(self.access_token_url, **self.build_access_token_request())
        response.raise_for_status()
        response_json = response.json()
        self.parse_native_representation(self.parse_access_token(response_json))
        expires_in_seconds = self.parse_expiration_in_seconds(response_json)
        self.token_expiry = pendulum.now().add(seconds=expires_in_seconds)

    def build_access_token_request(self) -> Dict[str, Any]:
        return {
            "headers": {
                "Content-Type": "application/x-www-form-urlencoded",
            },
            "data": {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
                **self.access_token_request_data,
            },
        }

    def parse_expiration_in_seconds(self, response_json: Any) -> int:
        return int(response_json.get("expires_in", self.default_token_expiration))

    def parse_access_token(self, response_json: Any) -> str:
        return str(response_json.get("access_token"))


@configspec
class OAuthJWTAuth(BearerTokenAuth):
    """This is a form of Bearer auth, actually there's not standard way to declare it in openAPI"""

    format: Final[Literal["JWT"]] = dataclasses.field(  # noqa: A003
        default="JWT", init=False, repr=False, compare=False
    )
    client_id: str = None
    private_key: TSecretStrValue = None
    auth_endpoint: str = None
    scopes: Optional[Union[str, List[str]]] = None
    headers: Optional[Dict[str, str]] = None
    private_key_passphrase: Optional[TSecretStrValue] = None
    default_token_expiration: int = 3600
    session: Annotated[BaseSession, NotResolved()] = None

    def __post_init__(self) -> None:
        self.scopes = self.scopes if isinstance(self.scopes, str) else " ".join(self.scopes)
        self.token = None
        self.token_expiry: Optional[pendulum.DateTime] = None
        # use default system session unless specified otherwise
        if self.session is None:
            self.session = requests.client.session

    def __call__(self, r: PreparedRequest) -> PreparedRequest:
        if self.token is None or self.is_token_expired():
            self.obtain_token()
        r.headers["Authorization"] = f"Bearer {self.token}"
        return r

    def is_token_expired(self) -> bool:
        return not self.token_expiry or pendulum.now() >= self.token_expiry

    def obtain_token(self) -> None:
        try:
            import jwt
        except ModuleNotFoundError:
            raise MissingDependencyException("dlt OAuth helpers", ["PyJWT"])

        payload = self.create_jwt_payload()
        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt.encode(payload, self.load_private_key(), algorithm="RS256"),
        }

        logger.debug(f"Obtaining token from {self.auth_endpoint}")

        response = self.session.post(self.auth_endpoint, headers=self.headers, data=data)
        response.raise_for_status()

        token_response = response.json()
        self.token = token_response["access_token"]
        self.token_expiry = pendulum.now().add(
            seconds=token_response.get("expires_in", self.default_token_expiration)
        )

    def create_jwt_payload(self) -> Dict[str, Union[str, int]]:
        now = pendulum.now()
        return {
            "iss": self.client_id,
            "sub": self.client_id,
            "aud": self.auth_endpoint,
            "exp": math.floor((now.add(hours=1)).timestamp()),
            "iat": math.floor(now.timestamp()),
            "scope": cast(str, self.scopes),
        }

    def load_private_key(self) -> "PrivateKeyTypes":
        try:
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization
        except ModuleNotFoundError:
            raise MissingDependencyException("dlt OAuth helpers", ["cryptography"])

        private_key_bytes = self.private_key.encode("utf-8")
        return serialization.load_pem_private_key(
            private_key_bytes,
            password=(
                self.private_key_passphrase.encode("utf-8") if self.private_key_passphrase else None
            ),
            backend=default_backend(),
        )
