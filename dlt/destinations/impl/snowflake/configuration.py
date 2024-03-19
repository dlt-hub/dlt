import os
import base64
from typing import Final, Optional, Any, Dict, ClassVar, List, TYPE_CHECKING

from dlt.common.configuration.specs import CredentialsWithDefault
from dlt.common.libs.sql_alchemy import URL

from dlt import version
from dlt.common import logger
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration import configspec
from dlt.common.destination.reference import DestinationClientDwhWithStagingConfiguration
from dlt.common.utils import digest128


def _read_private_key(private_key: str, password: Optional[str] = None) -> bytes:
    """Load an encrypted or unencrypted private key from string."""
    try:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives.asymmetric import dsa
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes
    except ModuleNotFoundError as e:
        raise MissingDependencyException(
            "SnowflakeCredentials with private key",
            dependencies=[f"{version.DLT_PKG_NAME}[snowflake]"],
        ) from e

    try:
        # load key from base64-encoded DER key
        pkey = serialization.load_der_private_key(
            base64.b64decode(private_key),
            password=password.encode() if password is not None else None,
            backend=default_backend(),
        )
    except Exception:
        # loading base64-encoded DER key failed -> assume it's a plain-text PEM key
        pkey = serialization.load_pem_private_key(
            private_key.encode(encoding="ascii"),
            password=password.encode() if password is not None else None,
            backend=default_backend(),
        )

    return pkey.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


@configspec
class SnowflakeCredentialsWithoutDefaults(ConnectionStringCredentials):
    drivername: Final[str] = "snowflake"  # type: ignore[misc]
    password: Optional[TSecretStrValue] = None
    token: Optional[TSecretStrValue] = None
    username: Optional[str] = None
    host: str = None
    """Snowflake account name"""
    database: str = None
    warehouse: Optional[str] = None
    role: Optional[str] = None
    authenticator: Optional[str] = None
    private_key: Optional[TSecretStrValue] = None
    private_key_passphrase: Optional[TSecretStrValue] = None
    _hostname: Optional[str] = None
    "Snowflake host, present in Snowflake Container Services"

    __config_gen_annotations__: ClassVar[List[str]] = ["password", "warehouse", "role"]

    def is_partial(self) -> bool:
        return super().is_partial() or (
            not self.password and not self.token and not self.private_key
        )

    def parse_native_representation(self, native_value: Any) -> None:
        super().parse_native_representation(native_value)
        self.warehouse = self.query.get("warehouse")
        self.role = self.query.get("role")
        self.token = self.query.get("token")  # type: ignore
        self.authenticator = self.query.get("authenticator")
        self.private_key = self.query.get("private_key")  # type: ignore
        self.private_key_passphrase = self.query.get("private_key_passphrase")  # type: ignore
        if not self.is_partial():
            self.resolve()

    def on_resolved(self) -> None:
        if not self.password and not self.private_key and not self.token:
            raise ConfigurationValueError(
                "Please specify password or private_key or oauth token. SnowflakeCredentials"
                " supports password and private key authentication and one of those must be"
                " specified. It also recognizes a login token if passed via `token` query parameter"
                " and with `authenticator` set to `oauth`."
            )
        if (self.password or self.private_key) and not self.username:
            raise ConfigurationValueError(
                "Please provide username when using password / private key authentication."
            )
        if self.token and self.authenticator != "oauth":
            logger.warning(
                "Login token was specified but authenticator is not set to oauth so it will be"
                " ignored"
            )

    def to_url(self) -> URL:
        query = dict(self.query or {})
        if self.warehouse:
            query["warehouse"] = self.warehouse
        if self.role:
            query["role"] = self.role
        if self.authenticator:
            query["authenticator"] = self.authenticator

        return URL.create(
            self.drivername,
            self.username,
            self.password,
            self.host,
            self.port,
            self.database,
            query,
        )

    def to_connector_params(self) -> Dict[str, Any]:
        private_key: Optional[bytes] = None
        if self.private_key:
            private_key = _read_private_key(self.private_key, self.private_key_passphrase)
        conn_params = dict(
            self.query or {},
            user=self.username,
            password=self.password,
            account=self.host,
            database=self.database,
            warehouse=self.warehouse,
            role=self.role,
            private_key=private_key,
        )
        if self.authenticator:
            conn_params["authenticator"] = self.authenticator
            if self.authenticator == "oauth":
                conn_params["token"] = self.login_token()
                conn_params["host"] = self._hostname

        return conn_params

    def login_token(self) -> str:
        """A method returning fresh login token"""
        return self.token


@configspec
class SnowflakeCredentials(SnowflakeCredentialsWithoutDefaults, CredentialsWithDefault):
    LOGIN_TOKEN_PATH: ClassVar[str] = "/snowflake/session/token"

    def on_partial(self) -> None:
        logger.info(
            "Snowflake credentials could not be resolved, looking for login token and env variables"
        )
        token = self._from_token()
        if token and not self.is_partial():
            logger.info("Login token and env variable found, switching to oauth authenticator")
            self.resolve()
            self._set_default_credentials(token)

    def _from_token(self) -> str:
        token = self.login_token()
        if token:
            if self.private_key or self.password:
                logger.warning(
                    "Password or private key were provided to SnowflakeCredentials and oauth2"
                    " token was found so they will be ignored."
                )
                self.private_key = None
                self.password = None
            # set authenticator
            self.host, self._hostname = os.getenv("SNOWFLAKE_ACCOUNT"), os.getenv("SNOWFLAKE_HOST")
            self.authenticator = "oauth"
            # NOTE: set token here so configuration resolves. still we'll be reading it each time connection params are created
            self.token = token  # type: ignore[assignment]
        return token

    def login_token(self) -> str:
        """
        Read the login token supplied automatically by Snowflake. These tokens
        are short lived and should always be read right before creating any new connection.
        """
        try:
            with open(SnowflakeCredentials.LOGIN_TOKEN_PATH, "r", encoding="utf-8") as f:
                return f.read()
        except Exception:
            return self.token


@configspec
class SnowflakeClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = "snowflake"  # type: ignore[misc]
    credentials: SnowflakeCredentials

    stage_name: Optional[str] = None
    """Use an existing named stage instead of the default. Default uses the implicit table stage per table"""
    keep_staged_files: bool = True
    """Whether to keep or delete the staged files after COPY INTO succeeds"""

    def fingerprint(self) -> str:
        """Returns a fingerprint of host part of a connection string"""
        if self.credentials and self.credentials.host:
            return digest128(self.credentials.host)
        return ""

    if TYPE_CHECKING:

        def __init__(
            self,
            *,
            destination_type: str = None,
            credentials: SnowflakeCredentials = None,
            dataset_name: str = None,
            default_schema_name: str = None,
            stage_name: str = None,
            keep_staged_files: bool = True,
            destination_name: str = None,
            environment: str = None,
        ) -> None: ...
