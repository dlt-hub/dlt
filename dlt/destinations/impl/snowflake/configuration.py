import dataclasses
import os
from pathlib import Path
from typing import Final, Optional, Any, Dict, ClassVar, List, Tuple

from dlt.common import logger
from dlt.common.destination.configuration import CsvFormatConfiguration
from dlt.common.libs.cryptography import decode_private_key
from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.specs import ConnectionStringCredentials, CredentialsWithDefault
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration import configspec
from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.utils import digest128, str2bool
from dlt.destinations.impl.snowflake.utils import (
    get_snowflake_connection_file_paths,
    get_snowflake_container_connection_name,
    get_snowflake_default_connection_name,
    list_snowflake_connections,
    read_snowflake_connection_params,
    read_snowflake_session_token,
    snowflake_session_token_available,
)


SNOWFLAKE_APPLICATION_ID = "dltHub_dlt"

CONNECTION_EXCLUSIVE_FIELDS: Tuple[str, ...] = (
    "host",
    "username",
    "password",
    "private_key",
    "private_key_path",
    "private_key_passphrase",
    "token",
    "authenticator",
    "workload_identity_provider",
    "_snowflake_host",
)
"""Credentials that a `connections.toml` connection provides.
They cannot be set together with `connection_name`."""

CONNECTION_OVERRIDE_PARAMS: Tuple[str, ...] = ("database", "warehouse", "role")
"""Session parameters that can override the values of the connection"""

CONNECTION_PARAMS_TO_FIELDS: Dict[str, str] = {
    "account": "host",
    "host": "_snowflake_host",
    "user": "username",
    "password": "password",
    "database": "database",
    "warehouse": "warehouse",
    "role": "role",
    "authenticator": "authenticator",
    "token": "token",
    "workload_identity_provider": "workload_identity_provider",
    "private_key": "private_key",
    "private_key_file": "private_key_path",
    "private_key_file_pwd": "private_key_passphrase",
}
"""Connector connection parameters and the credentials fields that receive them"""


@configspec(init=False)
class SnowflakeCredentialsWithoutDefaults(ConnectionStringCredentials):
    drivername: Final[str] = dataclasses.field(default="snowflake", init=False, repr=False, compare=False)  # type: ignore[misc]
    database: str = None
    host: str = None
    """Snowflake account identifier, e.g. `kgiotue-wn98412`"""
    username: Optional[str] = None
    warehouse: Optional[str] = None
    role: Optional[str] = None
    authenticator: Optional[str] = None
    token: Optional[str] = None
    workload_identity_provider: Optional[str] = None
    private_key: Optional[TSecretStrValue] = None
    private_key_path: Optional[str] = None
    private_key_passphrase: Optional[TSecretStrValue] = None
    application: Optional[str] = SNOWFLAKE_APPLICATION_ID
    session_timezone: Optional[str] = "UTC"
    """Session `TIMEZONE`. `None` keeps the account default"""
    connection_name: Optional[str] = None
    """Name of a connection in Snowflake `connections.toml`. The connector loads the connection and
    dlt sends only the session overrides. dlt also reads the connection parameters for tools that
    cannot use `connections.toml`, for example dbt."""

    _snowflake_host: Optional[str] = None
    """Snowflake account URL, e.g. https://kgiotue-wn98412.snowflakecomputing.com"""

    __config_gen_annotations__: ClassVar[List[str]] = [
        "username",
        "password",
        "warehouse",
        "role",
    ]
    __query_params__: ClassVar[List[str]] = [
        "warehouse",
        "role",
        "authenticator",
        "token",
        "workload_identity_provider",
        "private_key",
        "private_key_path",
        "private_key_passphrase",
        "connection_name",
    ]

    def parse_native_representation(self, native_value: Any) -> None:
        super().parse_native_representation(native_value)
        for param in self.__query_params__:
            if param in self.query:
                setattr(self, param, self.query.get(param))
        # the empty database of `snowflake:///?connection_name=dev` must not override the connection
        if self.connection_name and self.database == "":
            self.database = None

    def on_resolved(self) -> None:
        if self.connection_name:
            self._validate_connection_name()
            return

        self._read_private_key_path()

        if self._is_snowflake_auth and not self.private_key:
            if not self.username or not self.password:
                raise ConfigurationValueError(
                    "`SnowflakeCredentials` requires `username` and `password` to be specified when"
                    " using `snowflake` (username/password) authentication."
                )

    def get_query(self) -> Dict[str, Any]:
        query = dict(super().get_query() or {})
        for param in self.__query_params__:
            if self.get(param, None) is not None:
                query[param] = self[param]
        return query

    def to_connector_params(self) -> Dict[str, Any]:
        # gather all params in query
        query = self.get_query()
        if self.connection_name:
            return self._to_connection_params()

        if self.private_key:
            query["private_key"] = decode_private_key(self.private_key, self.private_key_passphrase)

        # we do not want passphrase to be passed
        query.pop("private_key_passphrase", None)

        conn_params: Dict[str, Any] = dict(
            query,
            user=self.username,
            password=self.password,
            account=self.host,
            database=self.database,
        )

        if self.application != "" and "application" not in conn_params:
            conn_params["application"] = self.application

        if self.session_timezone and "timezone" not in conn_params:
            conn_params["timezone"] = self.session_timezone

        # snowflake-connector-python fails if `host` is None, so we only set it if it's not None
        if self._snowflake_host:
            conn_params["host"] = self._snowflake_host

        return conn_params

    def _to_connection_params(self) -> Dict[str, Any]:
        """Connector parameters that load `connection_name` and apply the dlt session overrides."""
        # the connector loads the credentials itself, so dlt does not send them
        conn_params: Dict[str, Any] = dict(self.query or {})
        conn_params["connection_name"] = self.connection_name
        for key, path in get_snowflake_connection_file_paths(self.connection_name).items():
            conn_params.setdefault(key, path)
        for param in CONNECTION_OVERRIDE_PARAMS:
            if getattr(self, param):
                conn_params[param] = getattr(self, param)
        if self.application and "application" not in conn_params:
            conn_params["application"] = self.application
        if self.session_timezone and "timezone" not in conn_params:
            conn_params["timezone"] = self.session_timezone
        # a `None` or empty value overrides the connection value on connector 3.x and 4.x
        return {k: v for k, v in conn_params.items() if v is not None and v != ""}

    def _read_private_key_path(self) -> None:
        if self.private_key_path:
            try:
                self.private_key = Path(self.private_key_path).expanduser().read_text("ascii")
            except Exception:
                raise ValueError(
                    "Make sure that `private_key` in dlt recognized format is at"
                    f" `{self.private_key_path}`. Note that binary formats are not supported"
                )

    def _validate_connection_name(self) -> None:
        mixed = [f for f in CONNECTION_EXCLUSIVE_FIELDS if getattr(self, f)]
        if mixed:
            raise ConfigurationValueError(
                f"dlt cannot use Snowflake connection `{self.connection_name}` together with"
                f" {mixed}: the connection provides these credentials. Remove them or remove"
                " `connection_name`. Only `database`, `warehouse`, `role` and `query` can"
                " override the connection parameters."
            )

    @property
    def _has_explicit_auth(self) -> bool:
        return any(getattr(self, f) for f in CONNECTION_EXCLUSIVE_FIELDS)

    @property
    def _is_snowflake_auth(self) -> bool:
        return self._authenticator in (None, "snowflake")

    @property
    def _authenticator(self) -> Optional[str]:
        """`authenticator` in lower case, as the connector compares it."""
        return self.authenticator.lower() if self.authenticator else None


@configspec(init=False)
class SnowflakeCredentials(SnowflakeCredentialsWithoutDefaults, CredentialsWithDefault):
    allow_default_connection_fallback: bool = False
    """Use the default connection of the connector when `connection_name` is not defined"""

    _use_snowflake_session_token: bool = False

    def parse_native_representation(self, native_value: Any) -> None:
        super().parse_native_representation(native_value)
        # a dlt option, not a connector parameter
        if self.query and "allow_default_connection_fallback" in self.query:
            self.allow_default_connection_fallback = str2bool(
                self.query.pop("allow_default_connection_fallback")
            )

    def to_native_representation(self) -> str:
        if not (self.connection_name and self.has_default_credentials()):
            return super().to_native_representation()
        # the credentials loaded from the connection stand without it. a connection with `host`
        # and no `account` has no such form
        url = self.to_url()
        query = {k: v for k, v in url.query.items() if k != "connection_name"}
        return url.set(query=query).render_as_string(hide_password=False)

    def on_partial(self) -> None:
        # `on_resolved` rejects credentials mixed with a connection
        if not self.connection_name:
            if self._authenticator == "oauth" and not self.host:
                self._from_snowflake_session_token()
            elif not self._has_explicit_auth:
                self.connection_name = get_snowflake_container_connection_name()
                if self.connection_name:
                    logger.info(
                        f"dlt uses the Snowflake connection `{self.connection_name}` that the"
                        " container mounts"
                    )

        if self.connection_name or not self.is_partial():
            self.resolve()

    def on_resolved(self) -> None:
        if self.connection_name:
            self._from_connection()
        elif self._authenticator == "oauth" and not self.token:
            self._from_snowflake_session_token()

    def get_query(self) -> Dict[str, Any]:
        self._ensure_fresh_token()
        return super().get_query()

    def _from_snowflake_session_token(self) -> None:
        if not snowflake_session_token_available():
            raise ConfigurationValueError(
                "Snowflake-provided OAuth token not available. `dlt` expects this token to be"
                " available when `authenticator` is set to `oauth` and either `token` or `host` are"
                " not provided. "
            )
        self.host = os.environ["SNOWFLAKE_ACCOUNT"]
        self._snowflake_host = os.environ["SNOWFLAKE_HOST"]
        self._use_snowflake_session_token = True
        # tools that get the credentials, such as dbt, need the token. each connection reads it again
        self._ensure_fresh_token()

    def _ensure_fresh_token(self) -> None:
        if self._use_snowflake_session_token:
            self.token = read_snowflake_session_token()

    def _from_connection(self) -> None:
        """Loads the parameters of `connection_name` into the fields that are not set."""
        if self.connection_name not in list_snowflake_connections():
            self._use_default_connection()
        params = read_snowflake_connection_params(self.connection_name)
        for param, field_name in CONNECTION_PARAMS_TO_FIELDS.items():
            if param in params and getattr(self, field_name) is None:
                setattr(self, field_name, params[param])
        token_file_path = params.get("token_file_path")
        if token_file_path and self.token is None:
            self.token = Path(token_file_path).read_text("utf-8").strip()
        if self.private_key is None:
            self._read_private_key_path()
        self._set_default_credentials(self.connection_name)

    def _use_default_connection(self) -> None:
        """Replaces `connection_name`, which is not defined, with the default connection when
        `allow_default_connection_fallback` is set."""
        default_name = get_snowflake_default_connection_name()
        if not default_name:
            # `read_snowflake_connection_params` lists the known connections
            return
        if not self.allow_default_connection_fallback:
            raise ConfigurationValueError(
                f"dlt did not find Snowflake connection `{self.connection_name}`. Known"
                f" connections: {list_snowflake_connections()}. Set"
                " `allow_default_connection_fallback` to use the default connection"
                f" `{default_name}` instead."
            )
        logger.info(
            f"Snowflake connection `{self.connection_name}` is not defined: dlt uses the default"
            f" Snowflake connection `{default_name}`"
        )
        self.connection_name = default_name


@configspec
class SnowflakeClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = dataclasses.field(default="snowflake", init=False, repr=False, compare=False)  # type: ignore[misc]
    credentials: SnowflakeCredentials = None

    stage_name: Optional[str] = None
    """Use an existing named stage instead of the default. Default uses the implicit table stage per table"""
    keep_staged_files: bool = True
    """Whether to keep or delete the staged files after COPY INTO succeeds"""

    csv_format: Optional[CsvFormatConfiguration] = None
    """Optional csv format configuration"""

    query_tag: Optional[str] = None
    """A template with placeholders used to tag Snowflake sessions for dlt operations."""

    create_indexes: bool = False
    """Whether UNIQUE or PRIMARY KEY constrains should be created"""

    use_vectorized_scanner: bool = False
    """Whether to use or not use the vectorized scanner in COPY INTO"""

    enable_atomic_swap: bool = False
    """When enabled and using the `staging-optimized` replace strategy, tables will be atomically swapped using `ALTER TABLE ... SWAP WITH ...` instead of being cloned. This preserves permissions and enables zero-downtime loading."""

    use_decfloat: bool = False
    """Whether to use DECFLOAT type for unbound decimals instead of DECIMAL"""

    use_nested_types: bool = False
    """When true, arrow-nested `json` columns are created as native ARRAY/OBJECT (structured) types instead of VARIANT."""

    use_timestamp_tz: bool = False
    """When true, timezone-aware timestamps are created as `TIMESTAMP_TZ`, which stores the offset written with each value, instead of `TIMESTAMP_LTZ`"""

    def fingerprint(self) -> str:
        """Returns a fingerprint of the account host."""
        account = self._account_identifier()
        return digest128(account) if account else ""

    def data_location(self) -> str:
        """Returns the account host."""
        account = self._account_identifier()
        if not account:
            self._no_data_location("the configuration has no account host")
        return account

    def _account_identifier(self) -> Optional[str]:
        if not self.credentials:
            return None
        # a connection can identify its account by host only
        return self.credentials.host or self.credentials._snowflake_host
