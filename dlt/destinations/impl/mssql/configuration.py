import dataclasses
import struct
import warnings
from typing import ClassVar, Any, Final, List, Dict, Optional

from dlt.common.configuration import configspec, NotResolved
from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.configuration.specs import ConnectionStringCredentials, CredentialsWithDefault
from dlt.common.typing import TSecretStrValue, Annotated
from dlt.common.warnings import DltDeprecationWarning

from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.utils import digest128

# ODBC attribute used to inject a pre-acquired Entra ID access token via `attrs_before`, for the
# explicit `access_token` credential field.
# https://learn.microsoft.com/sql/connect/odbc/using-azure-active-directory#authenticating-with-an-access-token
SQL_COPT_SS_ACCESS_TOKEN = 1256

# mirrors mssql-python's own `_SENSITIVE_KEYS` (`auth.py`): the keys it strips from a connection
# string once an Entra token is in play. Applied to free-form query keys, which never pass through
# `apply_authentication_to_dsn`, so they would otherwise reach the driver and win over the token.
_TOKEN_INCOMPATIBLE_DSN_KEYS = frozenset({"authentication", "uid", "pwd", "trusted_connection"})

# Entra ID authentication methods supported by mssql-python's `Authentication=` connection
# option. mssql-python performs the sign-in for all of them; dlt only builds the DSN.
SUPPORTED_AUTHENTICATION = frozenset(
    {
        "ActiveDirectoryServicePrincipal",
        "ActiveDirectoryPassword",
        "ActiveDirectoryIntegrated",
        "ActiveDirectoryInteractive",
        "ActiveDirectoryMsi",
        "ActiveDirectoryDefault",
        "ActiveDirectoryDeviceCode",
    }
)

# Thin alias for `ActiveDirectoryDefault`, resolved by `_normalize_authentication`.
_AUTHENTICATION_ALIASES = {
    "default": "ActiveDirectoryDefault",
}


def _normalize_authentication(authentication: str) -> str:
    """Resolve the thin `default` alias to the canonical `ActiveDirectoryDefault` name."""
    return _AUTHENTICATION_ALIASES.get(authentication.lower(), authentication)


def uses_explicit_token(credentials: Any) -> bool:
    """True when `access_token` or `azure_credential` bypasses `authentication`."""
    # identity, not truthiness: a credential defining `__bool__`/`__len__` would otherwise take the
    # `authentication` branch here while `select_token_provider` still hands it to the driver
    return bool(credentials.access_token) or credentials.azure_credential is not None


def build_access_token_attrs_before(credentials: Any) -> dict[int, bytes] | None:
    """Return `attrs_before` with the pre-acquired `access_token`, or None."""
    if not credentials.access_token:
        return None
    encoded_token = str(credentials.access_token).encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(encoded_token)}s", len(encoded_token), encoded_token)
    return {SQL_COPT_SS_ACCESS_TOKEN: token_struct}


def select_token_provider(credentials: Any) -> Optional[Any]:
    """Return the credential mssql-python should acquire Entra ID tokens from, or None.

    `access_token` is already a token and keeps the `attrs_before` path, so it suppresses the
    provider — mssql-python rejects both token sources being supplied at once.
    """
    if credentials.access_token:
        return None
    return credentials.azure_credential


def validate_authentication(credentials: Any) -> None:
    """Validate the configured authentication method."""
    if uses_explicit_token(credentials):
        # A token (or a credential able to fetch one) was injected directly: it takes
        # precedence over `authentication`, whose value does not need to be validated.
        return
    authentication = credentials.authentication
    if not authentication:
        return  # plain SQL login (username/password)
    normalized = _normalize_authentication(authentication)
    if normalized not in SUPPORTED_AUTHENTICATION:
        raise ConfigurationException(
            f"Unsupported `authentication` method `{authentication}`."
            f" Supported methods: {', '.join(sorted(SUPPORTED_AUTHENTICATION))}."
        )
    if authentication == "ActiveDirectoryPassword" and not (
        credentials.username and credentials.password
    ):
        raise ConfigurationException(
            "`authentication = ActiveDirectoryPassword` requires `username` and `password`."
        )


def apply_authentication_to_dsn(credentials: Any, params: dict[str, Any]) -> None:
    """Add UID/PWD/Authentication keys to an ODBC DSN dict based on the authentication method."""
    # an explicit token takes precedence: auth is handled entirely outside the DSN
    if uses_explicit_token(credentials):
        return
    authentication = credentials.authentication
    if not authentication:
        # Plain SQL login.
        params["UID"] = credentials.username
        params["PWD"] = credentials.password
        return
    # Write the canonical name, not the thin `default` alias — mssql-python only recognizes the
    # canonical `ActiveDirectory*` values in the `Authentication=` DSN keyword.
    authentication = _normalize_authentication(authentication)
    params["AUTHENTICATION"] = authentication
    if (
        authentication == "ActiveDirectoryServicePrincipal"
        and credentials.azure_client_id
        and credentials.azure_tenant_id
        and credentials.azure_client_secret
    ):
        params["UID"] = f"{credentials.azure_client_id}@{credentials.azure_tenant_id}"
        params["PWD"] = str(credentials.azure_client_secret)
    elif (
        authentication == "ActiveDirectoryPassword"
        and credentials.username
        and credentials.password
    ):
        params["UID"] = credentials.username
        params["PWD"] = credentials.password


def escape_mssql_odbc_value(value: Optional[str]) -> str:
    """Escape a value for MSSQL ADO/ODBC connection string format.

    ODBC format supports `{value}` syntax where:
      - `}` inside braces must be doubled to `}}`
      - `;` can safely appear inside braces

    To safely handle values with special characters, we use ODBC-style bracing:
    - Values containing `;` or `}` are wrapped in `{}`
    - `}` inside the value is escaped as `}}`

    Args:
        value: The value to escape

    Returns:
        Escaped value safe for use in ADO/ODBC connection string
    """
    if not value:
        return ""
    # if value contains ; or }, use braced syntax with }} escaping
    if ";" in value or "}" in value:
        return "{" + value.replace("}", "}}") + "}"
    return value


def build_odbc_dsn(params: Dict[str, Any]) -> str:
    """Build an ADO/ODBC connection string for MSSQL, escaping values

    Args:
        params: Dictionary of connection parameters

    Returns:
        ADO/ODBC connection string
    """
    return ";".join(
        f"{k}={escape_mssql_odbc_value(str(v))}" for k, v in params.items() if v is not None
    )


@configspec(init=False)
class MsSqlCredentials(ConnectionStringCredentials, CredentialsWithDefault):
    drivername: Final[str] = dataclasses.field(default="mssql", init=False, repr=False, compare=False)  # type: ignore[misc]
    database: str = None
    username: str = None
    password: TSecretStrValue = None
    host: str = None
    port: int = 1433
    connect_timeout: int = 30
    driver: Optional[str] = None
    """DEPRECATED: ignored, mssql-python installs and manages its own driver dependency"""

    authentication: str | None = None
    """Authentication method. Empty (default) uses plain SQL login (`username`/`password`).
    Supported Entra ID methods, passed straight through as `Authentication=` in the DSN:
    `ActiveDirectoryServicePrincipal`, `ActiveDirectoryPassword`, `ActiveDirectoryIntegrated`,
    `ActiveDirectoryInteractive`, `ActiveDirectoryMsi`, `ActiveDirectoryDefault` (alias
    `default`), `ActiveDirectoryDeviceCode`."""

    azure_tenant_id: str | None = None
    """Entra ID tenant id, used with `ActiveDirectoryServicePrincipal` authentication."""

    azure_client_id: str | None = None
    """Service Principal client id, used with `ActiveDirectoryServicePrincipal` authentication."""

    azure_client_secret: TSecretStrValue | None = None
    """Service Principal client secret, used with `ActiveDirectoryServicePrincipal` authentication."""

    access_token: Optional[TSecretStrValue] = None
    """Pre-acquired Entra ID access token, injected as-is. Takes precedence over `azure_credential`
    and `authentication`. Use it for sovereign clouds, whose scope `azure_credential` cannot reach"""

    azure_credential: Annotated[Optional[Any], NotResolved()] = None
    """A `TokenCredential` injected at runtime, e.g. `DefaultAzureCredential()`, handed to
    mssql-python as `token_provider`. Takes precedence over `authentication` but not over
    `access_token`. The driver acquires the token for the Azure commercial SQL scope only"""

    __config_gen_annotations__: ClassVar[List[str]] = ["port", "connect_timeout"]

    def parse_native_representation(self, native_value: Any) -> None:
        # TODO: Support ODBC connection string or sqlalchemy URL
        super().parse_native_representation(native_value)
        if self.query is not None:
            self.query = {k.lower(): v for k, v in self.query.items()}  # Make case-insensitive.
        self.driver = self.query.get("driver", self.driver)
        self.connect_timeout = int(self.query.get("connect_timeout", self.connect_timeout))

    def on_resolved(self) -> None:
        validate_authentication(self)
        if self.driver:
            warnings.warn(
                DltDeprecationWarning(
                    "`driver` is deprecated and ignored; mssql-python installs and manages its"
                    " own driver dependency",
                    since="1.30.0",
                ),
                stacklevel=2,
            )
        self.database = self.database.lower()

    def get_query(self) -> Dict[str, Any]:
        query = dict(super().get_query())
        query["connect_timeout"] = self.connect_timeout
        return query

    def on_partial(self) -> None:
        if self.authentication or self.access_token or self.azure_credential:
            # Entra ID methods supply their own credentials and do not rely on username/password;
            # resolve once we have a target. `on_resolved` validates.
            if self.host and self.database:
                self.resolve()
        elif not self.is_partial():
            # Plain SQL login needs username/password.
            self.resolve()

    def get_odbc_dsn_dict(self) -> Dict[str, Any]:
        # mssql-python installs and manages its own driver dependency, so no DRIVER key is emitted.
        params: dict[str, Any] = {
            "SERVER": f"{self.host},{self.port}",
            "DATABASE": self.database,
        }
        apply_authentication_to_dsn(self, params)
        if self.query is not None:
            # mssql-python's connection-string parser rejects unknown keywords. `connect_timeout`
            # is passed separately via the connect() `timeout=` parameter, and `longasmax` is
            # unnecessary since the driver handles long/max types natively, so neither belongs
            # in the DSN.
            skip_keys = {"driver", "connect_timeout", "longasmax"}
            params.update(
                {k.upper(): v for k, v in self.query.items() if k.lower() not in skip_keys}
            )
        return params

    def to_odbc_dsn(self) -> str:
        params = self.get_odbc_dsn_dict()
        if uses_explicit_token(self):
            # BREAKING: an `authentication`/`uid`/`pwd`/`trusted_connection` query key is no longer
            # passed through when `access_token`/`azure_credential` is set. It used to reach the
            # driver and silently authenticate as a different identity than the configured token.
            params = {
                k: v for k, v in params.items() if k.lower() not in _TOKEN_INCOMPATIBLE_DSN_KEYS
            }
        return build_odbc_dsn(params)

    def to_odbc_attrs_before(self) -> dict[int, bytes] | None:
        """Return `attrs_before` with the pre-acquired `access_token`, or None."""
        return build_access_token_attrs_before(self)

    def to_odbc_token_provider(self) -> Optional[Any]:
        """Return the `azure_credential` mssql-python acquires tokens from, or None."""
        return select_token_provider(self)


@configspec
class MsSqlClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = dataclasses.field(default="mssql", init=False, repr=False, compare=False)  # type: ignore[misc]
    credentials: MsSqlCredentials = None

    create_indexes: bool = False
    has_case_sensitive_identifiers: bool = False

    def fingerprint(self) -> str:
        """Returns a fingerprint of the configured host."""
        if self.credentials and self.credentials.host:
            return digest128(self.credentials.host)
        return ""

    def data_location(self) -> str:
        """Returns host:port."""
        if not self.credentials or not self.credentials.host:
            self._no_data_location("the configuration has no host")
        port = self.credentials.port or 1433
        return f"{self.credentials.host}:{port}"
