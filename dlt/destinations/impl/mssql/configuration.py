import dataclasses
import struct
from typing import ClassVar, Any, Final, List, Dict, Optional, TYPE_CHECKING

from dlt import version
from dlt.common.configuration import configspec, NotResolved
from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.configuration.specs import ConnectionStringCredentials, CredentialsWithDefault
from dlt.common.typing import TSecretStrValue, Annotated
from dlt.common.exceptions import MissingDependencyException, SystemConfigurationException

from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.utils import digest128

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential

_AZURE_AUTH_EXTRA = f"{version.DLT_PKG_NAME}[az]"

# pyodbc connection attribute used to inject a pre-acquired Entra ID access token.
# https://learn.microsoft.com/sql/connect/odbc/using-azure-active-directory#authenticating-with-an-access-token
SQL_COPT_SS_ACCESS_TOKEN = 1256
SQL_TOKEN_SCOPE = "https://database.windows.net/.default"

# Authentication methods the ODBC driver performs natively (passed as `Authentication=` in the
# DSN). Poolable: the driver signs in itself, so no token needs to be reacquired and injected
# per connection.
DRIVER_NATIVE_AUTHENTICATION = frozenset(
    {
        "ActiveDirectoryServicePrincipal",
        "ActiveDirectoryPassword",
        "ActiveDirectoryIntegrated",
        "ActiveDirectoryInteractive",
        "ActiveDirectoryMsi",
    }
)

# Authentication methods pyodbc/msodbcsql does not support natively. dlt acquires the access
# token itself with azure-identity and injects it into the connection (`attrs_before`), so these
# work cross-platform but are not poolable.
AZURE_IDENTITY_INJECTION_AUTHENTICATION = frozenset(
    {
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


def create_token_credential(authentication: str) -> "TokenCredential":
    """Create an azure-identity credential for a token-injection authentication method.

    Expects the canonical name (`ActiveDirectoryDefault` or `ActiveDirectoryDeviceCode`), as
    already resolved by `_normalize_authentication`.
    """
    try:
        from azure.identity import DefaultAzureCredential, DeviceCodeCredential
    except ModuleNotFoundError:
        raise MissingDependencyException("MsSqlCredentials", [_AZURE_AUTH_EXTRA])

    credential_factories = {
        "ActiveDirectoryDefault": DefaultAzureCredential,
        "ActiveDirectoryDeviceCode": DeviceCodeCredential,
    }
    return credential_factories[authentication]()  # type: ignore[no-any-return]


def uses_token_authentication(credentials: Any) -> bool:
    """True when dlt must acquire an access token and inject it into the connection.

    `access_token`/`azure_credential` take precedence over `authentication`: when either is
    set, dlt injects that token directly regardless of the configured method. Otherwise derived
    from the configured method, not from `has_default_credentials`: cooperative `on_partial`
    calls up the MRO may set a default credential even for driver-native methods.
    """
    if credentials.access_token or credentials.azure_credential:
        return True
    authentication = credentials.authentication
    if not authentication:
        return False
    if _normalize_authentication(authentication) in AZURE_IDENTITY_INJECTION_AUTHENTICATION:
        return True
    # ActiveDirectoryServicePrincipal without a secret cannot authenticate through the ODBC
    # driver, so dlt falls back to a DefaultAzureCredential token.
    return authentication == "ActiveDirectoryServicePrincipal" and not (
        credentials.azure_client_id
        and credentials.azure_client_secret
        and credentials.azure_tenant_id
    )


def setup_token_credential(credentials: Any) -> None:
    """Create and store the azure-identity credential for token-injection authentication."""
    if credentials.access_token or credentials.azure_credential:
        # The token (or a credential able to fetch one) was injected directly: no
        # DefaultAzureCredential fallback is needed.
        return
    authentication = credentials.authentication or ""
    normalized = _normalize_authentication(authentication)
    if normalized in AZURE_IDENTITY_INJECTION_AUTHENTICATION:
        credentials._set_default_credentials(create_token_credential(normalized))
    elif authentication == "ActiveDirectoryServicePrincipal" and not (
        credentials.azure_client_id
        and credentials.azure_client_secret
        and credentials.azure_tenant_id
    ):
        credentials._set_default_credentials(create_token_credential("ActiveDirectoryDefault"))


def get_token_credential(credentials: Any) -> "TokenCredential":
    """Return the azure-identity credential used for token authentication.

    Reuses the credential created during resolution (so azure-identity can cache tokens
    across connections) and creates one on demand otherwise.
    """
    if credentials.has_default_credentials():
        return credentials.default_credentials()  # type: ignore[no-any-return]
    authentication = _normalize_authentication(credentials.authentication or "")
    if authentication not in AZURE_IDENTITY_INJECTION_AUTHENTICATION:
        authentication = "ActiveDirectoryDefault"
    return create_token_credential(authentication)


def get_access_token(credentials: Any) -> Optional[str]:
    """Return a directly usable Entra ID access token, bypassing `authentication` resolution.

    Prefers a pre-fetched `access_token`, then a token fetched from an injected
    `azure_credential`. Returns None when neither is set, so callers fall back to the
    `authentication` named-method resolution.
    """
    if credentials.access_token:
        return str(credentials.access_token)
    if credentials.azure_credential:
        return credentials.azure_credential.get_token(SQL_TOKEN_SCOPE).token  # type: ignore[no-any-return]
    return None


def build_token_attrs_before(credentials: Any) -> dict[int, bytes] | None:
    """Return pyodbc `attrs_before` with an Entra ID access token, or None for driver-native auth."""
    token = get_access_token(credentials)
    if token is None:
        if not uses_token_authentication(credentials):
            return None
        token = get_token_credential(credentials).get_token(SQL_TOKEN_SCOPE).token
    encoded_token = token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(encoded_token)}s", len(encoded_token), encoded_token)
    return {SQL_COPT_SS_ACCESS_TOKEN: token_struct}


def validate_authentication(credentials: Any) -> None:
    """Validate the configured authentication method."""
    if credentials.access_token or credentials.azure_credential:
        # A token (or a credential able to fetch one) was injected directly: it takes
        # precedence over `authentication`, whose value does not need to be validated.
        return
    authentication = credentials.authentication
    if not authentication:
        return  # plain SQL login (username/password)
    normalized = _normalize_authentication(authentication)
    if (
        normalized not in DRIVER_NATIVE_AUTHENTICATION
        and normalized not in AZURE_IDENTITY_INJECTION_AUTHENTICATION
    ):
        supported = sorted(DRIVER_NATIVE_AUTHENTICATION) + sorted(
            AZURE_IDENTITY_INJECTION_AUTHENTICATION
        )
        raise ConfigurationException(
            f"Unsupported `authentication` method `{authentication}`."
            f" Supported methods: {', '.join(supported)}."
        )
    if authentication == "ActiveDirectoryPassword" and not (
        credentials.username and credentials.password
    ):
        raise ConfigurationException(
            "`authentication = ActiveDirectoryPassword` requires `username` and `password`."
        )


def apply_authentication_to_dsn(credentials: Any, params: dict[str, Any]) -> None:
    """Add UID/PWD/Authentication keys to an ODBC DSN dict based on the authentication method."""
    if uses_token_authentication(credentials):
        # The access token is injected via attrs_before, so the DSN carries no credentials.
        return
    authentication = credentials.authentication
    if not authentication:
        # Plain SQL login.
        params["UID"] = credentials.username
        params["PWD"] = credentials.password
        return
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
    driver: str = None

    authentication: str | None = None
    """Authentication method. Empty (default) uses plain SQL login (`username`/`password`).
    Driver-native (passed as `Authentication=` in the DSN, poolable):
    `ActiveDirectoryServicePrincipal`, `ActiveDirectoryPassword`, `ActiveDirectoryIntegrated`,
    `ActiveDirectoryInteractive`, `ActiveDirectoryMsi`. azure-identity (token acquired and
    injected by dlt, not poolable): `ActiveDirectoryDefault` (alias `default`),
    `ActiveDirectoryDeviceCode`."""

    azure_tenant_id: str | None = None
    """Entra ID tenant id, used with `ActiveDirectoryServicePrincipal` authentication."""

    azure_client_id: str | None = None
    """Service Principal client id, used with `ActiveDirectoryServicePrincipal` authentication."""

    azure_client_secret: TSecretStrValue | None = None
    """Service Principal client secret, used with `ActiveDirectoryServicePrincipal` authentication."""

    access_token: Optional[TSecretStrValue] = None
    """Pre-acquired Entra ID access token, injected as-is. Takes precedence over `azure_credential` and `authentication`"""

    azure_credential: Annotated[Optional[Any], NotResolved()] = None
    """A `TokenCredential` injected at runtime, e.g. `DefaultAzureCredential()`. Takes precedence over `authentication` but not over `access_token`"""

    __config_gen_annotations__: ClassVar[List[str]] = ["port", "connect_timeout"]

    SUPPORTED_DRIVERS: ClassVar[List[str]] = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
    ]

    def parse_native_representation(self, native_value: Any) -> None:
        # TODO: Support ODBC connection string or sqlalchemy URL
        super().parse_native_representation(native_value)
        if self.query is not None:
            self.query = {k.lower(): v for k, v in self.query.items()}  # Make case-insensitive.
        self.driver = self.query.get("driver", self.driver)
        self.connect_timeout = int(self.query.get("connect_timeout", self.connect_timeout))

    def on_resolved(self) -> None:
        validate_authentication(self)
        if self.driver not in self.SUPPORTED_DRIVERS:
            raise SystemConfigurationException(
                f"The specified driver `{self.driver}` is not supported."
                f" Choose one of the supported drivers: {', '.join(self.SUPPORTED_DRIVERS)}."
            )
        self.database = self.database.lower()

    def get_query(self) -> Dict[str, Any]:
        query = dict(super().get_query())
        query["connect_timeout"] = self.connect_timeout
        return query

    def on_partial(self) -> None:
        self.driver = self._get_driver()
        setup_token_credential(self)
        if self.authentication or self.access_token or self.azure_credential:
            # Entra ID methods (token or driver-native) supply their own credentials and do not
            # rely on username/password; resolve once we have a target. `on_resolved` validates.
            if self.host and self.database:
                self.resolve()
        elif not self.is_partial():
            # Plain SQL login needs username/password.
            self.resolve()

    def _get_driver(self) -> str:
        if self.driver:
            return self.driver

        # Pick a default driver if available
        import pyodbc

        available_drivers = pyodbc.drivers()
        for d in self.SUPPORTED_DRIVERS:
            if d in available_drivers:
                return d
        docs_url = "https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16"
        raise SystemConfigurationException(
            f"No supported ODBC driver found for MS SQL Server.  See {docs_url} for information on"
            f" how to install the `{self.SUPPORTED_DRIVERS[0]}` on your platform."
        )

    def get_odbc_dsn_dict(self) -> Dict[str, Any]:
        params: dict[str, Any] = {
            "DRIVER": self.driver,
            "SERVER": f"{self.host},{self.port}",
            "DATABASE": self.database,
        }
        apply_authentication_to_dsn(self, params)
        if self.query is not None:
            params.update({k.upper(): v for k, v in self.query.items()})
        return params

    def to_odbc_dsn(self) -> str:
        params = self.get_odbc_dsn_dict()
        return build_odbc_dsn(params)

    def to_odbc_attrs_before(self) -> dict[int, bytes] | None:
        """Return pyodbc `attrs_before` with an Entra ID access token, or None for driver-native auth."""
        return build_token_attrs_before(self)


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
