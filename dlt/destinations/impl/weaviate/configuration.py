import dataclasses
from typing import Any, ClassVar, Dict, Final, List, Literal, Optional
from typing_extensions import Annotated
from urllib.parse import urlparse

from dlt.common.configuration import configspec, NotResolved
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration
from dlt.common.destination.client import (
    DestinationClientConfiguration,
    DestinationClientDwhConfiguration,
)
from dlt.common.utils import digest128
from dlt.version import __version__

from dlt.destinations.impl.weaviate.warnings import (
    batch_option_without_v4_equivalent_deprecated,
)

TWeaviateBatchConsistency = Literal["ONE", "QUORUM", "ALL"]
TWeaviateConnectionType = Literal["cloud", "local", "custom"]
TWeaviateBatchMode = Literal["auto", "stream", "dynamic", "fixed_size", "rate_limit"]

WEAVIATE_CLIENT_INTEGRATION_HEADER = "X-Weaviate-Client-Integration"
WEAVIATE_CLIENT_INTEGRATION = f"dlt/{__version__}"

# Server-side (stream) batching is only available from this Weaviate server version.
WEAVIATE_SERVER_SIDE_BATCH_MIN_VERSION = "1.36.0"

# `batch.stream` is GA from this weaviate-client version;
# earlier ones only ship `batch.experimental`.
WEAVIATE_CLIENT_MIN_VERSION = "4.20.0"

DEFAULT_HTTP_PORT = 8080
DEFAULT_GRPC_PORT = 50051


@configspec
class WeaviateCredentials(CredentialsConfiguration):
    url: str = "http://localhost:8080"
    """REST endpoint of the cluster. The scheme decides `http_secure` unless it is set."""
    api_key: Optional[str] = None
    additional_headers: Optional[Dict[str, str]] = None
    """Extra headers, e.g. a vectorizer API key. Overrides dlt's own headers on conflict."""

    http_port: Optional[int] = None
    """REST port. Required for `custom`, defaults to 8080 for `local`."""
    grpc_port: Optional[int] = None
    """gRPC port. Required for `custom`, defaults to 50051 for `local`."""
    grpc_host: Optional[str] = None
    """gRPC host when it differs from the REST host. Requires `connection_type="custom"`."""
    http_secure: Optional[bool] = None
    """Use TLS for REST. Defaults to whether `url` is https."""
    grpc_secure: Optional[bool] = None
    """Use TLS for gRPC. Defaults to `http_secure`."""

    def __str__(self) -> str:
        """Used to display user friendly data location"""
        # assuming no password in url scheme for Weaviate
        return self.url

    def host(self) -> str:
        """Returns the hostname of `url`, without scheme or port."""
        parsed = urlparse(self.url if "//" in self.url else f"//{self.url}")
        return parsed.hostname or ""

    def is_secure(self) -> bool:
        return self.http_secure if self.http_secure is not None else self.url.startswith("https")

    def is_grpc_secure(self) -> bool:
        return self.grpc_secure if self.grpc_secure is not None else self.is_secure()

    def resolved_grpc_host(self) -> str:
        return self.grpc_host or self.host()

    def integration_headers(self) -> Dict[str, str]:
        """Headers identifying dlt to Weaviate. Explicit `additional_headers` win on conflict."""
        return {
            WEAVIATE_CLIENT_INTEGRATION_HEADER: WEAVIATE_CLIENT_INTEGRATION,
            **(self.additional_headers or {}),
        }


@configspec
class WeaviateClientConfiguration(DestinationClientDwhConfiguration):
    destination_type: Final[str] = dataclasses.field(default="weaviate", init=False, repr=False, compare=False)  # type: ignore[misc]
    # make it optional so empty dataset is allowed
    dataset_name: Annotated[Optional[str], NotResolved()] = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )

    batch_mode: TWeaviateBatchMode = "auto"
    """How objects are batched. `auto` uses server-side batching when the server supports it."""
    batch_size: int = 100
    """Objects per request. Applies to `fixed_size` only."""
    batch_workers: int = 1
    """Concurrent requests. Applies to `fixed_size` and `stream`."""
    batch_requests_per_minute: int = 600
    """Request budget per minute. Applies to `rate_limit` only."""
    batch_consistency: TWeaviateBatchConsistency = "ONE"
    """Replica nodes that must acknowledge a write before it is considered successful."""

    batch_retries: int = 5
    """Deprecated, has no effect. The v4 client retries internally."""
    startup_period: int = 5
    """Deprecated, has no effect. Removed in the v4 client."""

    conn_timeout: float = 10.0
    """Seconds to wait when establishing a connection. `init_timeout` overrides it."""
    read_timeout: float = 3 * 60.0
    """Seconds to wait for queries and inserts. `query_timeout`/`insert_timeout` override it."""
    init_timeout: Optional[float] = None
    """Seconds for the connection handshake. Defaults to `conn_timeout`."""
    query_timeout: Optional[float] = None
    """Seconds for queries. Defaults to `read_timeout`."""
    insert_timeout: Optional[float] = None
    """Seconds for inserts. Defaults to `read_timeout`."""
    stream_timeout: Optional[float] = None
    """Seconds for a server-side batch stream. Defaults to the client's own default."""

    skip_init_checks: Optional[bool] = None
    """Skip the startup handshake. Defaults to `True` for `cloud` and `custom`, `False` for `local`."""
    proxies: Optional[Dict[str, str]] = None
    """Proxies passed to the client, e.g. `{"http": "...", "https": "..."}`."""
    trust_env: bool = False
    """Let the client read proxy settings from the environment."""

    session_pool_connections: Optional[int] = None
    """REST connection pool size. Defaults to the client's own default."""
    session_pool_maxsize: Optional[int] = None
    """Maximum REST connections kept in the pool."""
    session_pool_max_retries: Optional[int] = None
    """Retries the REST session performs on connection errors."""
    session_pool_timeout: Optional[int] = None
    """Seconds to wait for a free connection from the pool."""

    dataset_separator: str = "_"

    # Connection type: "cloud" for Weaviate Cloud, "local" for Docker, "custom" for self-hosted
    # If None, auto-detected from URL pattern
    connection_type: Optional[TWeaviateConnectionType] = None

    credentials: WeaviateCredentials = None
    vectorizer: str = "text2vec-openai"
    module_config: Dict[str, Dict[str, str]] = dataclasses.field(
        default_factory=lambda: {
            "text2vec-openai": {
                "model": "ada",
                "modelVersion": "002",
                "type": "text",
            }
        }
    )

    __config_gen_annotations__: ClassVar[List[str]] = [
        "batch_mode",
        "batch_size",
        "batch_consistency",
        "connection_type",
        "vectorizer",
    ]

    def on_resolved(self) -> None:
        if self.batch_retries != 5:
            batch_option_without_v4_equivalent_deprecated("batch_retries")
        if self.startup_period != 5:
            batch_option_without_v4_equivalent_deprecated("startup_period")

    def fingerprint(self) -> str:
        """Returns a fingerprint of the connection host."""
        if self.credentials and self.credentials.url:
            hostname = urlparse(self.credentials.url).hostname
            if hostname:
                return digest128(hostname)
        return ""

    def data_location(self) -> str:
        """Returns the host part of the connection URL."""
        hostname = urlparse(self.credentials.url).hostname if self.credentials else None
        if not hostname:
            self._no_data_location("the connection URL identifies no host")
        return hostname

    def can_read_from(self, other: DestinationClientConfiguration) -> bool:
        """Weaviate does not support dlt SQL joins."""
        return False

    def can_write_from(self, other: DestinationClientConfiguration) -> bool:
        """Weaviate does not support dlt SQL joins."""
        return False

    def resolve_connection_type(self) -> TWeaviateConnectionType:
        """Returns `connection_type`, inferring it from the URL when not set explicitly."""
        if self.connection_type is not None:
            return self.connection_type
        url = self.credentials.url
        if ".weaviate.cloud" in url or ".wcs.api.weaviate.io" in url:
            return "cloud"
        if "localhost" in url or "127.0.0.1" in url:
            return "local"
        return "custom"

    def _additional_config(self) -> Any:
        """Builds the client's `AdditionalConfig` from the timeout, proxy and pool options."""
        from weaviate.config import AdditionalConfig, ConnectionConfig, Timeout

        timeout = Timeout(
            init=self.init_timeout if self.init_timeout is not None else self.conn_timeout,
            query=self.query_timeout if self.query_timeout is not None else self.read_timeout,
            insert=self.insert_timeout if self.insert_timeout is not None else self.read_timeout,
            stream=self.stream_timeout,
        )
        pool = {
            "session_pool_connections": self.session_pool_connections,
            "session_pool_maxsize": self.session_pool_maxsize,
            "session_pool_max_retries": self.session_pool_max_retries,
            "session_pool_timeout": self.session_pool_timeout,
        }
        pool = {name: value for name, value in pool.items() if value is not None}
        config: Dict[str, Any] = {
            "timeout": timeout,
            "proxies": self.proxies,
            "trust_env": self.trust_env,
        }
        # leaving `connection` out keeps the client's own pool defaults
        if pool:
            config["connection"] = ConnectionConfig(**pool)
        return AdditionalConfig(**config)

    def _skip_init_checks(self, connection_type: TWeaviateConnectionType) -> bool:
        if self.skip_init_checks is not None:
            return self.skip_init_checks
        return connection_type != "local"

    def to_connector_params(self) -> Dict[str, Any]:
        """Builds the kwargs for the `weaviate.connect_to_*` helper matching the connection type."""
        # imported here so that capabilities() works without the weaviate extra installed
        from weaviate.auth import AuthApiKey

        credentials = self.credentials
        connection_type = self.resolve_connection_type()
        headers = credentials.integration_headers()
        auth_credentials = AuthApiKey(credentials.api_key) if credentials.api_key else None
        additional_config = self._additional_config()
        skip_init_checks = self._skip_init_checks(connection_type)

        if credentials.grpc_host and connection_type != "custom":
            raise ConfigurationValueError(
                "grpc_host",
                "`grpc_host` is only supported by connection_type 'custom', not"
                f" '{connection_type}'. The Weaviate client takes a separate gRPC host only on"
                ' `connect_to_custom`. Set `connection_type = "custom"` together with'
                " `http_port` and `grpc_port`.",
            )

        if connection_type == "cloud":
            url = credentials.url
            return {
                "cluster_url": url if url.startswith("https://") else f"https://{url}",
                "auth_credentials": auth_credentials,
                "headers": headers,
                "additional_config": additional_config,
                "skip_init_checks": skip_init_checks,
            }

        if connection_type == "local":
            return {
                "host": credentials.host(),
                "port": credentials.http_port or DEFAULT_HTTP_PORT,
                "grpc_port": credentials.grpc_port or DEFAULT_GRPC_PORT,
                "headers": headers,
                "auth_credentials": auth_credentials,
                "additional_config": additional_config,
                "skip_init_checks": skip_init_checks,
            }

        if credentials.http_port is None or credentials.grpc_port is None:
            raise ConfigurationValueError(
                "http_port and grpc_port",
                "http_port and grpc_port are required when connection_type is 'custom'. "
                "Set them in [destination.weaviate.credentials] or use connection_type='local' "
                f"for default ports (http: {DEFAULT_HTTP_PORT}, grpc: {DEFAULT_GRPC_PORT}).",
            )
        return {
            "http_host": credentials.host(),
            "http_port": credentials.http_port,
            "http_secure": credentials.is_secure(),
            "grpc_host": credentials.resolved_grpc_host(),
            "grpc_port": credentials.grpc_port,
            "grpc_secure": credentials.is_grpc_secure(),
            "auth_credentials": auth_credentials,
            "headers": headers,
            "additional_config": additional_config,
            "skip_init_checks": skip_init_checks,
        }
