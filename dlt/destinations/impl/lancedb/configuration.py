import dataclasses
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Final, List, Optional, Tuple

from dlt.common.configuration import configspec
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration.specs.base_configuration import (
    CredentialsConfiguration,
    NotResolved,
)
from dlt.common.destination.client import (
    DestinationClientConfiguration,
    DestinationClientDwhConfiguration,
)
from dlt.common.pendulum import timedelta
from dlt.common.schema import Schema
from dlt.common.typing import Annotated, TSecretStrValue
from dlt.common.utils import digest128
from dlt.destinations.impl.lance.configuration import LanceEmbeddingsConfiguration

if TYPE_CHECKING:
    from lancedb.remote.db import RemoteDBConnection

DEFAULT_FLIGHTSQL_PORT = 10025
PUBLIC_SCHEMA_NAME = "public"
"""Name the SQL endpoint gives to the root namespace."""


@configspec
class LanceDBCredentials(CredentialsConfiguration):
    """Credentials for a managed LanceDB Enterprise or Cloud cluster."""

    database: Optional[str] = None
    """Name of the database that holds every dataset, connected to as `db://<database>`.
    Leave empty to let each dataset be its own database"""
    api_key: TSecretStrValue = None
    """API key authenticating to the cluster."""
    host_override: Optional[str] = None
    """Cluster endpoint. Required for Enterprise, leave empty for LanceDB Cloud."""
    region: Optional[str] = "us-east-1"
    """Region of the LanceDB Cloud database."""
    flightsql_host: Optional[str] = None
    """Host of the Arrow Flight SQL endpoint, which Enterprise serves from a separate load balancer. Leave empty to disable SQL access."""
    flightsql_port: int = DEFAULT_FLIGHTSQL_PORT
    """Port of the Arrow Flight SQL endpoint."""
    flightsql_tls: bool = False
    """Whether the Arrow Flight SQL endpoint requires TLS."""
    headers: Optional[Dict[str, str]] = None
    """Extra HTTP headers sent to the cluster."""
    read_consistency_interval_seconds: float = 0.0
    """(Enterprise cluster does not honor this) How stale a read of the managed client can be, in seconds."""

    _conn: Annotated[Optional["RemoteDBConnection"], NotResolved()] = None
    _conns: Annotated[Optional[Dict[Tuple[str, float], "RemoteDBConnection"]], NotResolved()] = None

    __config_gen_annotations__: ClassVar[List[str]] = [
        "database",
        "api_key",
        "host_override",
        "flightsql_host",
    ]

    def parse_native_representation(self, native_value: Any) -> None:
        try:
            # database can be passed as an already connected managed client
            from lancedb.remote.db import RemoteDBConnection

            if isinstance(native_value, RemoteDBConnection):
                self._conn = native_value
                self.database = native_value.db_name
                return
        except ImportError:
            pass
        super().parse_native_representation(native_value)

    def on_resolved(self) -> None:
        if not (self.host_override or self.region):
            raise ConfigurationValueError(
                "LanceDB needs a cluster to connect to. Set"
                " `destination.lancedb.credentials.host_override` to the endpoint of your"
                " Enterprise cluster, or `region` for LanceDB Cloud."
            )

    def get_conn(
        self, database: str, read_consistency_interval_seconds: Optional[float] = None
    ) -> "RemoteDBConnection":
        """Returns a connection to `database` on the managed cluster, at the requested staleness.

        Args:
            database (str): Name of the database, which is the dataset being read or written.
            read_consistency_interval_seconds (Optional[float]): Overrides
                `read_consistency_interval_seconds`. Pass `0` to always read the latest version.

        Returns:
            RemoteDBConnection: Connection shared by all callers asking for the same database and
                staleness.
        """
        # an externally supplied connection is bound to its own database and consistency setting
        if self._conn is not None:
            return self._conn

        interval = (
            self.read_consistency_interval_seconds
            if read_consistency_interval_seconds is None
            else read_consistency_interval_seconds
        )
        if self._conns is None:
            self._conns = {}
        if (database, interval) not in self._conns:
            import lancedb

            self._conns[(database, interval)] = lancedb.connect(
                f"db://{database}",
                api_key=self.api_key,
                region=self.region,
                host_override=self.host_override,
                read_consistency_interval=timedelta(seconds=interval),
            )
        return self._conns[(database, interval)]

    def close_conn(self) -> None:
        self._conn = None
        self._conns = None

    @property
    def has_flightsql(self) -> bool:
        return bool(self.flightsql_host)

    def flightsql_uri(self) -> str:
        """Returns the gRPC URI of the Arrow Flight SQL endpoint."""
        host = self.flightsql_host
        tls = self.flightsql_tls
        # the endpoint is often configured as a URL, like the managed one in `host_override`
        if "://" in host:
            url_scheme, _, host = host.partition("://")
            tls = tls or url_scheme in ("https", "grpc+tls")
        host = host.rstrip("/")
        scheme = "grpc+tls" if tls else "grpc"
        return f"{scheme}://{host}:{self.flightsql_port}"

    def flightsql_headers(self, database: str) -> List[Tuple[bytes, bytes]]:
        """Returns call headers authenticating and routing Flight SQL requests to `database`."""
        headers: Dict[str, str] = dict(self.headers or {})
        if self.api_key:
            headers["authorization"] = f"Bearer {self.api_key}"
        if database:
            headers["database"] = database
        return [(name.lower().encode(), value.encode()) for name, value in headers.items()]


@configspec
class LanceDBClientConfiguration(DestinationClientDwhConfiguration):
    destination_type: Final[str] = dataclasses.field(  # type: ignore
        default="lancedb", init=False, repr=False, compare=False
    )
    credentials: LanceDBCredentials = None
    # dataset_name is optional: a configured `credentials.database` is the dataset when it is not set
    dataset_name: Final[Optional[str]] = dataclasses.field(  # type: ignore
        default=None, init=False, repr=False, compare=False
    )
    commit_tag: Optional[str] = None
    """Tag applied to every table of the dataset after a successful load, so the whole dataset can be read back as one tagged version."""

    embeddings: Optional[LanceEmbeddingsConfiguration] = None
    """Embeddings config. Adds a vector column when set."""

    dataset_sentinel_namespace_name: str = "_dlt_sentinel"
    """Namespace marking a dataset as created."""

    __config_gen_annotations__: ClassVar[List[str]] = ["commit_tag"]

    def on_resolved(self) -> None:
        if not (self.credentials.database or self.dataset_name):
            raise ConfigurationValueError(
                "LanceDB needs a dataset, which becomes the database holding its tables. Set"
                " `dataset_name` on the pipeline, or configure one database for all datasets with"
                " `destination.lancedb.credentials.database`."
            )

    def normalize_dataset_name(self, schema: Schema) -> str:
        """Returns the database where the schema tables of `schema` are materialized, which is the
        dataset itself.

        Raises:
            ConfigurationValueError: If `credentials.database` configures a database and
                `dataset_name` names a different one.
        """
        pinned_database = self.credentials.database if self.credentials else None
        if not pinned_database:
            return super().normalize_dataset_name(schema)
        # a configured database is the dataset, and holds every schema, so no schema suffix is added
        if self.dataset_name and schema.naming.normalize_table_identifier(
            self.dataset_name
        ) != schema.naming.normalize_table_identifier(pinned_database):
            raise ConfigurationValueError(
                f"Dataset `{self.dataset_name}` cannot be loaded because"
                f" `destination.lancedb.credentials.database` sets `{pinned_database}`, which is"
                " the dataset for this destination. Drop `database` from the credentials to give"
                " every dataset its own database, or leave `dataset_name` unset."
            )
        return pinned_database

    def fingerprint(self) -> str:
        """Returns a fingerprint of the cluster."""
        try:
            return digest128(self.data_location())
        except ConfigurationValueError:
            return ""

    def data_location(self) -> str:
        """Returns the cluster, which is where every dataset of this destination lives.

        The location excludes the database: one Arrow Flight SQL endpoint accesses every database of
        the cluster, so two datasets of one cluster must compare equal.
        """
        if not self.credentials:
            self._no_data_location("the config has no credentials")
        # an external client keeps its endpoint to itself, so the client object is the only identity
        if self.credentials._conn is not None:
            return f"lancedb-client:{hex(id(self.credentials._conn))}"
        # an Enterprise cluster is dedicated, so its endpoint identifies it
        if self.credentials.host_override:
            return f"lancedb:{self.credentials.host_override.rstrip('/')}"
        # Cloud shares a region between tenants, and the api key is the only tenant identity
        if self.credentials.api_key:
            return f"lancedb-cloud:{self.credentials.region}:{digest128(self.credentials.api_key)}"
        self._no_data_location("neither `host_override` nor `api_key` is set")

    def can_write_from(self, other: DestinationClientConfiguration) -> bool:
        """LanceDB has no engine that can execute SQL or run a model job, so `dlt` is that engine
        and materializes eagerly.
        """
        return False
