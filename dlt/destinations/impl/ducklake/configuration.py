# from __future__ import annotations

import dataclasses
from typing import ClassVar, Optional, Union

from dlt.common.configuration import configspec
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.specs.connection_string_credentials import ConnectionStringCredentials
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.utils import digest128
from dlt.common.destination.client import (
    DestinationClientConfiguration,
    DestinationClientDwhWithStagingConfiguration,
    WithAttachableEngine,
)
from dlt.common.storages.configuration import (
    FilesystemConfiguration,
    FilesystemConfigurationWithLocalFiles,
    WithLocalFiles,
)
from dlt.destinations.impl.duckdb.configuration import DuckDbConnectionPool, DuckDbBaseCredentials
from dlt.destinations.impl.duckdb.factory import _set_duckdb_raw_capabilities


DEFAULT_DUCKLAKE_NAME = "ducklake"
DUCKLAKE_STORAGE_PATTERN = "%s.files"


def _get_ducklake_capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps = _set_duckdb_raw_capabilities(caps)
    # load with parquet by default
    caps.preferred_loader_file_format = "parquet"
    # duckdb and sqllite will crash when loading in parallel, see adjust_capabilities in factory
    caps.loader_parallelism_strategy = "sequential"
    return caps


@configspec(init=False)
class DuckLakeCredentials(DuckDbBaseCredentials):
    ducklake_name: str = DEFAULT_DUCKLAKE_NAME
    metadata_schema: Optional[str] = None
    catalog: ConnectionStringCredentials = None
    # NOTE: consider moving to DuckLakeClientConfiguration so bucket_url is not a secret
    storage: FilesystemConfiguration = None

    __config_gen_annotations__: ClassVar[list[str]] = [
        "ducklake_name",
        "metadata_schema",
        "catalog",
        "storage",
    ]

    def __init__(
        self,
        ducklake_name: str = DEFAULT_DUCKLAKE_NAME,
        metadata_schema: Optional[str] = None,
        catalog: Union[str, ConnectionStringCredentials] = None,
        storage: Union[str, FilesystemConfiguration] = None,
    ) -> None:
        """Initialize DuckLake credentials by passing ducklake name, catalog and storage
        configuration.

        Args:
            ducklake_name: str
                This value is mainly used as ATTACH name for the ducklake database and
                as names for catalog and storage files if not configured explicitly.
                If omitted, ducklake name is derived from destination name or pipeline name.
            metadata_schema: str, optional
                Metadata schema to use for SQL-based catalogs. If omitted, defaults to
                `ducklake_name`.
            catalog: Either a connection string (for example,
                "sqlite:///catalog.sqlite", "duckdb:///catalog.duckdb",
                or "postgres://loader:loader@localhost:5432/dlt_data") or a
                ConnectionStringCredentials instance. If omitted,
                will default the catalog to a local sqlite database whose filename is
                derived from the name_or_conn_str argument.
            storage: Either a storage URL string (for example,
                "file://...", "s3://bucket/prefix") or a FilesystemConfiguration
                instance. If omitted, it will create a folder for data with name
                derived from the name_or_conn_str argument.

        """
        self.ducklake_name = ducklake_name
        self.metadata_schema = metadata_schema
        if isinstance(catalog, str):
            catalog = ConnectionStringCredentials(catalog)
        self.catalog = catalog
        if isinstance(storage, str):
            storage = FilesystemConfigurationWithLocalFiles(bucket_url=storage)
        self.storage = storage

    def _conn_str(self) -> str:
        return ":memory:"

    def on_partial(self) -> None:
        # this works only if wired to right exception type
        config_exception = self.__exception__
        if not isinstance(config_exception, ConfigFieldMissingException):
            return
        # set default catalog only if not present in config, partially resolved should generate exception
        if self.catalog is None and not config_exception.was_partially_resolved("catalog"):
            # use sqllite as default catalog
            self.catalog = ConnectionStringCredentials(
                {"drivername": "sqlite", "database": self.ducklake_name + ".sqlite"}
            ).resolve()
            config_exception.drop_traces_for_field("catalog")

        if self.storage is None and "bucket_url" in config_exception.traces["storage"][0].traces:  # type: ignore
            self.storage = FilesystemConfigurationWithLocalFiles(
                bucket_url=DUCKLAKE_STORAGE_PATTERN % self.ducklake_name, local_dir="."
            ).resolve()

        if not self.is_partial():
            self.resolve()

    def on_resolved(self) -> None:
        # the attach statement that names `ducklake` autoloads the extension. this pool opens a
        # connection per borrow, so an explicit `INSTALL` pays a repository check every time
        # set connection pool so it always opens a new connection on borrow.
        # connection duplication for parallelism does not work for ducklake.
        self.conn_pool = DuckDbConnectionPool(self, always_open_connection=True)

    @property
    def storage_url(self) -> str:
        """Convert file:// url into native os path so duckdb can read it"""
        if self.storage.is_local_filesystem:
            return self.storage.make_local_path(self.storage.bucket_url)
        else:
            return self.storage.bucket_url

    def catalog_location(self) -> str:
        """Returns a non-secret catalog identity which locates the ducklake. When the catalog has
        no credential-free identity, this method digests the catalog. Two lakes then never get
        the same identity."""
        if not self.catalog:
            return ""

        drivername = self.catalog.drivername or ""
        # attach statement converts `postgresql` to duckdb-known `postgres`
        if drivername == "postgresql":
            drivername = "postgres"

        metadata_schema = self.metadata_schema or self.ducklake_name or DEFAULT_DUCKLAKE_NAME

        # file catalogs: the database file is the lake, attach name is just an alias
        if drivername in ("duckdb", "sqlite"):
            if self.catalog.database:
                return f"{drivername}://{self.catalog.database}"
            return ""

        # sql catalogs host one lake per metadata schema which defaults to ducklake name
        if self.catalog.host and self.catalog.database:
            # NOTE: ports must be specified (or not) consistently across configs to match
            port_str = f":{self.catalog.port}" if self.catalog.port else ""
            return (
                f"{drivername}://{self.catalog.host}{port_str}/{self.catalog.database}"
                f"#{metadata_schema}"
            )

        # a MotherDuck-hosted catalog carries its account in the token, so this method digests
        # the whole native representation and keeps the identity non-secret
        native = str(self.catalog.to_native_representation())
        return f"{drivername}://{digest128(f'{native}#{metadata_schema}')}"


# TODO add connection to a specific snapshot
@configspec
class DuckLakeClientConfiguration(
    WithAttachableEngine, WithLocalFiles, DestinationClientDwhWithStagingConfiguration
):
    destination_type: str = dataclasses.field(
        default="ducklake",
        init=False,
        repr=False,
        compare=False,
    )
    credentials: DuckLakeCredentials = None
    create_indexes: bool = False  # does nothing but required
    override_data_path: bool = False
    automatic_migration: bool = False
    """When true, attaches with `AUTOMATIC_MIGRATION true` so DuckDB migrates an older DuckLake catalog schema on attach."""

    def fingerprint(self) -> str:
        """Returns a fingerprint of the underlying storage."""
        if not self.credentials or self.credentials.storage is None:
            return ""
        return self.credentials.storage.fingerprint()

    def data_location(self) -> str:
        """Returns the catalog identity which locates the ducklake."""
        if not self.credentials or not (location := self.credentials.catalog_location()):
            self._no_data_location("the configuration has no ducklake catalog")
        return location

    def needs_attach(self, other: DestinationClientConfiguration) -> bool:
        """Returns False within one lake. The query engine already accesses every schema of that
        lake."""
        return not self.is_same_location(other)

    def on_resolved(self) -> None:
        # redirect local catalog database file to `local_dir`
        if self.credentials.catalog.drivername in ("duckdb", "sqlite"):
            # name is <pipeline|dest name>.<duckdb|sqlite>
            local_db = self.make_location(
                self.credentials.catalog.database
                or self.credentials.ducklake_name + "." + self.credentials.catalog.drivername,
                "%s",
            )
            self.credentials.catalog.database = local_db

        # redirect storage to local filesystem
        if isinstance(self.credentials.storage, WithLocalFiles):
            self.credentials.storage.attach_from(self)
            if not self.credentials.storage.is_resolved():
                self.credentials.storage.resolve()
            else:
                self.credentials.storage.normalize_bucket_url()

    def __str__(self) -> str:
        """Return ducklake displayable location that contains catalog and storage locations"""
        if not self.credentials or not self.credentials.catalog or not self.credentials.storage:
            return ""
        return f"{self.credentials.ducklake_name}@{self.credentials.catalog}@{self.credentials.storage}"
