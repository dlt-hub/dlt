# from __future__ import annotations

import dataclasses
from typing import ClassVar, Union

from dlt.common.configuration import configspec
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.specs.connection_string_credentials import ConnectionStringCredentials
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.storages.configuration import (
    FilesystemConfiguration,
    FilesystemConfigurationWithLocalFiles,
    WithLocalFiles,
)
from dlt.common.utils import digest128
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
    catalog: ConnectionStringCredentials = None
    # NOTE: consider moving to DuckLakeClientConfiguration so bucket_url is not a secret
    storage: FilesystemConfiguration = None

    __config_gen_annotations__: ClassVar[list[str]] = ["ducklake_name", "catalog", "storage"]

    def __init__(
        self,
        ducklake_name: str = DEFAULT_DUCKLAKE_NAME,
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
        if self.extensions:
            self.extensions = list(set([*self.extensions, "ducklake"]))
        else:
            self.extensions = ["ducklake"]
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


# TODO add connection to a specific snapshot
@configspec
class DuckLakeClientConfiguration(WithLocalFiles, DestinationClientDwhWithStagingConfiguration):
    destination_type: str = dataclasses.field(
        default="ducklake",
        init=False,
        repr=False,
        compare=False,
    )
    credentials: DuckLakeCredentials = None
    create_indexes: bool = False  # does nothing but required

    def fingerprint(self) -> str:
        """Use fingerprint of underlying storage. This is precise to bucket level"""
        if self.credentials.storage is None:
            return ""
        return self.credentials.storage.fingerprint()

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
