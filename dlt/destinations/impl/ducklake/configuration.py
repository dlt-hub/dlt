# from __future__ import annotations

import dataclasses
from typing import Any, Dict, Final, List, Optional, TYPE_CHECKING, Union

from dlt.common.configuration import configspec
from dlt.common.configuration.specs.connection_string_credentials import ConnectionStringCredentials
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.storages.configuration import (
    FilesystemConfiguration,
    FilesystemConfigurationWithLocalFiles,
    WithLocalFiles,
)
from dlt.common.utils import digest128
from dlt.destinations.impl.duckdb.configuration import DuckDbConnectionPool, DuckDbCredentials
from dlt.destinations.impl.duckdb.factory import _set_duckdb_raw_capabilities

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection
else:
    DuckDBPyConnection = Any  # type: ignore[assignment,misc]


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
class DuckLakeCredentials(DuckDbCredentials):
    """
    For DuckLakeCredentials, the field `database` refers to the name
    of the DuckLake.
    """

    drivername: Final[str] = dataclasses.field(  # type: ignore[misc]
        default="ducklake", init=False, repr=False, compare=False
    )

    catalog: Optional[ConnectionStringCredentials] = None
    # NOTE: consider moving to DuckLakeClientConfiguration so bucket_url is not a secret
    storage: Optional[FilesystemConfiguration] = None

    def __init__(
        self,
        name_or_conn_str: str = None,
        catalog: Union[str, ConnectionStringCredentials] = None,
        storage: Union[str, FilesystemConfiguration] = None,
    ) -> None:
        """Initialize DuckLake credentials by passing ducklake name, catalog and storage
        configuration.

        Args:
            name_or_conn_str: DuckLake name or URI. Accepted forms include:
                - "ducklake:my_ducklake" (normalized to "ducklake:///my_ducklake")
                - "ducklake:///my_ducklake"
                - "my_ducklake" (just a name)
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
        self._apply_init_value(name_or_conn_str)
        if isinstance(catalog, str):
            catalog = ConnectionStringCredentials(catalog)
        self.catalog = catalog
        if isinstance(storage, str):
            storage = FilesystemConfigurationWithLocalFiles(bucket_url=storage)
        self.storage = storage

    def _conn_str(self) -> str:
        return ":memory:"

    def on_resolved(self) -> None:
        # use local duckdb
        if self.catalog is None:
            # use sqllite as default catalog
            self.catalog = ConnectionStringCredentials({"drivername": "sqlite"})
        # if self.storage is None:
        #     # create data in run_dir
        #     self.storage = FilesystemConfigurationWithLocalFiles(bucket_url=".")

        if self.extensions:
            self.extensions = list(set([*self.extensions, "ducklake"]))
        else:
            self.extensions = ["ducklake"]
        # set connection pool so it always opens a new connection on borrow.
        # connection duplication for parallelism does not work for ducklake.
        self.conn_pool = DuckDbConnectionPool(self, always_open_connection=True)

    @property
    def ducklake_name(self) -> str:
        # database is ducklake name ie. ducklake:my_ducklake
        return self.database

    @property
    def storage_url(self) -> str:
        """Convert file:// url into native os path so duckdb can read it"""
        if self.storage.is_local_filesystem:
            return self.storage.make_local_path(self.storage.bucket_url)
        else:
            return self.storage.bucket_url

    def parse_native_representation(self, native_value: Any) -> None:
        # allow to set ducklake name directly
        if isinstance(native_value, str):
            if native_value.startswith("ducklake:") and not native_value.startswith("ducklake:/"):
                native_value = "ducklake:///" + native_value[9:]
        super().parse_native_representation(native_value)


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
        return digest128(self.__str__())

    def on_resolved(self) -> None:
        # if database was not provided, pick a default location "ducklake"
        if not self.credentials.database:
            self.credentials.database = self.make_default_location("%s")
        # redirect local catalog database file to `local_dir`
        if self.credentials.catalog.drivername in ("duckdb", "sqlite"):
            # name is <pipeline|dest name>.<duckdb|sqlite>
            local_db = self.make_location(
                self.credentials.catalog.database, "%s." + self.credentials.catalog.drivername
            )
            self.credentials.catalog.database = local_db

        # redirect compliant storage to local filesystem
        if not self.credentials.storage:
            self.credentials.storage = FilesystemConfigurationWithLocalFiles(
                bucket_url=DUCKLAKE_STORAGE_PATTERN % self.credentials.ducklake_name
            )

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
