# from __future__ import annotations

import dataclasses
from typing import Optional, Union

from dlt.common.configuration import configspec
from dlt.common.configuration.specs.connection_string_credentials import ConnectionStringCredentials
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.storages.configuration import FilesystemConfiguration, WithLocalFiles
from dlt.common.utils import digest128
from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials
from dlt.destinations.impl.duckdb.factory import _set_duckdb_raw_capabilities


DUCKLAKE_NAME_PATTERN = "%s.ducklake"


# TODO maybe this needs to be parameterized by `catalog` and `storage` implementation
def _get_ducklake_capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext()
    caps = _set_duckdb_raw_capabilities(caps)
    return caps


# TODO support connecting to a snapshot


@configspec(init=False)
class DuckLakeCredentials(DuckDbCredentials):
    """
    For DuckLakeCredentials, the field `database` refers to the name
    of the DuckLake.

    """

    ducklake_name: str = ":pipeline:"
    catalog: Optional[ConnectionStringCredentials] = None
    storage: Optional[FilesystemConfiguration] = None

    def __init__(self, *, attach_statement: Optional[str] = None) -> None:
        self._attach_statement = attach_statement

    def _conn_str(self) -> str:
        return ":memory:"

    def on_resolved(self) -> None:
        if self.catalog is None:
            self.catalog = DuckDbCredentials()

        if self.extensions:
            self.extensions = list(set([*self.extensions, "ducklake"]))
        else:
            self.extensions = ["ducklake"]

    @staticmethod
    def build_attach_statement(
        *,
        ducklake_name: str,
        catalog: Union[ConnectionStringCredentials, str],
        storage: Optional[FilesystemConfiguration] = None,
    ) -> str:
        # TODO resolve ConnectionStringCredentials; duckdb has its own format
        attach_statement = f"ATTACH IF NOT EXISTS 'ducklake:{ducklake_name}.ducklake'"
        if storage:
            # TODO handle storage credentials by creating secrets
            attach_statement += f" (DATA_PATH {storage.bucket_url})"

        attach_statement += f" AS {ducklake_name}"
        return attach_statement

    @property
    def attach_statement(self) -> str:
        # TODO handle when `ducklake_name` or `catalog` is not set
        if not self.is_resolved():
            return None

        # return value when set explicitly
        if self._attach_statement:
            return self._attach_statement
        else:
            return self.build_attach_statement(
                ducklake_name=self.ducklake_name,
                catalog=self.catalog,
                storage=self.storage,
            )

    @attach_statement.setter
    def attach_statement(self, value: str) -> None:
        self._attach_statement = value


# TODO add connection to a specific snapshot
# TODO does it make sense for ducklake to have a staging destination?
@configspec
class DuckLakeClientConfiguration(WithLocalFiles, DestinationClientDwhWithStagingConfiguration):
    destination_type: str = dataclasses.field(
        default="ducklake",
        init=False,
        repr=False,
        compare=False,
    )
    credentials: DuckLakeCredentials = DuckLakeCredentials()
    create_indexes: bool = False  # does nothing but required

    def fingerprint(self) -> str:
        """Returns a fingerprint of user access token"""
        # TODO is this most appropriate?
        if self.credentials and self.credentials.password:
            return digest128(self.credentials.password)
        return ""

    def on_resolved(self) -> None:
        if self.credentials.ducklake_name == ":pipeline:":
            self.credentials.ducklake_name = self.pipeline_name

        # NOTE this is only for the file-based catalogs DuckDB and SQLite
        local_db = self.make_location(self.credentials.database, DUCKLAKE_NAME_PATTERN)
        self.credentials.database = local_db
        self.credentials.catalog.database = local_db
