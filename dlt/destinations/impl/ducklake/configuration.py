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
    def __init__(
        self,
        # TODO how does duckdb resolve the name of the database to the name of the dataset / pipeline
        ducklake_name: str = "ducklake",
        *,
        catalog_database: Optional[Union[ConnectionStringCredentials, DuckDbCredentials]] = None,
        storage: Optional[FilesystemConfiguration] = None,
        attach_statement: Optional[str] = None,
    ) -> None:
        if catalog_database is not None:
            raise NotImplementedError

        if storage is not None:
            raise NotImplementedError

        self.ducklake_name = ducklake_name
        self.catalog_database = self
        self.storage = storage

        self.database = self.catalog_database.database  # required
        self._attach_statement = attach_statement

    @staticmethod
    def build_attach_statement(
        *,
        ducklake_name: str,
        catalog_database: ConnectionStringCredentials,
        storage: Optional[FilesystemConfiguration] = None,
    ) -> str:
        attach_statement = f"ATTACH 'ducklake:{catalog_database.to_native_representation()}'"
        if storage:
            attach_statement += f" (DATA_PATH {storage.bucket_url})"

        attach_statement += f" AS {ducklake_name}"
        return attach_statement

    @property
    def attach_statement(self) -> str:
        # return value when set explicitly
        if self._attach_statement:
            return self._attach_statement
        else:
            return self.build_attach_statement(
                ducklake_name=self.ducklake_name,
                catalog_database=self.catalog_database,
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
    credentials: Optional[DuckLakeCredentials] = None
    create_indexes: bool = False  # does nothing but required

    def fingerprint(self) -> str:
        """Returns a fingerprint of user access token"""
        # TODO is this most appropriate?
        if self.credentials and self.credentials.password:
            return digest128(self.credentials.password)
        return ""

    def on_resolved(self) -> None:
        local_db = self.make_location(
            self.credentials.catalog_database.database, DUCKLAKE_NAME_PATTERN
        )
        self.credentials.database = local_db
        self.credentials.catalog_database.database = local_db
