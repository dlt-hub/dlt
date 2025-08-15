# from __future__ import annotations

import dataclasses
from typing import Final, Optional, overload

from dlt.common.configuration import configspec
from dlt.common.configuration.specs.connection_string_credentials import ConnectionStringCredentials
from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.utils import digest128
from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials


@configspec(init=False)
class DuckLakeCredentials(DuckDbCredentials):
    database: str = "my_ducklake"

    @overload
    def __init__(self) -> None:
        """Instantiate DuckLake from
            - ducklake client: local duckdb file with default name
            - catalog: use the ducklake client duckdb instance
            - storage: use local filesystem (default)
        """

    @overload
    def __init__(self, ducklake_name: str) -> None:
        """Instantiate DuckLake from
            - ducklake client: local duckdb file with name `ducklake_name`
            - catalog: use the ducklake client duckdb instance
            - storage: use local filesystem (default)
        """

    @overload
    def __init__(self, ducklake_name: str, *, ducklake: DuckDbCredentials) -> None:
        """Instantiate DuckLake from:
            - ducklake client: existing duckdb instance containing secrets
            - catalog: configured by secrets in ducklake client 
            - storage: configured by secrets in ducklake client 
        
        ref: https://ducklake.select/docs/stable/duckdb/usage/connecting
        """

    @overload
    def __init__(self, ducklake_name: str, *, catalog: ConnectionStringCredentials) -> None:
        """Instantiate DuckLake from
            - ducklake client: ephemeral duckdb client using `:memory:`
            - catalog: use the configured catalog
            - storage: use local filesystem (default)
        """

    @overload
    def __init__(self, ducklake_name: str, *, storage: FilesystemConfiguration) -> None:
        """Instantiate DuckLake from
            - ducklake client: local duckdb file derived from pipeline name
            - catalog: use the ducklake client duckdb instance
            - storage: use configured filesystem or object store
        """

    @overload
    def __init__(self, ducklake_name: str, *, catalog: ConnectionStringCredentials, storage: FilesystemConfiguration) -> None:
        """Instantiate DuckLake from
            - ducklake client: ephemeral duckdb client using `:memory:`
            - catalog: use the configured catalog
            - storage: use configured filesystem or object store
        """

    def __init__(
        self,
        ducklake_name: str = None,
        *,
        catalog: Optional[ConnectionStringCredentials] = None,
        storage: Optional[FilesystemConfiguration] = None,
        ducklake: Optional[DuckDbCredentials] = None,
    ) -> None:
        self.ducklake_name = ducklake_name if ducklake_name else "ducklake"

        if ducklake is not None:
            # ducklake is set, retrieve catalog and storage from stored secrets 
            catalog = "foo"
            storage = "..."
            # ensure the ducklake extension is installed
            if ducklake.extensions is None:
                ducklake.extensions = ["ducklake"]
            elif "ducklake" not in ducklake.extensions:
                ducklake.extensions = list(ducklake.extensions) + ["ducklake"]
        elif ducklake is None and catalog is None:
            # use a duckdb instance for both
            ducklake = DuckDbCredentials(extensions=["ducklake"])
            catalog = ducklake

        elif ducklake is None and catalog is not None:
            # use an ephemeral duckdb connection as ducklake client
            import duckdb
            ducklake = DuckDbCredentials(duckdb.connect(":memory:"), extensions=["ducklake"])

        assert ducklake is not None
        assert catalog is not None

        # TODO improve configuration here
        if storage is None:
            storage = FilesystemConfiguration(bucket_url=self.ducklake_name)

        self.ducklake: DuckDbCredentials = ducklake
        # TODO handle string connection
        self.catalog: ConnectionStringCredentials = catalog
        self.storage: FilesystemConfiguration = storage


@configspec
class DuckLakeClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = dataclasses.field(
        default="ducklake", init=False, repr=False, compare=False,
    )
    credentials: Optional[DuckLakeCredentials] = None
    create_indexes: bool = False

    def fingerprint(self) -> str:
        """Returns a fingerprint of user access token"""
        if self.credentials and self.credentials.password:
            return digest128(self.credentials.password)
        return ""