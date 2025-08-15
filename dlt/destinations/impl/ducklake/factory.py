from __future__ import annotations

from typing import Any, Optional

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.destinations.impl.duckdb.factory import _set_duckdb_raw_capabilities
from dlt.destinations.impl.ducklake.ducklake import DuckLakeClient
from dlt.destinations.impl.ducklake.configuration import (
    DuckLakeClientConfiguration,
    DuckLakeCredentials,
)


class ducklake(Destination[DuckLakeClientConfiguration, DuckLakeClient]):
    """Instantiate a DuckLake destination.

    A DuckLake has 3 components:
        - ducklake client: this is a `duckdb` instance with the `ducklake` extension
        - catalog: this is an SQL database storing metadata. It can be a duckdb instance
            (typically the ducklake client) or a remote database (sqlite, postgres, mysql)
        - storage: this is a filesystem where data is stored in files

    The dlt DuckLake destination gives access to the "ducklake client".
    You never have to manage the catalog and storage directly;
    this is done through the ducklake client.
    """

    def __init__(
        self,
        credentials: Optional[DuckLakeCredentials] = None,
        destination_name: Optional[str] = None,
        environment: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """
        Args:
            credentials: DuckLake credentials or instantiated connection to a DuckLake
                client (which is a duckdb instance). The DuckLake credentials include
                credentials for ducklake client, catalog, and storage
            destination_name: This is the name of the ducklake, which will be a namespace
                in the catalog and storage. This will be the name of the duckdb instance
                that serves as ducklake client
        """      
        super().__init__(
            credentials=credentials,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )

    @property
    def spec(self) -> type:
        return DuckLakeClientConfiguration
    
    @property
    def client_class(self) -> type:
        from dlt.destinations.impl.ducklake.ducklake import DuckLakeClient
        return DuckLakeClient
     
    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        # TODO adjust to actual DuckLake capabilities
        # how do they vary based on catalog and storage? does ducklake client automatically
        # handles disparities?
        caps = DestinationCapabilitiesContext()
        caps = _set_duckdb_raw_capabilities(caps)
        return caps
    

ducklake.register()
