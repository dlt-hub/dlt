from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Union

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.destinations.impl.duckdb.factory import _set_duckdb_raw_capabilities
from dlt.destinations.impl.ducklake.configuration import (
    DuckLakeClientConfiguration,
    DuckLakeCredentials,
)

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection
    from dlt.destinations.impl.ducklake.ducklake import DuckLakeClient


# TODO support multiple catalog DB
# TODO support multiple file destinations
class ducklake(Destination[DuckLakeClientConfiguration, DuckLakeClient]):
    def __init__(
        self,
        credentials: Optional[Union[DuckLakeCredentials, str, dict[str, Any], DuckDBPyConnection]] = None,
        create_indexes: bool = False,
        destination_name: Optional[str] = None,
        environment: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            credentials=credentials,
            create_indexes=create_indexes,
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
        # TODO adjust to actual DuckLake capabilities?
        caps = DestinationCapabilitiesContext()
        caps = _set_duckdb_raw_capabilities(caps)
        return caps
    

ducklake.register()
