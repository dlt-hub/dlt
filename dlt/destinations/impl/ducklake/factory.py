from __future__ import annotations

from typing import Any, Optional, TYPE_CHECKING

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.normalizers.naming import NamingConvention

from dlt.destinations.impl.ducklake.configuration import (
    DuckLakeClientConfiguration,
    DuckLakeCredentials,
    _get_ducklake_capabilities,
)

if TYPE_CHECKING:
    from dlt.destinations.impl.ducklake.ducklake import DuckLakeClient


class ducklake(Destination[DuckLakeClientConfiguration, "DuckLakeClient"]):
    """Instantiate a DuckLake destination.

    A DuckLake has 3 components:
        - ducklake client: this is a `duckdb` :memory: instance with the `ducklake` ATTACHed
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
        """Configure the ducklake destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials(Optional[DuckLakeCredentials]): DuckLake credentials or instantiated connection to a DuckLake
                client (which is a duckdb instance). The DuckLake credentials include
                catalog name, catalog, and storage
            destination_name(Optional[str]): Name of a destination which. May be used as ducklake name, if
                explicit name is not set in `credentials`.
            environment (Optional[str]): Environment of the destination
            **kwargs (Any, optional): Additional arguments forwarded to the destination config
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

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: DuckLakeClientConfiguration,
        naming: Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        # disable parallel loading for duckdb and sqllite
        # NOTE: sqllite fails on concurrent catalog updates also in WAL mode
        # https://github.com/duckdb/ducklake/issues/128 does not work
        if config.credentials.catalog and config.credentials.catalog.drivername not in (
            "duckdb",
            "sqlite",
        ):
            caps.loader_parallelism_strategy = "parallel"
        return super().adjust_capabilities(caps, config, naming)

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        return _get_ducklake_capabilities()


ducklake.register()
