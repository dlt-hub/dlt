from __future__ import annotations

from typing import Any, Type, Union, Dict, TYPE_CHECKING

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.destinations.impl.duckdb.factory import _set_duckdb_raw_capabilities
from dlt.destinations.impl.motherduck.configuration import (
    MotherDuckCredentials,
    MotherDuckClientConfiguration,
)

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection
    from dlt.destinations.impl.motherduck.motherduck import MotherDuckClient


class motherduck(Destination[MotherDuckClientConfiguration, "MotherDuckClient"]):
    spec = MotherDuckClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps = _set_duckdb_raw_capabilities(caps)
        caps.preferred_loader_file_format = "parquet"
        caps.max_query_length = 512 * 1024
        caps.max_parallel_load_jobs = 8
        return caps

    @property
    def client_class(self) -> Type["MotherDuckClient"]:
        from dlt.destinations.impl.motherduck.motherduck import MotherDuckClient

        return MotherDuckClient

    def __init__(
        self,
        credentials: Union[MotherDuckCredentials, str, Dict[str, Any], DuckDBPyConnection] = None,
        create_indexes: bool = False,
        destination_name: str = None,
        environment: str = None,
        **kwargs: Any,
    ) -> None:
        """Configure the MotherDuck destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials (Union[MotherDuckCredentials, str, Dict[str, Any], DuckDBPyConnection], optional): Credentials to connect to the MotherDuck database. Can be an instance of `MotherDuckCredentials` or
                a connection string in the format `md:///<database_name>?token=<service token>`.
                Instance of `DuckDbCredentials` allows to pass access token, extensions, configs and pragmas to be set up for connection.
            create_indexes (bool, optional): Should unique indexes be created
            destination_name (str, optional): Name of the destination, can be used in config section to differentiate between multiple of the same type
            environment (str, optional): Environment of the destination
            **kwargs (Any): Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            create_indexes=create_indexes,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


motherduck.register()
