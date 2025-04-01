from typing import Type, Union, Dict, Optional, Any
from dlt.destinations.impl.starrocks.configuration import (
    StarrocksClientConfiguration,
    StarrocksCredentials
)
from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.destinations.impl.sqlalchemy.factory import sqlalchemy
class starrocks(sqlalchemy):
    spec = StarrocksClientConfiguration
    
    @property
    def client_class(self) -> Type["StarrocksJobClient"]:
        from dlt.destinations.impl.starrocks.job_client import StarrocksJobClient
        return StarrocksJobClient

    def __init__(
        self,
        credentials: Union[StarrocksCredentials, Dict[str, Any]] = None,
        # default_table_type: str = 'primary_key',
        destination_name: Optional[str] = None,
        environment: Optional[str] = None,
        engine_args: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            credentials=credentials,
            destination_name=destination_name,
            environment=environment,
            engine_args=engine_args,
            **kwargs,
        )
    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = super()._raw_capabilities()
        caps.preferred_load_file_format = "typed-jsonl"
        caps.supported_loader_file_formats = ["typed-jsonl"]
        caps.preferred_staging_file_format = "parquet"
        caps.supported_staging_file_formats = ["parquet"]
        caps.sqlglog_dialect = "starrocks"
        caps.naming_convention = "dlt.destinations.impl.starrocks.naming"

        return caps



starrocks.register()
