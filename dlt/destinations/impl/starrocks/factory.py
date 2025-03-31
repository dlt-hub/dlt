from typing import Type, Union, Dict, Optional, Any
from dlt.destinations.impl.starrocks.configuration import (
    StarrocksClientConfiguration,
    StarrocksCredentials
)

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
        default_table_type: str = 'primary_key',
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


starrocks.register()
