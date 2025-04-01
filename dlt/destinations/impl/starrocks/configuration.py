import dataclasses
from starrocks.dialect import StarRocksDialect
from typing import Final, Dict, Any

from dlt.common.configuration import configspec

from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.destinations.impl.sqlalchemy.configuration import (
    SqlalchemyCredentials,
    SqlalchemyClientConfiguration
)

@configspec(init = False)
class StarrocksCredentials(SqlalchemyCredentials):
    drivername: str = "starrocks"
    http_port: int = 8040
    http_host: str = None

@configspec
class StarrocksClientConfiguration(SqlalchemyClientConfiguration, DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = dataclasses.field(default="starrocks", init=False, repr=False, compare=False)  # type: ignore[misc]
    credentials: StarrocksCredentials = None

    def get_dialect(self):
        return StarRocksDialect

    def get_backend_name(self):
        return "starrocks"
