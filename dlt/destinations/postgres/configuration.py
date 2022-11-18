from typing import Final
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import PostgresCredentials
from dlt.common.destination import DestinationClientDwhConfiguration


@configspec(init=True)
class PostgresClientConfiguration(DestinationClientDwhConfiguration):
    destination_name: Final[str] = "postgres"  # type: ignore
    credentials: PostgresCredentials

