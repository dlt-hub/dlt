from dlt.common.configuration import configspec
from dlt.common.configuration.specs import PostgresCredentials

from dlt.load.configuration import DestinationClientDwhConfiguration


@configspec(init=True)
class RedshiftClientConfiguration(DestinationClientDwhConfiguration):
    destination_name: str = "redshift"
    credentials: PostgresCredentials

