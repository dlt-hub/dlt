from dlt.common.configuration import configspec
from dlt.common.configuration.specs import GcpClientCredentialsWithDefault
from dlt.common.destination.reference import DestinationClientDwhConfiguration


@configspec(init=True)
class BigQueryClientConfiguration(DestinationClientDwhConfiguration):
    destination_name: str = "bigquery"
    credentials: GcpClientCredentialsWithDefault = None
