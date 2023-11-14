import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.destinations.impl.athena.configuration import AthenaClientConfiguration
from dlt.common.configuration.specs import AwsCredentials
from dlt.destinations.impl.athena import capabilities

if t.TYPE_CHECKING:
    from dlt.destinations.impl.athena.athena import AthenaClient


class athena(Destination[AthenaClientConfiguration, "AthenaClient"]):

    spec = AthenaClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["AthenaClient"]:
        from dlt.destinations.impl.athena.athena import AthenaClient

        return AthenaClient

    def __init__(
        self,
        query_result_bucket: t.Optional[str] = None,
        credentials: t.Optional[AwsCredentials] = None,
        athena_work_group: t.Optional[str] = None,
        aws_data_catalog: t.Optional[str] = "awsdatacatalog",
        supports_truncate_command: bool = False,
        force_iceberg: bool = False,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(**kwargs)
