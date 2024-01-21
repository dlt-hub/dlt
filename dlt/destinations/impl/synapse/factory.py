import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.schema.typing import TTableIndexType
from dlt.destinations.impl.synapse import capabilities

from dlt.destinations.impl.synapse.configuration import (
    SynapseCredentials,
    SynapseClientConfiguration,
)

if t.TYPE_CHECKING:
    from dlt.destinations.impl.synapse.synapse import SynapseClient


class synapse(Destination[SynapseClientConfiguration, "SynapseClient"]):
    spec = SynapseClientConfiguration

    def capabilities(self) -> DestinationCapabilitiesContext:
        return capabilities()

    @property
    def client_class(self) -> t.Type["SynapseClient"]:
        from dlt.destinations.impl.synapse.synapse import SynapseClient

        return SynapseClient

    def __init__(
        self,
        credentials: t.Union[SynapseCredentials, t.Dict[str, t.Any], str] = None,
        default_table_index_type: t.Optional[TTableIndexType] = "heap",
        create_indexes: bool = False,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Synapse destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the Synapse dedicated pool. Can be an instance of `SynapseCredentials` or
                a connection string in the format `synapse://user:password@host:port/database`
            default_table_index_type: Table index type that is used if no
                table index type is specified on the resource. This setting only
                applies to data tables, dlt system tables are not affected
                (they always have "heap" as table index type).
            create_indexes: Should unique indexes be created, defaults to False
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            default_table_index_type=default_table_index_type,
            create_indexes=create_indexes,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
