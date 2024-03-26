import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.destinations.impl.synapse import capabilities
from dlt.destinations.impl.synapse.configuration import (
    SynapseCredentials,
    SynapseClientConfiguration,
)
from dlt.destinations.impl.synapse.synapse_adapter import TTableIndexType

if t.TYPE_CHECKING:
    from dlt.destinations.impl.synapse.synapse import SynapseClient


class synapse(Destination[SynapseClientConfiguration, "SynapseClient"]):
    spec = SynapseClientConfiguration

    # TODO: implement as property everywhere and makes sure not accessed as class property
    # @property
    # def spec(self) -> t.Type[SynapseClientConfiguration]:
    #     return SynapseClientConfiguration

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
        staging_use_msi: bool = False,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Synapse destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials: Credentials to connect to the Synapse dedicated pool. Can be an instance of `SynapseCredentials` or
                a connection string in the format `synapse://user:password@host:port/database`
            default_table_index_type: Maps directly to the default_table_index_type attribute of the SynapseClientConfiguration object.
            create_indexes: Maps directly to the create_indexes attribute of the SynapseClientConfiguration object.
            staging_use_msi: Maps directly to the staging_use_msi attribute of the SynapseClientConfiguration object.
            **kwargs: Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            default_table_index_type=default_table_index_type,
            create_indexes=create_indexes,
            staging_use_msi=staging_use_msi,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
