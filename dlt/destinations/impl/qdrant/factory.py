from typing import Any, Optional, Type, Union, Dict, TYPE_CHECKING


from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.normalizers.naming import NamingConvention

from dlt.destinations.impl.qdrant.configuration import QdrantCredentials, QdrantClientConfiguration

if TYPE_CHECKING:
    from dlt.destinations.impl.qdrant.qdrant_job_client import QdrantClient
else:
    QdrantClient = Any


class qdrant(Destination[QdrantClientConfiguration, "QdrantClient"]):
    spec = QdrantClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "jsonl"
        caps.supported_loader_file_formats = ["jsonl"]
        caps.has_case_sensitive_identifiers = True
        caps.max_identifier_length = 200
        caps.max_column_identifier_length = 1024
        caps.max_query_length = 8 * 1024 * 1024
        caps.is_max_query_length_in_bytes = False
        caps.max_text_data_type_length = 8 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = False
        caps.supports_ddl_transactions = False
        caps.supported_replace_strategies = ["truncate-and-insert"]

        return caps

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: QdrantClientConfiguration,
        naming: Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        caps = super(qdrant, cls).adjust_capabilities(caps, config, naming)
        if config.is_local():
            # Local qdrant can not load in parallel
            caps.loader_parallelism_strategy = "sequential"
            caps.max_parallel_load_jobs = 1
        return caps

    @property
    def client_class(self) -> Type["QdrantClient"]:
        from dlt.destinations.impl.qdrant.qdrant_job_client import QdrantClient

        return QdrantClient

    def __init__(
        self,
        credentials: Union[QdrantClient, QdrantCredentials, Dict[str, Any]] = None,
        location: str = None,
        path: str = None,
        destination_name: str = None,
        environment: str = None,
        **kwargs: Any,
    ) -> None:
        """Configure the Qdrant destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            credentials (Union[QdrantClient, QdrantCredentials, Dict[str, Any]], optional): Credentials to connect to the Qdrant database. Can be an instance of `QdrantClient` or
                a dictionary with the credentials parameters.
            location (str, optional): The location of the Qdrant database.
            path (str, optional): The path to the Qdrant database.
            destination_name (str, optional): Name of the destination, can be used in config section to differentiate between multiple of the same type
            environment (str, optional): Environment of the destination
            **kwargs (Any, optional): Additional arguments passed to the destination config
        """
        super().__init__(
            credentials=credentials,
            location=location,
            path=path,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


qdrant.register()
