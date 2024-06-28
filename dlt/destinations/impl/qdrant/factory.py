import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.destinations.impl.qdrant.configuration import QdrantCredentials, QdrantClientConfiguration

if t.TYPE_CHECKING:
    from dlt.destinations.impl.qdrant.qdrant_client import QdrantClient


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

        return caps

    @property
    def client_class(self) -> t.Type["QdrantClient"]:
        from dlt.destinations.impl.qdrant.qdrant_client import QdrantClient

        return QdrantClient

    def __init__(
        self,
        credentials: t.Union[QdrantCredentials, t.Dict[str, t.Any]] = None,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(
            credentials=credentials,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
