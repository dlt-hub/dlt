from typing import Any, Dict, Optional, Type, Union, TYPE_CHECKING

from dlt.common.destination.configuration import ParquetFormatConfiguration
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.destination.capabilities import DataTypeMapper
from dlt.common.exceptions import MissingDependencyException
from dlt.destinations.impl.lance.configuration import (
    LanceCredentials,
    LanceClientConfiguration,
    LanceStorageConfiguration,
    TEmbeddingProvider,
)

LanceTypeMapper: Type[DataTypeMapper]
try:
    # lancedb type mapper cannot be used without pyarrow installed
    from dlt.destinations.impl.lance.type_mapper import LanceTypeMapper
except MissingDependencyException:
    # assign mock type mapper if no arrow
    from dlt.common.destination.capabilities import UnsupportedTypeMapper as LanceTypeMapper


if TYPE_CHECKING:
    from dlt.destinations.impl.lance.lance_client import LanceClient


class lance(Destination[LanceClientConfiguration, "LanceClient"]):
    spec = LanceClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "parquet"
        caps.supported_loader_file_formats = ["parquet", "reference"]
        caps.type_mapper = LanceTypeMapper

        caps.max_identifier_length = 200
        caps.max_column_identifier_length = 1024
        caps.max_query_length = 8 * 1024 * 1024
        caps.is_max_query_length_in_bytes = False
        caps.max_text_data_type_length = 8 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = False
        caps.supports_ddl_transactions = False

        caps.decimal_precision = (38, 18)
        caps.timestamp_precision = 6
        caps.supported_replace_strategies = ["truncate-and-insert"]

        caps.recommended_file_size = 128_000_000

        caps.supported_merge_strategies = ["upsert"]

        # enable creation of nested types to support own vectors
        caps.supports_nested_types = True

        # must store arrow-compatible nested types, not parquet default - otherwise schema checker in lance fails
        caps.parquet_format = ParquetFormatConfiguration(use_compliant_nested_type=False)

        return caps

    @property
    def client_class(self) -> Type["LanceClient"]:
        from dlt.destinations.impl.lance.lance_client import LanceClient

        return LanceClient

    def __init__(
        self,
        storage: LanceStorageConfiguration = None,
        credentials: Union[LanceCredentials, Dict[str, Any]] = None,
        embedding_model_provider: TEmbeddingProvider = None,
        embedding_model: str = None,
        vector_field_name: str = None,
        destination_name: str = None,
        environment: str = None,
        **kwargs: Any,
    ) -> None:
        """Configure the Lance destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            storage (LanceStorageConfiguration): Configuration for storage where lance datasets are stored.
            credentials (Union[LanceCredentials, Dict[str, Any]]): Credentials for the Lance destination. Can be
                an instance of `LanceCredentials` or a dictionary with the credentials parameters.
            embedding_model_provider (TEmbeddingProvider, optional): Embedding provider used for generating embeddings.
                Default is "cohere". See LanceDB documentation for the full list of available providers.
            embedding_model (str, optional): The model used by the embedding provider for generating embeddings.
                Default is "embed-english-v3.0". Check with the embedding provider which options are available.
            vector_field_name (str, optional): Name of the special field to store the vector embeddings.
                Default is "vector".
            destination_name (str, optional): Name of the destination.
            environment (str, optional): Environment of the destination.
            **kwargs (Any): Additional arguments forwarded to the destination config.
        """
        super().__init__(
            storage=storage,
            credentials=credentials,
            embedding_model_provider=embedding_model_provider,
            embedding_model=embedding_model,
            vector_field_name=vector_field_name,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


lance.register()
