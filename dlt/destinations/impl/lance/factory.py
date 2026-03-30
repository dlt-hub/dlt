from typing import Any, Type, TYPE_CHECKING

from dlt.common.destination.configuration import ParquetFormatConfiguration
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.destination.capabilities import DataTypeMapper
from dlt.common.exceptions import MissingDependencyException
from dlt.destinations.impl.lance.configuration import (
    LanceClientConfiguration,
    LanceEmbeddingsConfiguration,
    LanceStorageConfiguration,
)

LanceTypeMapper: Type[DataTypeMapper]
try:
    # lance type mapper cannot be used without pyarrow installed
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
        embeddings: LanceEmbeddingsConfiguration = None,
        destination_name: str = None,
        environment: str = None,
        **kwargs: Any,
    ) -> None:
        """Configure the Lance destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            storage (LanceStorageConfiguration): Configuration for storage where lance datasets are stored.
            embeddings (LanceEmbeddingsConfiguration, optional): Embeddings configuration including model provider,
                model, provider credentials, and vector field settings. If not provided, no vector column is added.
            destination_name (str, optional): Name of the destination.
            environment (str, optional): Environment of the destination.
            **kwargs (Any): Additional arguments forwarded to the destination config.
        """
        super().__init__(
            storage=storage,
            embeddings=embeddings,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


lance.register()
