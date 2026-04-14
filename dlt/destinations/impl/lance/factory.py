from __future__ import annotations

from typing import Any, Dict, Optional, Type, TYPE_CHECKING, Union

from dlt.common.destination.configuration import ParquetFormatConfiguration
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.destination.capabilities import DataTypeMapper
from dlt.common.exceptions import MissingDependencyException
from dlt.destinations.impl.lance.configuration import (
    LanceCatalogType,
    LanceClientConfiguration,
    LanceCredentials,
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
    def client_class(self) -> type[LanceClient]:
        from dlt.destinations.impl.lance.lance_client import LanceClient

        return LanceClient

    def __init__(
        self,
        catalog_type: LanceCatalogType = None,
        credentials: Union[LanceCredentials, Dict[str, Any]] = None,
        storage: Union[LanceStorageConfiguration, Dict[str, Any]] = None,
        branch_name: Optional[str] = None,
        embeddings: Union[LanceEmbeddingsConfiguration, Dict[str, Any]] = None,
        destination_name: str = None,
        environment: str = None,
        **kwargs: Any,
    ) -> None:
        """Configure the Lance destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment
        variables and dlt config files.

        Args:
            catalog_type (LanceCatalogType, optional): Lance catalog backend. Defaults to `"dir"`
                (directory namespace).
            credentials (Union[LanceCredentials, Dict[str, Any]], optional): Catalog-scoped credentials. For `"dir"`,
                this is an optional `DirectoryCatalogCredentials` overriding the `__manifest`
                location; when empty, catalog colocates with `storage`.
            storage (Union[LanceStorageConfiguration, Dict[str, Any]], optional): Storage configuration for table data
                (bucket, credentials, options, namespace subpath).
            branch_name (Optional[str]): Read/write branch for Lance operations. Uses `main` if not set.
            embeddings (Union[LanceEmbeddingsConfiguration, Dict[str, Any]], optional): Embedding provider, model,
                and credentials. If not provided, no vector column is added.
            destination_name (str, optional): Name of the destination.
            environment (str, optional): Environment of the destination.
            **kwargs (Any): Additional arguments forwarded to the destination config.
        """
        super().__init__(
            catalog_type=catalog_type,
            credentials=credentials,
            storage=storage,
            branch_name=branch_name,
            embeddings=embeddings,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


lance.register()
