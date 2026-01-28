from __future__ import annotations

from typing import Any, Dict, Union, Type, TYPE_CHECKING

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.destination.configuration import ParquetFormatConfiguration
from dlt.destinations.impl.huggingface.configuration import HfClientConfiguration

if TYPE_CHECKING:
    from dlt.destinations.impl.huggingface.configuration import HfCredentials
    from dlt.destinations.impl.huggingface.huggingface import HfClient


class huggingface(Destination[HfClientConfiguration, "HfClient"]):
    spec = HfClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "parquet"
        caps.supported_loader_file_formats = ["parquet", "jsonl", "csv"]
        caps.has_case_sensitive_identifiers = True
        caps.max_identifier_length = 200
        caps.max_column_identifier_length = 1024
        caps.max_query_length = 8 * 1024 * 1024
        caps.is_max_query_length_in_bytes = False
        caps.max_text_data_type_length = 8 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = False
        caps.supports_ddl_transactions = False
        caps.supported_replace_strategies = ["truncate-and-insert"]
        caps.parquet_format = ParquetFormatConfiguration(
            use_content_defined_chunking=True,
            write_page_index=True,
        )
        return caps

    @property
    def client_class(self) -> Type["HfClient"]:
        from dlt.destinations.impl.huggingface.huggingface import HfClient

        return HfClient

    def __init__(
        self,
        dataset_name: str,
        split: str = "train",
        credentials: Union[HfCredentials, Dict[str, Any], str] = None,
        **kwargs: Any,
    ) -> None:
        """Configure the Databricks destination to use in a pipeline.

        All arguments provided here supersede other configuration sources such as environment variables and dlt config files.

        Args:
            dataset_name: Destination dataset in the format "<user>/<dataset>"
            split: "train" (default), "test" or "validation"
            credentials (Union[HfCredentials, Dict[str, Any], str], optional): Credentials to connect to Hugging Face.
            **kwargs (Any): Additional arguments passed to the destination config
        """
        super().__init__(
            dataset_name=dataset_name,
            split=split,
            credentials=credentials,
            **kwargs,
        )



huggingface.register()
