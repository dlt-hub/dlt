import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.destinations.type_mapping import TypeMapperImpl
from dlt.destinations.impl.weaviate.configuration import (
    WeaviateCredentials,
    WeaviateClientConfiguration,
)

if t.TYPE_CHECKING:
    from dlt.destinations.impl.weaviate.weaviate_client import WeaviateClient


class WeaviateTypeMapper(TypeMapperImpl):
    sct_to_unbound_dbt = {
        "text": "text",
        "double": "number",
        "bool": "boolean",
        "timestamp": "date",
        "date": "date",
        "time": "text",
        "bigint": "int",
        "binary": "blob",
        "decimal": "text",
        "wei": "number",
        "json": "text",
    }

    sct_to_dbt = {}

    dbt_to_sct = {
        "text": "text",
        "number": "double",
        "boolean": "bool",
        "date": "timestamp",
        "int": "bigint",
        "blob": "binary",
    }


class weaviate(Destination[WeaviateClientConfiguration, "WeaviateClient"]):
    spec = WeaviateClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "jsonl"
        caps.supported_loader_file_formats = ["jsonl"]
        caps.type_mapper = WeaviateTypeMapper
        # weaviate names are case sensitive following GraphQL naming convention
        # https://weaviate.io/developers/weaviate/config-refs/schema
        caps.has_case_sensitive_identifiers = False
        # weaviate will upper case first letter of class name and lower case first letter of a property
        # we assume that naming convention will do that
        caps.casefold_identifier = str
        caps.max_identifier_length = 200
        caps.max_column_identifier_length = 1024
        caps.max_query_length = 8 * 1024 * 1024
        caps.is_max_query_length_in_bytes = False
        caps.max_text_data_type_length = 8 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = False
        caps.supports_ddl_transactions = False
        caps.naming_convention = "dlt.destinations.impl.weaviate.naming"
        caps.supported_replace_strategies = ["truncate-and-insert"]

        return caps

    @property
    def client_class(self) -> t.Type["WeaviateClient"]:
        from dlt.destinations.impl.weaviate.weaviate_client import WeaviateClient

        return WeaviateClient

    def __init__(
        self,
        credentials: t.Union[WeaviateCredentials, t.Dict[str, t.Any]] = None,
        vectorizer: str = None,
        module_config: t.Dict[str, t.Dict[str, str]] = None,
        destination_name: str = None,
        environment: str = None,
        **kwargs: t.Any,
    ) -> None:
        """Configure the Weaviate destination to use in a pipeline.

        All destination config parameters can be provided as arguments here and will supersede other config sources (such as dlt config files and environment variables).

        Args:
            credentials (t.Union[WeaviateCredentials, t.Dict[str, t.Any]], optional): Weaviate credentials containing URL, API key and optional headers
            vectorizer (str, optional): The name of the Weaviate vectorizer to use
            module_config (t.Dict[str, t.Dict[str, str]], optional): The configuration for the Weaviate modules
            destination_name (str, optional): Name of the destination. Defaults to None.
            environment (str, optional): Environment name. Defaults to None.
            **kwargs (t.Any, optional): Additional arguments forwarded to the destination config
        """
        super().__init__(
            credentials=credentials,
            vectorizer=vectorizer,
            module_config=module_config,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


weaviate.register()
