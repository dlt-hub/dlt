import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.destinations.type_mapping import TypeMapperImpl
from dlt.destinations.impl.weaviate.configuration import (
    TWeaviateBatchConsistency,
    TWeaviateBatchMode,
    WeaviateCredentials,
    WeaviateClientConfiguration,
    TWeaviateConnectionType,
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
        caps.supported_merge_strategies = ["upsert"]
        caps.supports_naive_datetime = False

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
        connection_type: TWeaviateConnectionType = None,
        batch_mode: TWeaviateBatchMode = None,
        batch_size: int = None,
        batch_workers: int = None,
        batch_requests_per_minute: int = None,
        batch_consistency: TWeaviateBatchConsistency = None,
        collection_config: t.Dict[str, t.Any] = None,
        multi_tenancy: bool = None,
        tenant: str = None,
        auto_tenant_creation: bool = None,
        auto_tenant_activation: bool = None,
        skip_init_checks: bool = None,
        conn_timeout: float = None,
        read_timeout: float = None,
        init_timeout: float = None,
        query_timeout: float = None,
        insert_timeout: float = None,
        stream_timeout: float = None,
        proxies: t.Dict[str, str] = None,
        trust_env: bool = None,
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
            connection_type (TWeaviateConnectionType, optional): Connection type - "cloud" for Weaviate Cloud,
                "local" for Docker instances, "custom" for self-hosted. If None, auto-detected from URL.
            batch_mode (TWeaviateBatchMode, optional): How objects are batched: "auto" (default) uses
                server-side batching when the server supports it, otherwise one of "stream",
                "fixed_size", "rate_limit" or "dynamic".
            batch_size (int, optional): Objects per request, applies to "fixed_size" batching.
            batch_workers (int, optional): Concurrent requests, applies to "fixed_size" and "stream".
            batch_requests_per_minute (int, optional): Request budget, applies to "rate_limit".
            batch_consistency (TWeaviateBatchConsistency, optional): Replica nodes that must
                acknowledge a write: "ONE", "QUORUM" or "ALL".
            collection_config (t.Dict[str, t.Any], optional): Extra arguments passed to
                `collections.create` for every collection, such as `replication_config`,
                `generative_config` or `inverted_index_config`.
            multi_tenancy (bool, optional): Create collections as multi-tenant.
            tenant (str, optional): Tenant to load into. Requires `multi_tenancy`.
            auto_tenant_creation (bool, optional): Create the tenant on first write instead of
                failing. Defaults to True.
            auto_tenant_activation (bool, optional): Activate an inactive tenant on any
                operation against it instead of failing. Defaults to True.
            skip_init_checks (bool, optional): Skip the client startup handshake. Defaults to True
                for "cloud" and "custom", False for "local".
            conn_timeout (float, optional): Seconds for the connection handshake.
            read_timeout (float, optional): Seconds for queries and inserts.
            init_timeout (float, optional): Overrides `conn_timeout` for the handshake.
            query_timeout (float, optional): Overrides `read_timeout` for queries.
            insert_timeout (float, optional): Overrides `read_timeout` for inserts.
            stream_timeout (float, optional): Seconds for a server-side batch stream.
            proxies (t.Dict[str, str], optional): Proxies passed to the Weaviate client.
            trust_env (bool, optional): Let the client read proxy settings from the environment.
            destination_name (str, optional): Name of the destination. Defaults to None.
            environment (str, optional): Environment name. Defaults to None.
            **kwargs (t.Any, optional): Additional arguments forwarded to the destination config
        """
        super().__init__(
            credentials=credentials,
            vectorizer=vectorizer,
            module_config=module_config,
            connection_type=connection_type,
            batch_mode=batch_mode,
            batch_size=batch_size,
            batch_workers=batch_workers,
            batch_requests_per_minute=batch_requests_per_minute,
            batch_consistency=batch_consistency,
            collection_config=collection_config,
            multi_tenancy=multi_tenancy,
            tenant=tenant,
            auto_tenant_creation=auto_tenant_creation,
            auto_tenant_activation=auto_tenant_activation,
            skip_init_checks=skip_init_checks,
            conn_timeout=conn_timeout,
            read_timeout=read_timeout,
            init_timeout=init_timeout,
            query_timeout=query_timeout,
            insert_timeout=insert_timeout,
            stream_timeout=stream_timeout,
            proxies=proxies,
            trust_env=trust_env,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


weaviate.register()
