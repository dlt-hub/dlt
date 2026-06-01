from __future__ import annotations

import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.normalizers.naming import NamingConvention

from dlt.destinations.impl.hotdata.configuration import (
    HotdataClientConfiguration,
    HotdataCredentials,
)

if t.TYPE_CHECKING:
    from dlt.destinations.impl.hotdata.hotdata import HotdataClient


class hotdata(Destination[HotdataClientConfiguration, "HotdataClient"]):
    spec = HotdataClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "parquet"
        caps.supported_loader_file_formats = ["parquet"]
        caps.preferred_staging_file_format = None
        caps.supported_staging_file_formats = []
        caps.loader_parallelism_strategy = "table-sequential"
        caps.max_table_nesting = 1000
        caps.naming_convention = "snake_case"
        caps.has_case_sensitive_identifiers = False
        caps.max_identifier_length = 255
        caps.max_column_identifier_length = 255
        caps.supports_ddl_transactions = False
        caps.supported_merge_strategies = ["upsert", "insert-only"]
        caps.supported_replace_strategies = ["truncate-and-insert"]
        return caps

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: HotdataClientConfiguration,
        naming: t.Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        caps = super().adjust_capabilities(caps, config, naming)
        if config.max_table_nesting is not None:
            caps.max_table_nesting = config.max_table_nesting
        if config.loader_parallelism_strategy is not None:
            caps.loader_parallelism_strategy = config.loader_parallelism_strategy
        return caps

    @property
    def client_class(self) -> t.Type["HotdataClient"]:
        from dlt.destinations.impl.hotdata.hotdata import HotdataClient

        return HotdataClient

    def __init__(
        self,
        credentials: t.Union[HotdataCredentials, t.Dict[str, t.Any], str] = None,
        database_name: str = None,
        schema: str = None,
        write_disposition: str = None,
        declared_tables: t.Optional[t.List[str]] = None,
        create_database_if_missing: bool = None,
        api_base_url: str = None,
        max_retries: int = None,
        retry_backoff_seconds: float = None,
        max_table_nesting: int = None,
        loader_parallelism_strategy: str = None,
        destination_name: str = None,
        environment: str = None,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(
            credentials=credentials,
            database_name=database_name,
            schema=schema,
            write_disposition=write_disposition,
            declared_tables=declared_tables,
            create_database_if_missing=create_database_if_missing,
            api_base_url=api_base_url,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
            max_table_nesting=max_table_nesting,
            loader_parallelism_strategy=loader_parallelism_strategy,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


hotdata.register()
