import typing as t

from dlt.common.destination import Destination, DestinationCapabilitiesContext

from dlt.common.destination.capabilities import TLoaderFileFormat
from dlt.common.normalizers.naming.naming import NamingConvention
from dlt.destinations.impl.dummy.configuration import (
    DummyClientConfiguration,
    DummyClientCredentials,
)

if t.TYPE_CHECKING:
    from dlt.destinations.impl.dummy.dummy import DummyClient


class dummy(Destination[DummyClientConfiguration, "DummyClient"]):
    spec = DummyClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_staging_file_format = None
        caps.has_case_sensitive_identifiers = True
        caps.max_identifier_length = 127
        caps.max_column_identifier_length = 127
        caps.max_query_length = 8 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 65536
        caps.is_max_text_data_type_length_in_bytes = True
        caps.supports_ddl_transactions = False
        caps.supported_merge_strategies = ["delete-insert", "upsert"]

        return caps

    @property
    def client_class(self) -> t.Type["DummyClient"]:
        from dlt.destinations.impl.dummy.dummy import DummyClient

        return DummyClient

    def __init__(
        self,
        credentials: DummyClientCredentials = None,
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

    @classmethod
    def adjust_capabilities(
        cls,
        caps: DestinationCapabilitiesContext,
        config: DummyClientConfiguration,
        naming: t.Optional[NamingConvention],
    ) -> DestinationCapabilitiesContext:
        caps = super().adjust_capabilities(caps, config, naming)
        additional_formats: t.List[TLoaderFileFormat] = (
            ["reference"]
            if (config.create_followup_jobs or config.create_followup_table_chain_reference_jobs)
            else []
        )
        caps.preferred_loader_file_format = config.loader_file_format
        caps.supported_loader_file_formats = additional_formats + [config.loader_file_format]
        caps.supported_staging_file_formats = additional_formats + [config.loader_file_format]
        return caps
