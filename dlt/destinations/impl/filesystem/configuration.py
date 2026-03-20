import dataclasses

import os
from typing import Dict, Final, Optional, Type

from dlt.common.typing import DictStrAny, DictStrOptionalStr

from dlt.common import logger
from dlt.common.configuration import configspec, resolve_type
from dlt.common.configuration.specs.hf_credentials import HfCredentials
from dlt.common.destination.client import (
    CredentialsConfiguration,
    DestinationClientStagingConfiguration,
)
from dlt.common.storages import FilesystemConfigurationWithLocalFiles

from dlt.destinations.impl.filesystem.typing import TCurrentDateTime, TExtraPlaceholders
from dlt.destinations.path_utils import check_layout, get_unused_placeholders


@configspec
class FilesystemDestinationClientConfiguration(FilesystemConfigurationWithLocalFiles, DestinationClientStagingConfiguration):  # type: ignore[misc]
    destination_type: Final[str] = dataclasses.field(  # type: ignore[misc]
        default="filesystem", init=False, repr=False, compare=False
    )
    current_datetime: Optional[TCurrentDateTime] = None
    extra_placeholders: Optional[TExtraPlaceholders] = None
    max_state_files: int = 100
    """Maximum number of pipeline state files to keep; 0 or negative value disables cleanup."""
    always_refresh_views: bool = False
    """Always refresh table scanner views by setting the newest table metadata or globbing table files"""
    deltalake_storage_options: Optional[DictStrAny] = None
    """Additional storage options passed to `deltalake` library, overriding credentials-derived values."""
    deltalake_configuration: Optional[DictStrOptionalStr] = None
    """Delta table configuration passed to `write_deltalake` and `create_deltalake` calls."""
    deltalake_streamed_exec: bool = True
    """When true, delta merge operations use streamed execution to reduce memory usage."""
    iceberg_table_properties: Optional[Dict[str, str]] = None
    """Default Iceberg table properties applied to all tables; per-table adapter properties take precedence."""
    iceberg_namespace_properties: Optional[Dict[str, str]] = None
    """Properties passed to the Iceberg catalog when creating the namespace."""
    iceberg_gc_collect_interval: int = 0
    """How often (in batches) to run gc.collect() during streamed Iceberg writes. Set to 0 to disable."""

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        return super().resolve_credentials_type()

    def on_resolved(self) -> None:
        # Validate layout and show unused placeholders
        _, layout_placeholders = check_layout(self.layout, self.extra_placeholders)
        unused_placeholders = get_unused_placeholders(
            layout_placeholders, list((self.extra_placeholders or {}).keys())
        )
        if unused_placeholders:
            logger.info(f"Found unused layout placeholders: {', '.join(unused_placeholders)}")


@configspec
class HfFilesystemDestinationClientConfiguration(FilesystemDestinationClientConfiguration):
    credentials: HfCredentials = None
    hf_dataset_card: bool = True
    """Create and update dataset card (README.md) with subset metadata for the Hugging Face dataset viewer."""

    @property
    def hf_namespace(self) -> str:
        return os.path.basename(self.bucket_url)
