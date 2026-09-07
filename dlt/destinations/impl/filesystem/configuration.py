import dataclasses

import os
from typing import ClassVar, Dict, Final, Optional, Tuple, Type
from urllib.parse import urlparse

from dlt.common.typing import DictStrAny, DictStrOptionalStr

from dlt.common import logger
from dlt.common.configuration import configspec, resolve_type
from dlt.common.configuration.specs.hf_credentials import HfCredentials
from dlt.common.destination.attach import TAttachType
from dlt.common.destination.client import (
    CredentialsConfiguration,
    DestinationClientConfiguration,
    DestinationClientDwhConfiguration,
    DestinationClientStagingConfiguration,
    WithAttachableEngine,
)
from dlt.common.storages import FilesystemConfigurationWithLocalFiles

from dlt.destinations.impl.filesystem.typing import TCurrentDateTime, TExtraPlaceholders
from dlt.destinations.path_utils import check_layout, get_unused_placeholders


@configspec
class FilesystemDestinationClientConfiguration(  # type: ignore[misc]
    WithAttachableEngine,
    FilesystemConfigurationWithLocalFiles,
    DestinationClientStagingConfiguration,
):
    destination_type: Final[str] = dataclasses.field(default="filesystem", init=False, repr=False, compare=False)  # type: ignore[misc]
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
    iceberg_use_catalog_purge: bool = False
    """When true, dropped iceberg tables are purged via the persistent catalog which may leave
    files in place (e.g. rejected purge, deferred GC). When false, the table is dropped from
    the catalog and dlt deletes the table files itself."""

    ATTACHABLE_PROTOCOLS: ClassVar[Tuple[str, ...]] = ("file", "s3", "az", "abfss", "hf")
    """Protocols a foreign DuckDB engine reads with SQL alone. These protocols mirror the
    `CREATE SECRET` statements that `DuckDbSqlClient` builds. The other protocols (`gs`, `sftp`,
    `gdrive`) access their data through an fsspec filesystem that only the process which
    registered it holds."""

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        return super().resolve_credentials_type()

    def attach_type(self) -> Optional[TAttachType]:
        """Returns None for a protocol a foreign DuckDB engine cannot access with SQL."""
        if self.protocol not in self.ATTACHABLE_PROTOCOLS:
            return None
        return super().attach_type()

    def data_location(self) -> str:
        """Returns scheme://netloc for remote filesystems, or the absolute local path."""
        if not self.bucket_url:
            self._no_data_location("the configuration has no `bucket_url`")

        if self.is_local_path(self.bucket_url):
            return self.make_local_path(self.make_file_url(self.bucket_url))

        url = urlparse(self.bucket_url)
        if url.scheme == "file":
            return self.make_local_path(self.bucket_url)
        return f"{url.scheme}://{url.netloc}"

    def can_write_from(self, other: DestinationClientConfiguration) -> bool:
        """Filesystem does not have an engine that can write. `dlt` is that engine,
        and setting False here we enforce it's usage
        """
        return False

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
    hf_show_progress: bool = False
    """Show `huggingface_hub` upload and commit progress bars in the terminal. Disabled by default so
    that `dlt` controls progress reporting via `dlt.pipeline(progress=...)`."""

    @property
    def hf_namespace(self) -> str:
        return os.path.basename(self.bucket_url)
