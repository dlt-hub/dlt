import dataclasses

from typing import Final, Optional, Type

from dlt.common import logger
from dlt.common.configuration import configspec, resolve_type
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
