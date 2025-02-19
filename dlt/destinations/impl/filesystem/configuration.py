import dataclasses

from typing import Final, Optional, Type

from dlt.common import logger
from dlt.common.configuration import configspec, resolve_type
from dlt.common.destination.client import (
    CredentialsConfiguration,
    DestinationClientStagingConfiguration,
)

from dlt.common.storages import FilesystemConfiguration

from dlt.destinations.impl.filesystem.typing import TCurrentDateTime, TExtraPlaceholders
from dlt.destinations.configuration import WithLocalFiles
from dlt.destinations.path_utils import check_layout, get_unused_placeholders


@configspec
class FilesystemDestinationClientConfiguration(FilesystemConfiguration, WithLocalFiles, DestinationClientStagingConfiguration):  # type: ignore[misc]
    destination_type: Final[str] = dataclasses.field(  # type: ignore[misc]
        default="filesystem", init=False, repr=False, compare=False
    )
    current_datetime: Optional[TCurrentDateTime] = None
    extra_placeholders: Optional[TExtraPlaceholders] = None
    max_state_files: int = 100
    """Maximum number of pipeline state files to keep; 0 or negative value disables cleanup."""

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        # use known credentials or empty credentials for unknown protocol
        return (
            self.PROTOCOL_CREDENTIALS.get(self.protocol)
            or Optional[CredentialsConfiguration]  # type: ignore[return-value]
        )

    def on_resolved(self) -> None:
        # Validate layout and show unused placeholders
        _, layout_placeholders = check_layout(self.layout, self.extra_placeholders)
        unused_placeholders = get_unused_placeholders(
            layout_placeholders, list((self.extra_placeholders or {}).keys())
        )
        if unused_placeholders:
            logger.info(f"Found unused layout placeholders: {', '.join(unused_placeholders)}")

    def normalize_bucket_url(self) -> None:
        # here we deal with normalized file:// local paths
        if self.is_local_filesystem:
            # convert to native path
            try:
                local_file_path = self.make_local_path(self.bucket_url)
            except ValueError:
                local_file_path = self.bucket_url
            relocated_path = self.make_location(local_file_path, "%s")
            # convert back into file:// schema if relocated
            if local_file_path != relocated_path:
                if self.bucket_url.startswith("file:"):
                    self.bucket_url = self.make_file_url(relocated_path)
                else:
                    self.bucket_url = relocated_path
        # modified local path before it is normalized
        super().normalize_bucket_url()
