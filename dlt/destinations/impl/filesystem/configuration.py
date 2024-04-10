import dataclasses

from typing import Final, List, Optional, Type

from dlt.cli import echo as fmt
from dlt.common.configuration import configspec, resolve_type
from dlt.common.destination.reference import (
    CredentialsConfiguration,
    DestinationClientStagingConfiguration,
)

from dlt.common.storages import FilesystemConfiguration
from dlt.destinations.impl.filesystem.typing import TCurrentDateTime, TExtraPlaceholders
from pendulum import DateTime

from dlt.destinations.path_utils import check_layout


@configspec
class FilesystemDestinationClientConfiguration(FilesystemConfiguration, DestinationClientStagingConfiguration):  # type: ignore[misc]
    destination_type: Final[str] = dataclasses.field(  # type: ignore[misc]
        default="filesystem", init=False, repr=False, compare=False
    )
    current_datetime: Optional[TCurrentDateTime] = None
    extra_placeholders: Optional[TExtraPlaceholders] = None

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        # use known credentials or empty credentials for unknown protocol
        return (
            self.PROTOCOL_CREDENTIALS.get(self.protocol)
            or Optional[CredentialsConfiguration]  # type: ignore[return-value]
        )

    def on_resolved(self) -> None:
        if callable(self.current_datetime):
            current_datetime = self.current_datetime()
            if not isinstance(current_datetime, DateTime):
                raise RuntimeError(
                    "current_datetime is not an instance instance of pendulum.DateTime"
                )

        # Validate layout and show unused placeholders
        _, layout_placeholders = check_layout(self.layout, self.extra_placeholders)
        unused_placeholders: List[str] = []
        if self.extra_placeholders:
            unused_placeholders = [
                p for p in self.extra_placeholders.keys() if p not in layout_placeholders
            ]
            if unused_placeholders:
                fmt.secho(
                    f"Found unused layout placeholders: {', '.join(unused_placeholders)}",
                    fg="yellow",
                )

        return super().on_resolved()
