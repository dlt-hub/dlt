from typing import Any, Type

from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.destination.reference import (
    Destination,
    DestinationClientConfiguration,
    JobClientBase,
)


class null(Destination[DestinationClientConfiguration, "JobClientBase"]):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    spec = DestinationClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        return DestinationCapabilitiesContext.generic_capabilities()

    @property
    def client_class(self) -> Type["JobClientBase"]:
        return JobClientBase
