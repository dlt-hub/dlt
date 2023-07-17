import dataclasses
import typing as t

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.normalizers.typing import TJSONNormalizer
from dlt.common.typing import StrAny


@configspec(init=True)
class NormalizersConfiguration(BaseConfiguration):
    # always in section
    __section__: str = "schema"

    naming: str
    json_normalizer: StrAny = dataclasses.field(
        default_factory=lambda: dict({"module": "dlt.common.normalizers.json.relational"})
    )
    destination_capabilities: t.Optional[DestinationCapabilitiesContext] = None  # injectable

    def on_partial(self) -> None:
        if self.naming is None:
            if self.destination_capabilities:
                self.naming = self.destination_capabilities.naming_convention
            else:
                self.naming = "snake_case"
            # is resolved
            self.resolve()
        else:
            raise self.__exception__

    if t.TYPE_CHECKING:

        def __init__(self, naming: str = None, json_normalizer: TJSONNormalizer = None) -> None:
            ...
