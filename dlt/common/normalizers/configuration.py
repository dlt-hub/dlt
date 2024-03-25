from typing import ClassVar, Optional, TYPE_CHECKING

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration, known_sections
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.normalizers.typing import TJSONNormalizer
from dlt.common.typing import DictStrAny


@configspec
class NormalizersConfiguration(BaseConfiguration):
    # always in section
    __section__: ClassVar[str] = known_sections.SCHEMA

    naming: Optional[str] = None
    json_normalizer: Optional[DictStrAny] = None
    destination_capabilities: Optional[DestinationCapabilitiesContext] = None  # injectable

    def on_resolved(self) -> None:
        # get naming from capabilities if not present
        if self.naming is None:
            if self.destination_capabilities:
                self.naming = self.destination_capabilities.naming_convention
        # if max_table_nesting is set, we need to set the max_table_nesting in the json_normalizer
        if (
            self.destination_capabilities
            and self.destination_capabilities.max_table_nesting is not None
        ):
            self.json_normalizer = self.json_normalizer or {}
            self.json_normalizer.setdefault("config", {})
            self.json_normalizer["config"][
                "max_nesting"
            ] = self.destination_capabilities.max_table_nesting
