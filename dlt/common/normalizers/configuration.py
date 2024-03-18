from typing import Optional, TYPE_CHECKING, Union

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.normalizers.typing import TJSONNormalizer
from dlt.common.typing import StrAny


@configspec
class NormalizersConfiguration(BaseConfiguration):
    # always in section
    __section__: str = "schema"

    naming: Optional[Union[str, NamingConvention]] = None
    json_normalizer: Optional[StrAny] = None
    allow_identifier_change_on_table_with_data: Optional[bool] = None
    destination_capabilities: Optional[DestinationCapabilitiesContext] = None  # injectable

    def on_resolved(self) -> None:
        # get naming from capabilities if not present
        if self.naming is None:
            if self.destination_capabilities:
                self.naming = self.destination_capabilities.naming_convention

    if TYPE_CHECKING:

        def __init__(self, naming: str = None, json_normalizer: TJSONNormalizer = None) -> None: ...
