from typing import List, Optional, TYPE_CHECKING, cast

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.schema.typing import TJSONNormalizer, TNormalizersConfig
from dlt.common.typing import StrAny


@configspec(init=True)
class SchemaNormalizersConfiguration(BaseConfiguration):
    # always in namespace
    __namespace__: str = "schema"

    detections: Optional[List[str]] = None
    names: str
    json: StrAny

    def to_native_representation(self) -> TNormalizersConfig:
        return cast(TNormalizersConfig, dict(self))


    if TYPE_CHECKING:
        def __init__(self, detections: Optional[List[str]] = None, names: str = None, json: TJSONNormalizer = None) -> None:
            ...