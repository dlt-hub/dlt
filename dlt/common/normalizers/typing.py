from typing import List, Optional, TypedDict

from dlt.common.typing import StrAny


class TJSONNormalizer(TypedDict, total=False):
    module: str
    config: Optional[StrAny]  # config is a free form and is consumed by `module`


class TNormalizersConfig(TypedDict, total=False):
    names: str
    detections: Optional[List[str]]
    json: TJSONNormalizer