from typing import List, Optional, TypedDict, Literal

from dlt.common.typing import StrAny


TRowIdType = Literal["random", "row_hash", "key_hash"]


class TJSONNormalizer(TypedDict, total=False):
    module: str
    config: Optional[StrAny]  # config is a free form and is consumed by `module`


class TNormalizersConfig(TypedDict, total=False):
    names: str
    detections: Optional[List[str]]
    json: TJSONNormalizer
