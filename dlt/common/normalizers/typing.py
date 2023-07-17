import typing as t

from dlt.common.typing import StrAny


class TJSONNormalizer(t.TypedDict, total=False):
    module: str
    config: t.Optional[StrAny]  # config is a free form and is consumed by `module`


class TNormalizersConfig(t.TypedDict, total=False):
    names: str
    detections: t.Optional[t.List[str]]
    json: TJSONNormalizer
