from typing import List, Optional, Type, Literal, Union
from types import ModuleType

from dlt.common.typing import StrAny, TypedDict
from dlt.common.normalizers.naming import NamingConvention

TNamingConventionReferenceArg = Union[str, Type[NamingConvention], ModuleType]


TRowIdType = Literal["random", "row_hash", "key_hash"]


class TJSONNormalizer(TypedDict, total=False):
    module: str
    config: Optional[StrAny]  # config is a free form and is validated by `module`


class TNormalizersConfig(TypedDict, total=False):
    names: str
    allow_identifier_change_on_table_with_data: Optional[bool]
    use_break_path_on_normalize: Optional[bool]
    """Post 1.4.0 to allow table and column names that contain table separators"""
    detections: Optional[List[str]]
    json: TJSONNormalizer
