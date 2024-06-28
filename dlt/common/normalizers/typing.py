from typing import List, Optional, TypedDict, Literal, Union

from dlt.common.typing import StrAny
from dlt.common.normalizers.naming import NamingConvention


TRowIdType = Literal["random", "row_hash", "key_hash"]


class TJSONNormalizer(TypedDict, total=False):
    module: str
    config: Optional[StrAny]  # config is a free form and is validated by `module`


class TNormalizersConfig(TypedDict, total=False):
    names: Union[str, NamingConvention]
    allow_identifier_change_on_table_with_data: Optional[bool]
    detections: Optional[List[str]]
    json: TJSONNormalizer
