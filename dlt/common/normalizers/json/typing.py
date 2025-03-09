from typing import Dict, Optional

from dlt.common.schema.typing import TColumnName, TypedDict


class RelationalNormalizerConfigPropagation(TypedDict, total=False):
    root: Optional[Dict[TColumnName, TColumnName]]
    tables: Optional[Dict[str, Dict[TColumnName, TColumnName]]]


class RelationalNormalizerConfig(TypedDict, total=False):
    max_nesting: Optional[int]
    propagation: Optional[RelationalNormalizerConfigPropagation]
