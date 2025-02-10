from typing import Dict, Optional, TypedDict

from dlt.common.schema.typing import TColumnName


class RelationalNormalizerConfigPropagation(TypedDict, total=False):
    root: Optional[Dict[TColumnName, TColumnName]]
    tables: Optional[Dict[str, Dict[TColumnName, TColumnName]]]


class RelationalNormalizerConfig(TypedDict, total=False):
    max_nesting: Optional[int]
    propagation: Optional[RelationalNormalizerConfigPropagation]
