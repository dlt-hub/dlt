from typing import Any, Dict, Type

from dlt.common.data_types.type_helpers import PY_TYPE_TO_SC_TYPE, py_type_to_sc_type
from dlt.common.data_types.typing import TDataType
from dlt.common.normalizers.json.relational import (
    DataItemNormalizer as RelationalNormalizer,
)


class DataItemNormalizer(RelationalNormalizer):
    """Normalizer that never coerces across types.

    Type mismatches always produce variant columns instead of attempting
    value conversion.
    """

    @property
    def py_type_to_sc_type_map(self) -> Dict[Type[Any], TDataType]:
        return PY_TYPE_TO_SC_TYPE

    def py_type_to_sc_type(self, t: Type[Any]) -> TDataType:
        return py_type_to_sc_type(t)

    def can_coerce_type(self, to_type: TDataType, from_type: TDataType) -> bool:
        return to_type == from_type

    def coerce_type(self, to_type: TDataType, from_type: TDataType, value: Any) -> Any:
        if to_type == from_type:
            return value
        raise ValueError(f"No cross-type coercion: cannot convert {from_type} to {to_type}")
