from typing import Tuple, ClassVar, Dict, Optional

from dlt.common.schema.typing import TColumnSchema, TDataType
from dlt.common.destination.capabilities import DestinationCapabilitiesContext


class TypeMapper:
    capabilities: DestinationCapabilitiesContext

    sct_to_unbound_dbt: Dict[TDataType, str]
    """Data types without precision or scale specified (e.g. `"text": "varchar"` in postgres)"""
    sct_to_dbt: Dict[TDataType, str]
    """Data types that require a precision or scale (e.g. `"text": "varchar(%i)"` or `"decimal": "numeric(%i,%i)"` in postgres).
    Values should have printf placeholders for precision (and scale if applicable)
    """

    def __init__(self, capabilities: DestinationCapabilitiesContext) -> None:
        self.capabilities = capabilities

    def to_db_type(self, column: TColumnSchema) -> str:
        precision, scale = column.get("precision"), column.get("scale")
        sc_t = column["data_type"]
        bounded_template = self.sct_to_dbt.get(sc_t)
        precision_tuple = self.precision_tuple_or_default(sc_t, precision, scale)
        if not precision_tuple or not bounded_template:
            return self.sct_to_unbound_dbt[sc_t]
        return self.sct_to_dbt[sc_t] % precision_tuple

    def precision_tuple_or_default(self, data_type: TDataType, precision: Optional[int], scale: Optional[int]) -> Optional[Tuple[int, ...]]:
        if data_type in ("timestamp", "time"):
            return (precision or self.capabilities.timestamp_precision, )
        elif data_type == "decimal":
            return self.decimal_precision(precision, scale)
        elif data_type == "wei":
            return self.wei_precision(precision, scale)

        if precision is None:
            return None
        elif scale is None:
            return (precision, )
        return (precision, scale)

    def decimal_precision(self, precision: Optional[int], scale: Optional[int]) -> Tuple[int, int]:
        default_precision, default_scale = self.capabilities.decimal_precision
        return (
            precision if precision is not None else default_precision, scale if scale is not None else default_scale
        )

    def wei_precision(self, precision: Optional[int], scale: Optional[int]) -> Tuple[int, int]:
        default_precision, default_scale = self.capabilities.wei_precision
        return (
            precision if precision is not None else default_precision, scale if scale is not None else default_scale
        )

    def timestamp_precision(self, precision: Optional[int]) -> int:
        return precision or self.capabilities.timestamp_precision
