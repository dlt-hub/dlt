from typing import Tuple, ClassVar, Dict, Optional

from dlt.common.schema.typing import TColumnSchema, TDataType, TColumnType, TTableFormat
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.utils import without_none


class TypeMapper:
    capabilities: DestinationCapabilitiesContext

    sct_to_unbound_dbt: Dict[TDataType, str]
    """Data types without precision or scale specified (e.g. `"text": "varchar"` in postgres)"""
    sct_to_dbt: Dict[TDataType, str]
    """Data types that require a precision or scale (e.g. `"text": "varchar(%i)"` or `"decimal": "numeric(%i,%i)"` in postgres).
    Values should have printf placeholders for precision (and scale if applicable)
    """

    dbt_to_sct: Dict[str, TDataType]

    def __init__(self, capabilities: DestinationCapabilitiesContext) -> None:
        self.capabilities = capabilities

    def to_db_integer_type(
        self, precision: Optional[int], table_format: TTableFormat = None
    ) -> str:
        # Override in subclass if db supports other integer types (e.g. smallint, integer, tinyint, etc.)
        return self.sct_to_unbound_dbt["bigint"]

    def to_db_datetime_type(
        self, precision: Optional[int], table_format: TTableFormat = None
    ) -> str:
        # Override in subclass if db supports other timestamp types (e.g. with different time resolutions)
        return None

    def to_db_time_type(self, precision: Optional[int], table_format: TTableFormat = None) -> str:
        # Override in subclass if db supports other time types (e.g. with different time resolutions)
        return None

    def to_db_type(self, column: TColumnSchema, table_format: TTableFormat = None) -> str:
        precision, scale = column.get("precision"), column.get("scale")
        sc_t = column["data_type"]
        if sc_t == "bigint":
            db_t = self.to_db_integer_type(precision, table_format)
        elif sc_t == "timestamp":
            db_t = self.to_db_datetime_type(precision, table_format)
        elif sc_t == "time":
            db_t = self.to_db_time_type(precision, table_format)
        else:
            db_t = None
        if db_t:
            return db_t
        # try templates with precision
        bounded_template = self.sct_to_dbt.get(sc_t)
        if not bounded_template:
            return self.sct_to_unbound_dbt[sc_t]
        precision_tuple = self.precision_tuple_or_default(sc_t, precision, scale)
        if not precision_tuple:
            return self.sct_to_unbound_dbt[sc_t]
        return self.sct_to_dbt[sc_t] % precision_tuple

    def precision_tuple_or_default(
        self, data_type: TDataType, precision: Optional[int], scale: Optional[int]
    ) -> Optional[Tuple[int, ...]]:
        if data_type in ("timestamp", "time"):
            if precision is None:
                return None  # Use default which is usually the max
        elif data_type == "decimal":
            return self.decimal_precision(precision, scale)
        elif data_type == "wei":
            return self.wei_precision(precision, scale)

        if precision is None:
            return None
        elif scale is None:
            return (precision,)
        return (precision, scale)

    def decimal_precision(
        self, precision: Optional[int] = None, scale: Optional[int] = None
    ) -> Optional[Tuple[int, int]]:
        defaults = self.capabilities.decimal_precision
        if not defaults:
            return None
        default_precision, default_scale = defaults
        return (
            precision if precision is not None else default_precision,
            scale if scale is not None else default_scale,
        )

    def wei_precision(
        self, precision: Optional[int] = None, scale: Optional[int] = None
    ) -> Optional[Tuple[int, int]]:
        defaults = self.capabilities.wei_precision
        if not defaults:
            return None
        default_precision, default_scale = defaults
        return (
            precision if precision is not None else default_precision,
            scale if scale is not None else default_scale,
        )

    def from_db_type(
        self, db_type: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return without_none(
            dict(  # type: ignore[return-value]
                data_type=self.dbt_to_sct.get(db_type, "text"), precision=precision, scale=scale
            )
        )
