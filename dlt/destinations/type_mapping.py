from typing import Tuple, Dict, Optional

from dlt.common import logger
from dlt.common.destination import PreparedTableSchema
from dlt.common.schema.typing import (
    TColumnSchema,
    TDataType,
    TColumnType,
)
from dlt.common.destination.capabilities import DataTypeMapper
from dlt.common.typing import TLoaderFileFormat
from dlt.common.utils import without_none


# NOTE it seems that multiple methods pass `table` only for logging/exception purposes
# should refactor to remove `table` kwarg and have callers catch exceptions
class TypeMapperImpl(DataTypeMapper):
    sct_to_unbound_dbt: Dict[TDataType, str]
    """Data types without precision or scale specified (e.g. `"text": "varchar"` in postgres)"""
    sct_to_dbt: Dict[TDataType, str]
    """Data types that require a precision or scale (e.g. `"text": "varchar(%i)"` or `"decimal": "numeric(%i,%i)"` in postgres).
    Values should have printf placeholders for precision (and scale if applicable)
    """

    dbt_to_sct: Dict[str, TDataType]

    def ensure_supported_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema,
        loader_file_format: TLoaderFileFormat,
    ) -> None:
        pass

    def to_db_integer_type(self, column: TColumnSchema, table: PreparedTableSchema = None) -> str:
        # Override in subclass if db supports other integer types (e.g. smallint, integer, tinyint, etc.)
        return self.sct_to_unbound_dbt["bigint"]

    def to_db_datetime_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema = None,
    ) -> str:
        # Override in subclass if db supports other timestamp types (e.g. with different time resolutions)
        timezone = column.get("timezone")
        precision = column.get("precision")

        if timezone is not None or precision is not None:
            message = (
                "Column flags for timezone or precision are not yet supported in this"
                " destination. One or both of these flags were used in column"
                f" '{column.get('name')}'."
            )
            # TODO: refactor lancedb and wevavite to make table object required
            if table:
                message += f" in table '{table.get('name')}'."

            logger.warning(message)

        return None

    def to_db_time_type(self, column: TColumnSchema, table: PreparedTableSchema = None) -> str:
        # Override in subclass if db supports other time types (e.g. with different time resolutions)
        return None

    def to_db_decimal_type(self, column: TColumnSchema) -> str:
        precision_tup = self.decimal_precision(column.get("precision"), column.get("scale"))
        if not precision_tup or "decimal" not in self.sct_to_dbt:
            return self.sct_to_unbound_dbt["decimal"]
        return self.sct_to_dbt["decimal"] % (precision_tup[0], precision_tup[1])

    # TODO: refactor lancedb and weaviate to make table object required
    def to_destination_type(self, column: TColumnSchema, table: PreparedTableSchema) -> str:
        sc_t = column["data_type"]
        if sc_t == "bigint":
            db_t = self.to_db_integer_type(column, table)
        elif sc_t == "timestamp":
            db_t = self.to_db_datetime_type(column, table)
        elif sc_t == "time":
            db_t = self.to_db_time_type(column, table)
        elif sc_t == "decimal":
            db_t = self.to_db_decimal_type(column)
        else:
            db_t = None
        if db_t:
            return db_t
        # try templates with precision
        bounded_template = self.sct_to_dbt.get(sc_t)
        if not bounded_template:
            return self.sct_to_unbound_dbt[sc_t]
        precision_tuple = self.precision_tuple_or_default(sc_t, column)
        if not precision_tuple:
            return self.sct_to_unbound_dbt[sc_t]
        return self.sct_to_dbt[sc_t] % precision_tuple

    def precision_tuple_or_default(
        self, data_type: TDataType, column: TColumnSchema
    ) -> Optional[Tuple[int, ...]]:
        precision = column.get("precision")
        scale = column.get("scale")
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

    def from_destination_type(
        self, db_type: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return without_none(
            dict(  # type: ignore[return-value]
                data_type=self.dbt_to_sct.get(db_type, "text"), precision=precision, scale=scale
            )
        )
