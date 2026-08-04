from typing import Dict, Optional, cast
from dlt.common import logger
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.exceptions import TerminalValueError
from dlt.common.libs.pyarrow import get_py_arrow_datatype, pyarrow as pa

from dlt.common.schema.typing import TColumnSchema, TColumnType, TDataType
from dlt.destinations.type_mapping import TypeMapperImpl

TIMESTAMP_PRECISION_TO_UNIT: Dict[int, str] = {0: "s", 3: "ms", 6: "us", 9: "ns"}
UNIT_TO_TIMESTAMP_PRECISION: Dict[str, int] = {v: k for k, v in TIMESTAMP_PRECISION_TO_UNIT.items()}

ARROW_TYPE_NAMES: Dict[pa.DataType, str] = {
    pa.null(): "Null",
    pa.bool_(): "Boolean",
    pa.int8(): "Int8",
    pa.int16(): "Int16",
    pa.int32(): "Int32",
    pa.int64(): "Int64",
    pa.uint8(): "UInt8",
    pa.uint16(): "UInt16",
    pa.uint32(): "UInt32",
    pa.uint64(): "UInt64",
    pa.float16(): "Float16",
    pa.float32(): "Float32",
    pa.float64(): "Float64",
    pa.string(): "Utf8",
    pa.large_string(): "LargeUtf8",
    pa.binary(): "Binary",
    pa.large_binary(): "LargeBinary",
    pa.date32(): "Date32",
    pa.date64(): "Date64",
}
"""DataFusion names of the arrow types, as accepted by `arrow_cast`."""
ARROW_TIME_UNIT_NAMES: Dict[str, str] = {
    "s": "Second",
    "ms": "Millisecond",
    "us": "Microsecond",
    "ns": "Nanosecond",
}


def arrow_type_to_datafusion(arrow_type: pa.DataType) -> str:
    """Returns the DataFusion name of an arrow type, which `arrow_cast` takes as a string."""
    if name := ARROW_TYPE_NAMES.get(arrow_type):
        return name
    if pa.types.is_timestamp(arrow_type):
        timezone = f'Some("{arrow_type.tz}")' if arrow_type.tz else "None"
        return f"Timestamp({ARROW_TIME_UNIT_NAMES[arrow_type.unit]}, {timezone})"
    if pa.types.is_time32(arrow_type) or pa.types.is_time64(arrow_type):
        width = 32 if pa.types.is_time32(arrow_type) else 64
        return f"Time{width}({ARROW_TIME_UNIT_NAMES[arrow_type.unit]})"
    if pa.types.is_decimal128(arrow_type) or pa.types.is_decimal256(arrow_type):
        width = 128 if pa.types.is_decimal128(arrow_type) else 256
        return f"Decimal{width}({arrow_type.precision}, {arrow_type.scale})"
    if pa.types.is_fixed_size_list(arrow_type):
        value_type = arrow_type_to_datafusion(arrow_type.value_type)
        return f"FixedSizeList({arrow_type.list_size}, {value_type})"
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        prefix = "LargeList" if pa.types.is_large_list(arrow_type) else "List"
        return f"{prefix}({arrow_type_to_datafusion(arrow_type.value_type)})"
    raise TerminalValueError(
        f"Arrow type `{arrow_type}` has no DataFusion name, so a column of this type cannot be"
        " added to an existing LanceDB table."
    )


# TODO: TypeMapperImpl must be a Generic where pa.DataType will be a concrete class
class LanceDBTypeMapper(TypeMapperImpl):
    sct_to_dbt = {}

    dbt_to_sct = {
        pa.string(): "text",
        pa.float64(): "double",
        pa.bool_(): "bool",
        pa.int64(): "bigint",
        pa.binary(): "binary",
        pa.date32(): "date",
    }

    def to_destination_type(self, column: TColumnSchema, table: PreparedTableSchema) -> pa.DataType:
        # reuse existing type mapper
        dt_ = get_py_arrow_datatype(column, self.capabilities, "UTC")
        if column["data_type"] == "timestamp":
            column_name = column.get("name")
            timezone = column.get("timezone")
            precision = column.get("precision")
            if timezone is not None or precision is not None:
                logger.warning(
                    "LanceDB does not currently support column flags for timezone or precision."
                    f" These flags were used in column '{column_name}'."
                )
        return dt_

    def from_destination_type(
        self,
        db_type: pa.DataType,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
    ) -> TColumnType:
        # TODO: use pyarrow helpers to convert type, this is code duplication
        if isinstance(db_type, pa.TimestampType):
            return dict(
                data_type="timestamp",
                precision=UNIT_TO_TIMESTAMP_PRECISION[db_type.unit],
                scale=scale,
            )
        if isinstance(db_type, pa.Time64Type):
            return dict(
                data_type="time",
                precision=UNIT_TO_TIMESTAMP_PRECISION[db_type.unit],
                scale=scale,
            )
        if isinstance(db_type, pa.Decimal128Type):
            precision, scale = db_type.precision, db_type.scale
            if (precision, scale) == self.capabilities.wei_precision:
                return cast(TColumnType, dict(data_type="wei"))
            return dict(data_type="decimal", precision=precision, scale=scale)
        return super().from_destination_type(db_type, precision, scale)

    def to_null_column_expression(self, column: TColumnSchema) -> str:
        """Returns the expression adding `column` to a table, filled with nulls."""
        # the cluster plans column additions as SQL, so `arrow_cast` carries the arrow type
        arrow_type = self.to_destination_type(column, None)
        return f"arrow_cast(NULL, '{arrow_type_to_datafusion(arrow_type)}')"
