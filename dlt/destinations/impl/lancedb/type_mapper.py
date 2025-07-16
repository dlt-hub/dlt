from typing import Dict, Optional, cast
from dlt.common import logger
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.libs.pyarrow import get_py_arrow_datatype, pyarrow as pa

from dlt.common.schema.typing import TColumnSchema, TColumnType
from dlt.destinations.type_mapping import TypeMapperImpl

TIMESTAMP_PRECISION_TO_UNIT: Dict[int, str] = {0: "s", 3: "ms", 6: "us", 9: "ns"}
UNIT_TO_TIMESTAMP_PRECISION: Dict[str, int] = {v: k for k, v in TIMESTAMP_PRECISION_TO_UNIT.items()}


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
