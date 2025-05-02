from typing import Optional

from dlt.common.utils import without_none
from dlt.common.exceptions import MissingDependencyException, TerminalValueError
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.destination.capabilities import DataTypeMapper
from dlt.common.schema.typing import (
    TColumnSchema,
    TColumnType,
    TDataType,
)

try:
    import sqlglot.expressions as sge
    from sqlglot.expressions import DataType, DATA_TYPE
except ModuleNotFoundError:
    raise MissingDependencyException("dlt sqlglot helpers", ["sqlglot"])


SQLGLOT_TO_DLT_TYPE_MAP: dict[DataType.Type, TDataType] = {
    # NESTED_TYPES
    DataType.Type.OBJECT: "json",
    DataType.Type.STRUCT: "json",
    DataType.Type.NESTED: "json",
    DataType.Type.UNION: "json",
    DataType.Type.ARRAY: "json",
    DataType.Type.LIST: "json",
    DataType.Type.JSON: "json",
    DataType.Type.VECTOR: "json",
    # TEXT
    DataType.Type.CHAR: "text",
    DataType.Type.NCHAR: "text",
    DataType.Type.NVARCHAR: "text",
    DataType.Type.TEXT: "text",
    DataType.Type.VARCHAR: "text",
    DataType.Type.NAME: "text",
    # SIGNED_INTEGER
    DataType.Type.BIGINT: "bigint",
    DataType.Type.INT: "bigint",
    DataType.Type.INT128: "bigint",
    DataType.Type.INT256: "bigint",
    DataType.Type.MEDIUMINT: "bigint",
    DataType.Type.SMALLINT: "bigint",
    DataType.Type.TINYINT: "bigint",
    # UNSIGNED_INTEGER
    DataType.Type.UBIGINT: "bigint",
    DataType.Type.UINT: "bigint",
    DataType.Type.UINT128: "bigint",
    DataType.Type.UINT256: "bigint",
    DataType.Type.UMEDIUMINT: "bigint",
    DataType.Type.USMALLINT: "bigint",
    DataType.Type.UTINYINT: "bigint",
    # other INTEGER
    DataType.Type.BIT: "bigint",
    # FLOAT
    DataType.Type.DOUBLE: "double",
    DataType.Type.FLOAT: "double",
    # DECIMAL
    DataType.Type.BIGDECIMAL: "decimal",
    DataType.Type.DECIMAL: "decimal",
    DataType.Type.DECIMAL32: "decimal",
    DataType.Type.DECIMAL64: "decimal",
    DataType.Type.DECIMAL128: "decimal",
    DataType.Type.DECIMAL256: "decimal",
    DataType.Type.MONEY: "decimal",
    DataType.Type.SMALLMONEY: "decimal",
    DataType.Type.UDECIMAL: "decimal",
    DataType.Type.UDOUBLE: "decimal",
    # TEMPORAL
    DataType.Type.DATE: "date",
    DataType.Type.DATE32: "date",
    DataType.Type.DATETIME: "date",
    DataType.Type.DATETIME2: "date",
    DataType.Type.DATETIME64: "date",
    DataType.Type.SMALLDATETIME: "date",
    DataType.Type.TIMESTAMP: "timestamp",
    DataType.Type.TIMESTAMPNTZ: "timestamp",
    DataType.Type.TIMESTAMPLTZ: "timestamp",
    DataType.Type.TIMESTAMPTZ: "timestamp",
    DataType.Type.TIMESTAMP_MS: "timestamp",
    DataType.Type.TIMESTAMP_NS: "timestamp",
    DataType.Type.TIMESTAMP_S: "timestamp",
    DataType.Type.TIME: "time",
    DataType.Type.TIMETZ: "time",
    # binary
    DataType.Type.VARBINARY: "binary",
    # BOOLEAN
    DataType.Type.BOOLEAN: "bool",
    # UNKNOWN
    DataType.Type.UNKNOWN: None,
}

SQLGLOT_INT_PRECISION = {
    DataType.Type.TINYINT: 3,
    DataType.Type.SMALLINT: 5,
    DataType.Type.MEDIUMINT: 8,
    DataType.Type.INT: 10,
    DataType.Type.BIGINT: 19,
    DataType.Type.INT128: 39,
    DataType.Type.INT256: 78,
    DataType.Type.UTINYINT: 3,
    DataType.Type.USMALLINT: 5,
    DataType.Type.UMEDIUMINT: 8,
    DataType.Type.UINT: 10,
    DataType.Type.UBIGINT: 19,
    DataType.Type.UINT128: 39,
    DataType.Type.UINT256: 78,
}

SQLGLOT_DECIMAL_PRECISION_AND_SCALE = {
    DataType.Type.BIGDECIMAL: (38, 10),
    DataType.Type.DECIMAL: (38, 10),
    DataType.Type.DECIMAL32: (7, 2),
    DataType.Type.DECIMAL64: (16, 4),
    DataType.Type.DECIMAL128: (34, 10),
    DataType.Type.DECIMAL256: (76, 20),
    DataType.Type.MONEY: (19, 4),
    DataType.Type.SMALLMONEY: (10, 4),
    DataType.Type.UDECIMAL: (38, 10),
}

SQLGLOT_TEMPORAL_PRECISION = {
    DataType.Type.TIMESTAMP_S: 0,
    DataType.Type.TIMESTAMP_MS: 3,
    DataType.Type.TIMESTAMP_NS: 9,
}

SQLGLOT_HAS_TIMEZONE = {
    DataType.Type.TIMESTAMP: False,
    DataType.Type.TIMESTAMPNTZ: False,
    DataType.Type.TIMESTAMPLTZ: True,
    DataType.Type.TIMESTAMPTZ: True,
    DataType.Type.TIMESTAMP_MS: False,
    DataType.Type.TIMESTAMP_NS: False,
    DataType.Type.TIMESTAMP_S: False,
    DataType.Type.TIME: False,
    DataType.Type.TIMETZ: True,
}

DLT_TO_SQLGLOT = {
    "json": DataType.Type.JSON,
    "text": DataType.Type.TEXT,
    "double": DataType.Type.DOUBLE,
    "bool": DataType.Type.BOOLEAN,
    "date": DataType.Type.DATE,
    "timestamp": DataType.Type.TIMESTAMP,
    "bigint": DataType.Type.BIGINT,
    "binary": DataType.Type.VARBINARY,
    "time": DataType.Type.TIME,
    "decimal": DataType.Type.DECIMAL,
    "wei": DataType.Type.UINT256,
}


def from_sqlglot_type(
    sqlglot_type: DATA_TYPE,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
    timezone: Optional[bool] = None,
) -> TColumnType:
    if isinstance(sqlglot_type, str):
        sqlglot_type = sge.DataType.build(sqlglot_type)
    elif isinstance(sqlglot_type, sge.DataType.Type):
        sqlglot_type = sge.DataType.build(sqlglot_type)
    assert isinstance(sqlglot_type, sge.DataType)

    # only the `DataType.UNKNOWN` will produce `dlt_type=None`
    dlt_type = SQLGLOT_TO_DLT_TYPE_MAP.get(sqlglot_type.this, "text")

    # TODO retrieve explicit `precision`, `scale`, and `timezone` directly from DataType.expressions

    if dlt_type == "bigint":
        precision = SQLGLOT_INT_PRECISION.get(sqlglot_type.this)
        precision = (
            None if precision == 19 else precision
        )  # 19 is the default precision, we ignore the hint

    elif dlt_type == "decimal":
        precision_and_scale = SQLGLOT_DECIMAL_PRECISION_AND_SCALE.get(sqlglot_type.this)
        if precision_and_scale is not None:
            precision = precision if precision else precision_and_scale[0]
            scale = scale if scale else precision_and_scale[1]

    elif dlt_type == "timestamp":
        precision = SQLGLOT_TEMPORAL_PRECISION.get(sqlglot_type.this)
        timezone = SQLGLOT_HAS_TIMEZONE.get(sqlglot_type.this)

    # `nullable` is not a required arg on exp.DataType
    nullable = sqlglot_type.args.get("nullable")

    return without_none(
        dict(  # type: ignore[return-value]
            data_type=dlt_type,
            precision=precision,
            scale=scale,
            timezone=timezone,
            nullable=nullable,
        )
    )


def to_sqlglot_type(
    dlt_type: TDataType,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
    timezone: Optional[bool] = None,
    nullable: Optional[bool] = None,
) -> DataType:
    sqlglot_type = DLT_TO_SQLGLOT[dlt_type]
    if dlt_type == "bigint" and precision is not None:
        sqlglot_type = _to_integer_type(precision=precision)

    elif dlt_type == "decimal" and precision is not None:
        sqlglot_type = _to_decimal_type(precision=precision, scale=scale)

    elif dlt_type == "timestamp" and (precision is not None or timezone is not None):
        sqlglot_type = _to_temporal_type(dlt_type=dlt_type, precision=precision, timezone=timezone)

    hints = {}
    if nullable is not None:
        hints["nullable"] = nullable

    return DataType.build(sqlglot_type, **hints)  # type: ignore[arg-type]


def _to_integer_type(precision: Optional[int]) -> DataType.Type:
    if precision is None:
        return DataType.Type.BIGINT
    elif precision <= 0:
        # raise TerminalValueError(f"Precision must be positive, got {precision}")
        sqlglot_type = DataType.Type.BIGINT  # default value
    elif precision <= 3:
        sqlglot_type = DataType.Type.TINYINT
    elif precision <= 5:
        sqlglot_type = DataType.Type.SMALLINT
    elif precision <= 8:
        sqlglot_type = DataType.Type.MEDIUMINT
    elif precision <= 10:
        sqlglot_type = DataType.Type.INT
    elif precision <= 19:
        sqlglot_type = DataType.Type.BIGINT
    elif precision <= 39:
        sqlglot_type = DataType.Type.INT128
    elif precision <= 78:
        sqlglot_type = DataType.Type.INT256
    else:
        raise TerminalValueError(f"Precision must be less than 78, got {precision}")

    return sqlglot_type


def _to_decimal_type(precision: Optional[int], scale: Optional[int]) -> DataType.Type:
    sqlglot_type = DataType.Type.DECIMAL
    # if precision is None:
    #     sqlglot_type = DataType.Type.DECIMAL
    # elif precision <= 0:
    #     raise TerminalValueError(f"Precision must be positive, got {precision}")
    # else

    return sqlglot_type


def _to_temporal_type(
    dlt_type: str, precision: Optional[int], timezone: Optional[bool]
) -> DataType.Type:
    if dlt_type == "timestamp":
        sqlglot_type = DataType.Type.TIMESTAMP
    elif dlt_type == "time":
        sqlglot_type = DataType.Type.TIME
    else:
        raise TerminalValueError(f"Invalid temporal type: {dlt_type}")

    return sqlglot_type


class SQLGlotTypeMapper(DataTypeMapper):
    # TODO check how other `from_destination_type()` methods are called
    def from_destination_type(
        self, db_type: DATA_TYPE, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return from_sqlglot_type(db_type, precision, scale)

    def to_destination_type(self, column: TColumnSchema, table: PreparedTableSchema) -> str:
        dlt_type = column["data_type"]
        precision = column.get("precision")
        scale = column.get("scale")
        timezone = column.get("timezone")
        sqlglot_type = to_sqlglot_type(dlt_type, precision, scale, timezone)
        return sqlglot_type.value  # type: ignore[no-any-return]
