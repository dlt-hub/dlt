from typing import Optional

from dlt.common.utils import without_none
from dlt.common.exceptions import MissingDependencyException, TerminalValueError
from dlt.common.schema.typing import TColumnType, TDataType

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
    DataType.Type.BIGINT: None,
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
    DataType.Type.TIMESTAMP: None,  # default value
    DataType.Type.TIMESTAMPNTZ: False,
    DataType.Type.TIMESTAMPLTZ: True,
    DataType.Type.TIMESTAMPTZ: True,
    DataType.Type.TIMESTAMP_MS: False,
    DataType.Type.TIMESTAMP_NS: False,
    DataType.Type.TIMESTAMP_S: False,
    DataType.Type.TIME: None,  # default value
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
    """Convert a SQLGlot DataType to dlt column hints.
    
    precision (int): If specified, modifies hints for `bigint`, `decimal`, `timestamp`, and `time`
        - `bigint`:
        - `decimal`:
        - `timestamp`:
        - `time`:
    scale (int): ... Requires precision to also be specified.
    timezone (bool): If True, the timestamp value is associated with a timezone
    """
    if isinstance(sqlglot_type, str):
        sqlglot_type = sge.DataType.build(sqlglot_type)
    elif isinstance(sqlglot_type, sge.DataType.Type):
        sqlglot_type = sge.DataType.build(sqlglot_type)
    assert isinstance(sqlglot_type, sge.DataType)

    # only the `DataType.UNKNOWN` will produce `dlt_type=None`
    dlt_type = SQLGLOT_TO_DLT_TYPE_MAP.get(sqlglot_type.this, "text")

    # TODO retrieve explicit `precision`, `scale`, and `timezone` directly from DataType.expressions

    if dlt_type == "bigint":
        # NOTE this check is experimental; need validation from SQLGlot team for reliability
        if sqlglot_type.expressions and len(sqlglot_type.expressions) == 1:
            precision = int(sqlglot_type.expressions[0].this.this)  # get the type param
        else:
            precision = SQLGLOT_INT_PRECISION.get(sqlglot_type.this)

    elif dlt_type == "decimal":
        # NOTE this check is experimental; need validation from SQLGlot team for reliability
        if sqlglot_type.expressions and len(sqlglot_type.expressions) == 1:
            precision = int(sqlglot_type.expressions[0].this.this)
        elif sqlglot_type.expressions and len(sqlglot_type.expressions) == 2:
            precision = int(sqlglot_type.expressions[0].this.this)
            scale = int(sqlglot_type.expressions[1].this.this)
        else:
            precision_and_scale = SQLGLOT_DECIMAL_PRECISION_AND_SCALE.get(sqlglot_type.this)
            if precision_and_scale is not None:
                precision = precision if precision else precision_and_scale[0]
                scale = scale if scale else precision_and_scale[1]
        # TODO validate precision > scale

    elif dlt_type == "timestamp":
        precision = SQLGLOT_TEMPORAL_PRECISION.get(sqlglot_type.this)
        timezone = SQLGLOT_HAS_TIMEZONE.get(sqlglot_type.this)

    elif dlt_type == "time":
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
    hints = {}
    if nullable is not None:
        hints["nullable"] = nullable

    sqlglot_type = DLT_TO_SQLGLOT[dlt_type]
    if dlt_type == "bigint" and precision is not None:
        sqlglot_type = _to_integer_type(precision=precision)

    elif dlt_type == "decimal" and precision is not None:
        sqlglot_type = _to_decimal_type(precision=precision, scale=scale)

    elif dlt_type == "timestamp" and (precision is not None or timezone is not None):
        sqlglot_type = _to_timestamp_type(precision=precision, timezone=timezone)

    elif dlt_type == "time" and timezone is not None:
        sqlglot_type = _to_time_type(timezone=timezone)

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


def _to_timestamp_type(
    precision: Optional[int], timezone: Optional[bool]
) -> DataType.Type:
    if precision is None:
        if timezone is None:
            sqlglot_type = DataType.Type.TIMESTAMP
        elif timezone is True:
            sqlglot_type = DataType.Type.TIMESTAMPTZ
        elif timezone is False:
            sqlglot_type = DataType.Type.TIMESTAMPNTZ
    else:
        if timezone is None and precision == 1:
            sqlglot_type = sge.DataType.Type.TIMESTAMP_S
        elif timezone is None and precision == 3:
            sqlglot_type = sge.DataType.Type.TIMESTAMP_MS
        elif timezone is None and precision == 9:
            sqlglot_type = sge.DataType.Type.TIMESTAMP_NS
        else:
            # TODO add subtype
            sqlglot_type = DataType.Type.TIMESTAMP

    return sqlglot_type


def _to_time_type(timezone: Optional[bool]) -> DataType.Type:
    if timezone is None:
        sqlglot_type = DataType.Type.TIME
    elif timezone is True:
        sqlglot_type = DataType.Type.TIMETZ
    elif timezone is False:
        sqlglot_type = DataType.Type.TIME
    else:
        raise TerminalValueError()

    return sqlglot_type
