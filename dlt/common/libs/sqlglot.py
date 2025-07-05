from typing import Optional, Union, Set, Any, Iterable, Literal

from dlt.common.utils import without_none
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema.typing import TColumnType, TDataType, TColumnSchema

import sqlglot.expressions as sge
from sqlglot.expressions import DataType, DATA_TYPE
from sqlglot.optimizer.scope import build_scope


TSqlGlotDialect = Literal[
    "athena",
    "bigquery",
    "clickhouse",
    "databricks",
    "doris",
    "drill",
    "druid",
    "duckdb",
    "dune",
    "hive",
    "materialize",
    "mysql",
    "oracle",
    "postgres",
    "presto",
    "prql",
    "redshift",
    "risingwave",
    "snowflake",
    "spark",
    "spark2",
    "sqlite",
    "starrocks",
    "tableau",
    "teradata",
    "trino",
    "tsql",
]

SQLGLOT_TO_DLT_TYPE_MAP: dict[DataType.Type, TDataType] = {
    # NESTED_TYPES
    DataType.Type.OBJECT: "json",
    DataType.Type.STRUCT: "json",
    DataType.Type.NESTED: "json",
    DataType.Type.ARRAY: "json",
    DataType.Type.JSON: "json",
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
    DataType.Type.MONEY: "decimal",
    DataType.Type.SMALLMONEY: "decimal",
    DataType.Type.UDECIMAL: "decimal",
    # TEMPORAL
    DataType.Type.DATE: "date",
    DataType.Type.DATE32: "date",
    DataType.Type.DATETIME: "date",
    DataType.Type.DATETIME64: "date",
    DataType.Type.TIMESTAMP: "timestamp",
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

# these types were introduced after our sqlglot minimum version of 23.6.3
try:
    SQLGLOT_TO_DLT_TYPE_MAP[DataType.Type.UDOUBLE] = "decimal"
except AttributeError:
    pass

try:
    SQLGLOT_TO_DLT_TYPE_MAP[DataType.Type.DATETIME2] = "date"
except AttributeError:
    pass

try:
    SQLGLOT_TO_DLT_TYPE_MAP[DataType.Type.SMALLDATETIME] = "date"
except AttributeError:
    pass

try:
    SQLGLOT_TO_DLT_TYPE_MAP[DataType.Type.UNION] = "json"
except AttributeError:
    pass

try:
    SQLGLOT_TO_DLT_TYPE_MAP[DataType.Type.LIST] = "json"
except AttributeError:
    pass

try:
    SQLGLOT_TO_DLT_TYPE_MAP[DataType.Type.VECTOR] = "json"
except AttributeError:
    pass

try:
    SQLGLOT_TO_DLT_TYPE_MAP[DataType.Type.DECIMAL32] = "decimal"
    SQLGLOT_TO_DLT_TYPE_MAP[DataType.Type.DECIMAL64] = "decimal"
    SQLGLOT_TO_DLT_TYPE_MAP[DataType.Type.DECIMAL128] = "decimal"
    SQLGLOT_TO_DLT_TYPE_MAP[DataType.Type.DECIMAL256] = "decimal"
except AttributeError:
    pass


# NOTE in Snowflake, TIMESTAMPNTZ == DATETIME; is this true for dlt?
SQLGLOT_HAS_TIMEZONE = {
    DataType.Type.TIMESTAMP: None,  # default value; False
    DataType.Type.TIMESTAMPLTZ: True,
    DataType.Type.TIMESTAMPTZ: True,
    DataType.Type.TIMESTAMP_MS: False,
    DataType.Type.TIMESTAMP_NS: False,
    DataType.Type.TIMESTAMP_S: False,
    DataType.Type.TIME: False,  # default value; False
    DataType.Type.TIMETZ: True,
}

has_timestampntz = hasattr(DataType.Type, "TIMESTAMPNTZ")
if has_timestampntz:
    SQLGLOT_HAS_TIMEZONE[DataType.Type.TIMESTAMPNTZ] = False
    SQLGLOT_TO_DLT_TYPE_MAP[DataType.Type.TIMESTAMPNTZ] = "timestamp"

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
}


# TODO should we raise errors on type conversion or silently convert to correct values?
def from_sqlglot_type(sqlglot_type: DATA_TYPE) -> TColumnType:
    """Convert a SQLGlot DataType to dlt column hints.

    reference: https://dlthub.com/docs/general-usage/schema/#tables-and-columns
    """
    if isinstance(sqlglot_type, str):
        sqlglot_type = sge.DataType.build(sqlglot_type)
    elif isinstance(sqlglot_type, sge.DataType.Type):
        sqlglot_type = sge.DataType.build(sqlglot_type)
    assert isinstance(sqlglot_type, sge.DataType)

    # only the `DataType.UNKNOWN` will produce `dlt_type=None`
    dlt_type = SQLGLOT_TO_DLT_TYPE_MAP.get(sqlglot_type.this, "text")

    if dlt_type == "bigint":
        hints = _from_integer_type(sqlglot_type)
    elif dlt_type == "decimal":
        hints = _from_decimal_type(sqlglot_type)
    elif dlt_type == "timestamp":
        hints = _from_timezone_type(sqlglot_type)
    elif dlt_type == "time":
        hints = _from_time_type(sqlglot_type)
    elif dlt_type in ("text", "binary"):
        hints = _from_string_type(sqlglot_type)
    else:
        hints = {}

    # `nullable` is not a required arg on exp.DataType
    nullable = sqlglot_type.args.get("nullable")

    return without_none({"data_type": dlt_type, "nullable": nullable, **hints})  # type: ignore[return-value]


def _from_integer_type(sqlglot_type: sge.DataType) -> TColumnSchema:
    if sqlglot_type.expressions:  # from parameterized type
        assert len(sqlglot_type.expressions) == 1
        assert isinstance(sqlglot_type.expressions[0], sge.DataTypeParam)
        return {"precision": sqlglot_type.expressions[0].this.to_py()}

    return {}


def _from_decimal_type(sqlglot_type: sge.DataType) -> TColumnSchema:
    hints = {}
    if sqlglot_type.expressions:
        assert all(isinstance(e, sge.DataTypeParam) for e in sqlglot_type.expressions)
        assert len(sqlglot_type.expressions) in (1, 2)
        if len(sqlglot_type.expressions) == 1:
            precision = sqlglot_type.expressions[0].this.to_py()
            hints = {"precision": precision}
        elif len(sqlglot_type.expressions) == 2:
            precision = sqlglot_type.expressions[0].this.to_py()
            scale = sqlglot_type.expressions[1].this.to_py()
            hints = {"precision": precision, "scale": scale}
        else:
            raise RuntimeError(
                "Expected 1 or 2 `DataTypeParam` attached to expression. "
                f"Found {len(sqlglot_type.expressions)}: {sqlglot_type.expressions}"
            )

    return hints  # type: ignore[return-value]


def _from_timezone_type(sqlglot_type: sge.DataType) -> TColumnSchema:
    timezone = SQLGLOT_HAS_TIMEZONE.get(sqlglot_type.this)
    precision = None

    if sqlglot_type.expressions:  # from parameterized type
        assert len(sqlglot_type.expressions) == 1
        assert isinstance(sqlglot_type.expressions[0], sge.DataTypeParam)
        precision = sqlglot_type.expressions[0].this.to_py()

    return {"precision": precision, "timezone": timezone}


def _from_time_type(sqlglot_type: sge.DataType) -> TColumnSchema:
    timezone = SQLGLOT_HAS_TIMEZONE.get(sqlglot_type.this)

    if sqlglot_type.expressions:  # from parameterized type
        assert len(sqlglot_type.expressions) == 1
        assert isinstance(sqlglot_type.expressions[0], sge.DataTypeParam)
        precision = sqlglot_type.expressions[0].this.to_py()
        hints = {"precision": precision, "timezone": timezone}
    else:  # from named type
        hints = {"timezone": timezone}

    return hints  # type: ignore[return-value]


def _from_string_type(sqlglot_type: sge.DataType) -> TColumnSchema:
    if sqlglot_type.expressions:  # from parameterized type
        assert len(sqlglot_type.expressions) == 1
        assert isinstance(sqlglot_type.expressions[0], sge.DataTypeParam)
        precision = sqlglot_type.expressions[0].this.to_py()
        hints: TColumnSchema = {"precision": precision}
    else:
        hints = {}
    return hints


def to_sqlglot_type(
    dlt_type: TDataType,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
    timezone: Optional[bool] = None,
    nullable: Optional[bool] = None,
    use_named_types: bool = False,
) -> DataType:
    """Convert the dlt `data_type` and column hints to a SQLGlot DataType expression.

    `if use_named_type is True:` use "named" types with fallback on "parameterized" types.
    `else:` use "parameterized" types everywhere.

    Named types:
    - have some set attributes, e.g., `DataType.Type.DECIMAL64` has `precision=16` and `scale=4`.
    - can be referenced directly in SQL (table definition, CAST(), which could make SQLGlot transpiling more reliable.
    - may not exist in all dialects and will be casted automatically during transpiling
    - can have limited expressivity, e.g., named types for timestamps only included precision 0, 3, 9

    Parameterized types:
    - instead of DECIMAL64, a generic `DataType.Type.DECIMAL` is used with `DataTypeParam` expressions attached
      to represent `precision` and `scale`. SQLGlot is responsible for properly compiling and transpiling them.
    - parameters might be handled differently across dialects and would require greater testing on the dlt side.

    reference: https://dlthub.com/docs/general-usage/schema/#tables-and-columns
    """
    if use_named_types is True:
        try:
            sqlglot_type = _build_named_sqlglot_type(
                dlt_type=dlt_type,
                nullable=nullable,
                precision=precision,
                scale=scale,
                timezone=timezone,
            )
        except TerminalValueError:
            sqlglot_type = _build_parameterized_sqlglot_type(
                dlt_type=dlt_type,
                nullable=nullable,
                precision=precision,
                scale=scale,
                timezone=timezone,
            )
    else:
        sqlglot_type = _build_parameterized_sqlglot_type(
            dlt_type=dlt_type,
            nullable=nullable,
            precision=precision,
            scale=scale,
            timezone=timezone,
        )

    return sqlglot_type


# NOTE Let's ignore named types for now
def _build_named_sqlglot_type(
    dlt_type: TDataType,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
    timezone: Optional[bool] = None,
    nullable: Optional[bool] = None,
) -> DataType:
    hints = {}
    if nullable is not None:
        hints["nullable"] = nullable

    if dlt_type == "bigint" and precision is not None:
        sqlglot_type = _to_named_integer_type(precision=precision)
    elif dlt_type == "decimal" and precision is not None:
        sqlglot_type = _to_named_decimal_type(precision=precision, scale=scale)
    elif dlt_type == "timestamp" and (precision is not None or timezone is not None):
        sqlglot_type = _to_named_timestamp_type(precision=precision, timezone=timezone)
    elif dlt_type == "time" and timezone is not None:
        sqlglot_type = _to_named_time_type(timezone=timezone)
    else:
        raise TerminalValueError()

    return DataType.build(dtype=sqlglot_type, **hints)  # type: ignore[arg-type]


def _build_parameterized_sqlglot_type(
    dlt_type: TDataType,
    nullable: Optional[bool] = None,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
    timezone: Optional[bool] = None,
) -> DataType:
    hints: TColumnType = {}
    # `nullable` should always be added to DataType
    if nullable is not None:
        hints["nullable"] = nullable

    if dlt_type == "bigint" and precision is not None:
        # NOTE INT precision is not supported by many backend.
        # For instance, INT(5) in MySQL should be DECIMAL(5,0) in DuckDB
        # Currently, SQLGLot can't transpile from INT(5) to DECIMAL(5,0).
        # Using non-parameterized type would be more reliable in these cases.
        sqlglot_type = sge.DataType.build(f"INT({precision})", **hints)

    elif dlt_type == "decimal" and precision is not None:
        # this code is inspired from sqlglot.dialects.dialect.build_default_decimal_type
        params = f"({precision}{f', {scale}' if scale is not None else ''})"
        sqlglot_type = sge.DataType.build(f"DECIMAL{params}", **hints)

    elif dlt_type == "timestamp" and (precision is not None or timezone is not None):
        base_sqlglot_type = _to_named_timestamp_type(precision=None, timezone=timezone)
        params = f"({precision})" if precision is not None else ""
        sqlglot_type = sge.DataType.build(f"{base_sqlglot_type.value}{params}", **hints)

    elif dlt_type == "time" and (precision is not None or timezone is not None):
        base_sqlglot_type = _to_named_time_type(timezone=timezone)
        params = f"({precision})" if precision is not None else ""
        sqlglot_type = sge.DataType.build(f"{base_sqlglot_type.value}{params}", **hints)

    elif dlt_type == "text" and precision is not None:
        sqlglot_type = sge.DataType.build(f"TEXT({precision})", **hints)

    elif dlt_type == "binary" and precision is not None:
        sqlglot_type = sge.DataType.build(f"VARBINARY({precision})", **hints)
    elif dlt_type == "wei":
        sqlglot_type = sge.DataType.build("DECIMAL(38, 0)", **hints)
    else:
        sqlglot_type = sge.DataType.build(DLT_TO_SQLGLOT[dlt_type], **hints)

    return sqlglot_type


def _to_named_integer_type(precision: Optional[int]) -> DataType.Type:
    """Return the SQLGlot INTEGER DataType with precision `>= precision`."""
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
        raise TerminalValueError(
            f"Precision must be less than maximum precision of 78, got {precision}"
        )

    return sqlglot_type


# TODO
def _to_named_decimal_type(precision: Optional[int], scale: Optional[int]) -> DataType.Type:
    sqlglot_type = DataType.Type.DECIMAL
    # if precision is None:
    #     sqlglot_type = DataType.Type.DECIMAL
    # elif precision <= 0:
    #     raise TerminalValueError(f"Precision must be positive, got {precision}")
    # else

    return sqlglot_type


def _to_named_timestamp_type(precision: Optional[int], timezone: Optional[bool]) -> DataType.Type:
    if precision is None:
        if timezone is True:
            sqlglot_type = DataType.Type.TIMESTAMPTZ
        elif timezone is False and has_timestampntz:
            sqlglot_type = DataType.Type.TIMESTAMPNTZ
        elif timezone is None:
            sqlglot_type = DataType.Type.TIMESTAMP
        else:
            raise TerminalValueError(
                f"Received `timezone` value not in (True, False, None), got {timezone}"
            )

    else:
        if timezone is None and precision == 0:
            sqlglot_type = sge.DataType.Type.TIMESTAMP_S
        elif timezone is None and precision <= 3:
            sqlglot_type = sge.DataType.Type.TIMESTAMP_MS
        elif timezone is None and precision <= 9:
            sqlglot_type = sge.DataType.Type.TIMESTAMP_NS
        else:
            raise TerminalValueError(
                f"Precision must be less than maximum precision of 9, got {precision}"
            )

    return sqlglot_type


def _to_named_time_type(timezone: Optional[bool]) -> DataType.Type:
    if timezone is None:
        sqlglot_type = DataType.Type.TIME
    elif timezone is True:
        sqlglot_type = DataType.Type.TIMETZ
    elif timezone is False:
        sqlglot_type = DataType.Type.TIME
    else:
        raise TerminalValueError(
            f"Received `timezone` value not in (True, False, None), got {timezone}"
        )

    return sqlglot_type


# TODO implement an SQLGlot annotator for setting dlt hints instead of relying on types.
# ref: https://github.com/tobymao/sqlglot/blob/17f7eaff564790b1fe7faa414143accf362f550e/sqlglot/optimizer/annotate_types.py#L135
def set_metadata(sqlglot_type: sge.DataType, metadata: TColumnSchema) -> sge.DataType:
    """Set a metadata dictionary on the SQGLot DataType object.

    By attaching dlt hints to a DataType object, they will be propagated
    until the DataType is modified.
    """
    sqlglot_type._meta = _filter_dlt_hints(metadata)  # type: ignore[assignment]
    return sqlglot_type


def get_metadata(sqlglot_type: sge.DataType) -> TColumnSchema:
    """Get a metadata dictionary from the SQLGlot DataType object."""
    metadata = sqlglot_type.meta
    return _filter_dlt_hints(metadata)  # type: ignore[arg-type]


def _filter_dlt_hints(hints: TColumnSchema) -> TColumnSchema:
    """Filter the metadata dictionary to only include dlt hints."""
    DATA_TYPE_HINTS = ("data_type", "nullable", "precision", "scale", "timezone")
    # allow original name to be propagated, so we can track it if aliases were used
    ALLOWED_HINTS = ("name", "hard_delete", "incremental")
    # only propagate specially designated hints
    # TODO review all x- hints we use and decide which to propagate
    CUSTOM_HINT_PREFIX = "x-annotation"

    final_hints = {}
    for k, v in hints.items():
        # those are directly expressed via the DataType and Column
        if k in DATA_TYPE_HINTS:
            continue

        if k in ALLOWED_HINTS:
            final_hints[k] = v
        elif k.startswith(CUSTOM_HINT_PREFIX):
            final_hints[k] = v

    return final_hints  # type: ignore[return-value]


def query_is_complex(
    parsed_select: Union[sge.Select, sge.Union],
    columns: Set[str],
) -> bool:
    """
    Return **True** unless the query is provably “simple”.
    A *simple* query
    1. references **exactly one** physical table,
    2. contains no complex constructs (CTEs, sub-queries, derived tables,
       unions, window functions, GROUP BY, DISTINCT, etc.),
    3. projects either
         • a plain/qualified star with only constant literals after it, or
         • the full, explicit list of *all* columns with only constant
           literals after it.
    Anything we cannot *prove* to be simple is conservatively flagged
    as complex.
    """
    root_scope = build_scope(parsed_select)
    tables: list[sge.Table] = []
    non_literal_cols: dict[str, int] = {}
    star_present = False
    for node in root_scope.walk():
        if isinstance(node, sge.Table):
            tables.append(node)
        if isinstance(node, sge.Window):
            return True
        if isinstance(node, sge.SetOperation):
            return True
        if isinstance(node, sge.With):
            return True
        if isinstance(node, sge.Distinct):
            return True
        if isinstance(node, sge.Group):
            return True
        if isinstance(node, sge.DPipe):
            return True
        # Detect a star in the outermost projection list.
        # A star can be either “*” or a qualified “t.*”.
        # In the AST:
        #   • bare  “*”    Star -> SELECT
        #   • qual. “t.*”  Star -> Column -> SELECT
        if isinstance(node, sge.Star) and (
            node.parent == parsed_select or (node.parent and node.parent.parent == parsed_select)
        ):
            star_present = True
        # Detect plain (aliased) column references that belong to the outermost projection list.
        if isinstance(node, sge.Column) and not isinstance(node.this, sge.Star):
            # it's an (aliased) column in our outer select
            if node.parent == parsed_select or (
                isinstance(node.parent, sge.Alias) and node.parent_select == parsed_select
            ):
                non_literal_cols[node.name] = non_literal_cols.get(node.name, 0) + 1
        # An aliased expression that is *not* just a column or a
        # literal disqualifies the query from being “simple
        if isinstance(node, sge.Alias):
            if not (isinstance(node.this, sge.Column) or isinstance(node.this, sge.Literal)):
                return True
    if len(tables) > 1:
        return True
    if star_present and not non_literal_cols:
        return False
    non_literal_cols_names = set(non_literal_cols.keys())
    # if any column is selected twice, it is complex
    if any(count > 1 for count in non_literal_cols.values()):
        return True
    # if selected columns are exactly the same as the table columns, it is simple
    if non_literal_cols_names == columns:
        return False
    # if newly added columns are all exclusively dlt columns, it is simple
    if (
        all(col_name.startswith("_dlt_") for col_name in non_literal_cols_names - columns)
        and non_literal_cols_names - columns != set()
    ):
        return False
    # if missing columns are all exclusively dlt columns, it is simple
    if (
        all(col_name.startswith("_dlt_") for col_name in columns - non_literal_cols_names)
        and columns - non_literal_cols_names != set()
    ):
        return False
    return True


def build_typed_literal(
    value: Any, sqlglot_type: sge.DataType = None
) -> Union[sge.Expression, sge.Tuple]:
    """Create a literal and CAST it to the requested sqlglot DataType."""

    def _literal(v: Any) -> sge.Expression:
        lit: sge.Expression
        if v is None:
            lit = sge.Null()
        elif isinstance(v, str):
            lit = sge.Literal.string(v)
        elif isinstance(v, (int, float)):
            lit = sge.Literal.number(v)
        elif isinstance(v, (bytes, bytearray)):
            lit = sge.Literal.string(v.hex())
        else:
            lit = sge.Literal.string(str(v))
        return sge.Cast(this=lit, to=sqlglot_type.copy()) if sqlglot_type is not None else lit

    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        return sge.Tuple(expressions=[_literal(v) for v in value])
    else:
        return _literal(value)
