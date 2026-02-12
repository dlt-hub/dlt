from typing import Callable, Dict, List, Optional, Tuple, Union, Set, Any, Iterable, Literal
import sqlglot
import sqlglot.expressions as sge
from sqlglot.expressions import DataType, DATA_TYPE
from sqlglot.optimizer.scope import build_scope

from dlt.common.utils import without_none
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema.typing import TColumnType, TDataType, TColumnSchema, TTableSchemaColumns
from dlt.common.schema.exceptions import CannotCoerceNullException


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
    "fabric",
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


DLT_SUBQUERY_NAME = "_dlt_subquery"


class SqlModel:
    """A SqlModel is a named tuple that contains a query and a dialect.
    It is used to represent a SQL query and the dialect to use for parsing it.
    """

    __slots__ = ("_query", "_dialect")

    def __init__(self, query: str, dialect: Optional[str] = None) -> None:
        self._query = query
        self._dialect = dialect

    def to_sql(self) -> str:
        return self._query

    @property
    def query_dialect(self) -> str:
        return self._dialect

    @classmethod
    def from_query_string(cls, query: str, dialect: Optional[str] = None) -> "SqlModel":
        """Creates a SqlModel from a raw SQL query string using sqlglot.
        Ensures that the parsed query is an instance of sqlglot.exp.Select.

        Args:
            query: The raw SQL query string.
            dialect: The SQL dialect to use for parsing.

        Returns:
            An instance of SqlModel with the normalized query and dialect.

        Raises:
            ValueError: If the parsed query is not an instance of sqlglot.exp.Select.
        """
        parsed_query = sqlglot.parse_one(query, read=dialect)

        if not isinstance(parsed_query, sge.Select):
            raise ValueError("Only SELECT statements are allowed to create a `SqlModel`.")

        normalized_query = parsed_query.sql(dialect=dialect)
        return cls(query=normalized_query, dialect=dialect)


def uuid_expr_for_dialect(dialect: TSqlGlotDialect, load_id: str) -> sge.Expression:
    """Generates a UUID expression based on the specified dialect.

    Args:
        dialect: The SQL dialect for which the UUID expression needs to be generated.
        load_id: The load ID used for deterministic UUID generation (redshift).

    Returns:
        A SQL expression that generates a UUID for the specified dialect.
    """
    # NOTE: redshift and sqlite don't have an in-built uuid function
    if dialect == "redshift":
        row_num = sge.Window(
            this=sge.Anonymous(this="row_number"),
            partition_by=None,
            order=None,
        )
        concat_expr = sge.func(
            "CONCAT",
            sge.Literal.string(load_id),
            sge.Literal.string("-"),
            row_num,
        )
        return sge.func("MD5", concat_expr)
    elif dialect == "sqlite":
        return sge.func(
            "lower",
            sge.func("hex", sge.func("randomblob", sge.Literal.number(16))),
        )
    elif dialect == "clickhouse":
        return sge.func("generateUUIDv4")
    # NOTE: UUID in Athena creates a native UUID data type
    # which needs to be typecasted
    elif dialect == "athena":
        return sge.Cast(this=sge.func("UUID"), to=sge.DataType.build("VARCHAR"))
    else:
        return sge.func("UUID")


def get_select_column_names(
    selects: List[sge.Expression],
    dialect: Optional[TSqlGlotDialect] = None,
) -> List[str]:
    """Extract output column names from a SELECT clause's expression list.

    Handles Alias (returns alias), Column and Dot (returns output_name with
    fallback to name — needed for BigQuery quoted identifiers that parse as Dot).
    Raises ValueError for star expressions or unsupported expression types.

    Args:
        selects: The ``.selects`` list from a parsed SELECT statement.
        dialect: SQL dialect used only for error message formatting.

    Returns:
        Column output names in SELECT order.
    """
    names: List[str] = []
    for select in selects:
        if isinstance(select, sge.Star):
            raise ValueError(
                "Star expressions are not supported."
                " Please rewrite the query to explicitly specify columns."
            )
        if isinstance(select, sge.Alias):
            names.append(select.alias)
        elif isinstance(select, sge.Column) or isinstance(select, sge.Dot):
            if isinstance(select.this, sge.Star):
                raise ValueError(
                    "Star expressions are not supported."
                    " Please rewrite the query to explicitly specify columns."
                )
            name = select.output_name or select.name
            names.append(name)
        else:
            sql_text = select.sql(dialect) if dialect else str(select)
            raise ValueError(
                "\n\nUnsupported SELECT expression in the model query:\n\n"
                f"  {sql_text}\n\nOnly simple column selections like `column`"
                " or `column AS alias` are currently supported.\n"
            )
    return names


def filter_select_column_names(
    selects: List[sge.Expression],
    discard_columns: Set[str],
    normalize_fn: Callable[[str], str],
    dialect: Optional[TSqlGlotDialect] = None,
) -> List[str]:
    """Return SELECT column names excluding those whose normalized form is in discard_columns.

    Preserves original SELECT order.

    Args:
        selects: The ``.selects`` list from a parsed SELECT statement.
        discard_columns: Set of normalized column names to exclude.
        normalize_fn: Callable that normalizes a column name (e.g. casefold).
        dialect: SQL dialect used only for error message formatting.

    Returns:
        Remaining column names in their original SELECT order.
    """
    all_names = get_select_column_names(selects, dialect)
    return [name for name in all_names if normalize_fn(name) not in discard_columns]


def validate_no_star_select(parsed_select: sge.Select, dialect: TSqlGlotDialect) -> None:
    """Raises ValueError if the SELECT statement contains a star expression.

    Args:
        parsed_select: The parsed SELECT statement.
        dialect: The SQL dialect (used for error message formatting).
    """
    if any(
        isinstance(expr, sge.Star)
        or (isinstance(expr, sge.Column) and isinstance(expr.this, sge.Star))
        for expr in parsed_select.selects
    ):
        raise ValueError(
            "\n\nA `SELECT *` was detected in the model query:\n\n"
            f"{parsed_select.sql(dialect)}\n\n"
            "Model queries using a star (`*`) expression cannot be normalized. "
            "Please rewrite the query to explicitly specify the columns to be selected.\n"
        )


def build_outer_select_statement(
    select_dialect: TSqlGlotDialect,
    parsed_select: sge.Select,
    columns: TTableSchemaColumns,
    normalize_casefold_fn: Callable[[str], str],
) -> Tuple[sge.Select, bool]:
    """Wraps the parsed SELECT in a subquery and builds an outer SELECT statement.

    Args:
        select_dialect: The SQL dialect to use for parsing and formatting.
        parsed_select: The parsed SELECT statement.
        columns: The schema columns to match.
        normalize_casefold_fn: A callable that normalizes and casefolds an identifier.

    Returns:
        Tuple of the outer SELECT statement and a flag indicating if reordering is needed.
    """
    subquery = parsed_select.subquery(alias=DLT_SUBQUERY_NAME)

    column_names = get_select_column_names(parsed_select.selects, select_dialect)

    selected_columns: List[str] = []
    outer_selects: List[sge.Expression] = []
    for name in column_names:
        norm_casefolded = normalize_casefold_fn(name)
        selected_columns.append(norm_casefolded)

        column_ref = sge.Dot(
            this=sqlglot.to_identifier(DLT_SUBQUERY_NAME),
            expression=sqlglot.to_identifier(name, quoted=True),
        )

        outer_selects.append(column_ref.as_(norm_casefolded, quoted=True))

    needs_reordering = selected_columns != list(columns.keys())

    outer_select = sqlglot.select(*outer_selects).from_(subquery)

    return outer_select, needs_reordering


def create_outer_select_identifier_normalizer(
    naming_convention: Any,  # NamingConvention instance
    casefold_identifier: Callable[[str], str],
) -> Callable[[str], str]:
    """Create a normalization function for outer SELECT identifiers."""

    def _normalizer(identifier: str) -> str:
        # use normalize_path() to preserve __ separators in already-normalized names
        normalized = naming_convention.normalize_path(identifier)
        return casefold_identifier(normalized)

    return _normalizer


def reorder_or_adjust_outer_select(
    outer_parsed_select: sge.Select,
    columns: TTableSchemaColumns,
    normalize_casefold_fn: Callable[[str], str],
    schema_name: str,
    table_name: str,
) -> None:
    """Reorders or adjusts the SELECT statement to match the schema.

    Adds missing columns as NULL and removes extra columns not in the schema.

    Args:
        outer_parsed_select: The parsed outer SELECT statement.
        columns: The schema columns to match.
        normalize_casefold_fn: A callable that normalizes and casefolds an identifier.
        schema_name: The schema name (used for error messages).
        table_name: The table name (used for error messages).
    """
    alias_map = {expr.alias.lower(): expr for expr in outer_parsed_select.selects}

    new_selects: List[sge.Expression] = []
    for col in columns:
        lower_col = col.lower()
        expr = alias_map.get(lower_col)
        if expr:
            new_selects.append(expr)
        else:
            if columns[col]["nullable"]:
                new_selects.append(
                    sge.Alias(
                        this=sge.Null(),
                        alias=sqlglot.to_identifier(normalize_casefold_fn(col), quoted=True),
                    )
                )
            else:
                exc = CannotCoerceNullException(schema_name, table_name, col)
                exc.args = (
                    exc.args[0]
                    + " — column is missing from the SELECT clause but is non-nullable and must"
                    " be explicitly selected",
                )
                raise exc

    outer_parsed_select.set("expressions", new_selects)


def normalize_query_identifiers(
    query: sge.Query,
    naming_convention: Any,
) -> sge.Query:
    """Normalize all logical identifiers in a query to match the naming convention.

    Normalizes table names, column names, and aliases. Does not modify database
    or catalog names. Call before bind_query to ensure identifiers match the
    dlt schema naming.

    Args:
        query: Query with logical (possibly unnormalized) identifiers
        naming_convention: Naming convention to apply

    Returns:
        Query with all identifiers normalized
    """
    query = query.copy()

    for node in query.walk():
        # normalize table names
        if isinstance(node, sge.Table):
            normalized = naming_convention.normalize_tables_path(node.name)
            if node.name != normalized:
                node.this.set("this", normalized)

        # normalize column and other identifiers (but not table/db/catalog)
        elif isinstance(node, sge.Identifier):
            # skip identifiers that are part of Table nodes (table/db/catalog names)
            if isinstance(node.parent, sge.Table):
                continue

            normalized = naming_convention.normalize_path(node.this)
            if node.this != normalized:
                node.set("this", normalized)

        # normalize aliases
        elif isinstance(node, sge.Alias) and node.alias:
            normalized = naming_convention.normalize_path(node.alias)
            if node.alias != normalized:
                node.set("alias", sqlglot.to_identifier(normalized, quoted=False))

    return query


def bind_query(
    qualified_query: sge.Query,
    sqlglot_schema: Any,  # SQLGlotSchema
    *,
    expand_table_name: Callable[[str], List[str]],
    casefold_identifier: Callable[[str], str],
) -> sge.Query:
    """Binds a logical query (compliant with dlt schema) to physical tables in the destination dataset.

    This function performs name resolution/binding - a standard query processing step that maps
    logical identifiers to actual database objects. It takes a query using dlt's logical naming
    convention and adapts it for execution on the destination database.
    Note that outer select list after binding preserve select list in `qualified_query`.

    Binding steps performed:
    1. **Table name expansion**: Resolves table references to fully qualified physical paths
       (e.g., for ClickHouse: `dataset.table` → `dataset___table`)
    2. **Identifier case-folding**: Applies destination-specific case transformation
       (`str.upper` for Snowflake, `str.lower` for most databases, `str` for case-sensitive)
    3. **Identifier quoting**: Quotes all identifiers for safe execution and to preserve
       exact casing (e.g., `column_name` → `"column_name"`)
    4. **Alias preservation**: For case-folding destinations, adds aliases to SELECT columns
       to maintain compatibility with dlt schema naming (e.g., `SELECT "VALUE" AS "value"`)

    Args:
        qualified_query: SQLGlot query expression with qualified table/column references
        sqlglot_schema: Schema mapping for name validation and column resolution
        expand_table_name: Function that expands table name to fully qualified path [catalog, schema, table]
        casefold_identifier: Case transformation function (`str`, `str.upper`, or `str.lower`)

    Returns:
        Bound query expression ready for execution on the destination database
    """
    qualified_query = qualified_query.copy()
    is_casefolding = casefold_identifier is not str

    # preserve "column" names in original selects which are done in dlt schema namespace
    orig_selects: Dict[int, str] = None
    if is_casefolding:
        orig_selects = {}
        for i, proj in enumerate(qualified_query.selects):
            orig_selects[i] = proj.name or proj.args["alias"].name

    # case fold all identifiers and quote
    for node in qualified_query.walk():
        if isinstance(node, sge.Table):
            # expand named of known tables. this is currently clickhouse things where
            # we use dataset.table in queries but render those as dataset___table
            if sqlglot_schema.column_names(node):
                expanded_path = expand_table_name(node.name)
                # set the table name
                if node.name != expanded_path[-1]:
                    node.this.set("this", expanded_path[-1])
                # set the dataset/schema name
                if node.db != expanded_path[-2]:
                    node.set("db", sqlglot.to_identifier(expanded_path[-2], quoted=False))
                # set the catalog name
                if len(expanded_path) == 3:
                    if node.db != expanded_path[0]:
                        node.set("catalog", sqlglot.to_identifier(expanded_path[0], quoted=False))
        # quote and case-fold identifiers, TODO: maybe we could be more intelligent, but then we need to unquote ibis
        if isinstance(node, sge.Identifier):
            if is_casefolding:
                node.set("this", casefold_identifier(node.this))
            node.set("quoted", True)

    # add aliases to output selects to stay compatible with dlt schema after the query
    if orig_selects:
        for i, orig in orig_selects.items():
            case_folded_orig = casefold_identifier(orig)
            if case_folded_orig != orig:
                # somehow we need to alias just top select in UNION (tested on Snowflake)
                sel_expr = qualified_query.selects[i]
                qualified_query.selects[i] = sge.alias_(sel_expr, orig, quoted=True)

    return qualified_query
