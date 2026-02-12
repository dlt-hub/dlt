from typing import List, Optional, Set, cast

import pytest
import sqlglot
import sqlglot.expressions as sge

from dlt.common.libs.sqlglot import (
    DLT_SUBQUERY_NAME,
    TSqlGlotDialect,
    filter_select_column_names,
    from_sqlglot_type,
    get_select_column_names,
    to_sqlglot_type,
    uuid_expr_for_dialect,
    validate_no_star_select,
    build_outer_select_statement,
    reorder_or_adjust_outer_select,
)
from dlt.common.schema.typing import TDataType, TColumnType, TTableSchemaColumns
from dlt.common.schema.exceptions import CannotCoerceNullException


@pytest.mark.parametrize(
    "dlt_type, expected_sqlglot_type",
    [
        ("json", sge.DataType.Type.JSON),
        ("text", sge.DataType.Type.TEXT),
        ("double", sge.DataType.Type.DOUBLE),
        ("bool", sge.DataType.Type.BOOLEAN),
        ("date", sge.DataType.Type.DATE),
        ("timestamp", sge.DataType.Type.TIMESTAMP),
        ("bigint", sge.DataType.Type.BIGINT),
        ("binary", sge.DataType.Type.VARBINARY),
        ("time", sge.DataType.Type.TIME),
        ("decimal", sge.DataType.Type.DECIMAL),
    ],
)
@pytest.mark.parametrize("use_named_type", [True, False])
def test_to_sqlglot(
    dlt_type: TDataType,
    expected_sqlglot_type: sge.DataType.Type,
    use_named_type: bool,
) -> None:
    """Test basic dlt type to SQLGlot type enum"""
    sqlglot_type = to_sqlglot_type(dlt_type, use_named_types=use_named_type)
    assert sqlglot_type == sge.DataType.build(expected_sqlglot_type)


def _from_sqlglot_cases() -> list[tuple[sge.DataType.Type, Optional[TDataType]]]:
    """Define explicit SQLGlot enum to dlt type mapping.Other types default to `text`"""
    mapping: dict[sge.DataType.Type, Optional[TDataType]] = {
        sge.DataType.Type.OBJECT: "json",
        sge.DataType.Type.STRUCT: "json",
        sge.DataType.Type.NESTED: "json",
        sge.DataType.Type.ARRAY: "json",
        sge.DataType.Type.LIST: "json",
        sge.DataType.Type.JSON: "json",
        # TEXT
        sge.DataType.Type.CHAR: "text",
        sge.DataType.Type.NCHAR: "text",
        sge.DataType.Type.NVARCHAR: "text",
        sge.DataType.Type.TEXT: "text",
        sge.DataType.Type.VARCHAR: "text",
        sge.DataType.Type.NAME: "text",
        # SIGNED_INTEGER
        sge.DataType.Type.BIGINT: "bigint",
        sge.DataType.Type.INT: "bigint",
        sge.DataType.Type.INT128: "bigint",
        sge.DataType.Type.INT256: "bigint",
        sge.DataType.Type.MEDIUMINT: "bigint",
        sge.DataType.Type.SMALLINT: "bigint",
        sge.DataType.Type.TINYINT: "bigint",
        # UNSIGNED_INTEGER
        sge.DataType.Type.UBIGINT: "bigint",
        sge.DataType.Type.UINT: "bigint",
        sge.DataType.Type.UINT128: "bigint",
        sge.DataType.Type.UINT256: "bigint",
        sge.DataType.Type.UMEDIUMINT: "bigint",
        sge.DataType.Type.USMALLINT: "bigint",
        sge.DataType.Type.UTINYINT: "bigint",
        # other INTEGER
        sge.DataType.Type.BIT: "bigint",
        # FLOAT
        sge.DataType.Type.DOUBLE: "double",
        sge.DataType.Type.FLOAT: "double",
        # DECIMAL
        sge.DataType.Type.BIGDECIMAL: "decimal",
        sge.DataType.Type.DECIMAL: "decimal",
        sge.DataType.Type.MONEY: "decimal",
        sge.DataType.Type.SMALLMONEY: "decimal",
        sge.DataType.Type.UDECIMAL: "decimal",
        # TEMPORAL
        sge.DataType.Type.DATE: "date",
        sge.DataType.Type.DATE32: "date",
        sge.DataType.Type.DATETIME: "date",
        sge.DataType.Type.DATETIME64: "date",
        sge.DataType.Type.TIMESTAMP: "timestamp",
        sge.DataType.Type.TIMESTAMPNTZ: "timestamp",
        sge.DataType.Type.TIMESTAMPLTZ: "timestamp",
        sge.DataType.Type.TIMESTAMPTZ: "timestamp",
        sge.DataType.Type.TIMESTAMP_MS: "timestamp",
        sge.DataType.Type.TIMESTAMP_NS: "timestamp",
        sge.DataType.Type.TIMESTAMP_S: "timestamp",
        sge.DataType.Type.TIME: "time",
        sge.DataType.Type.TIMETZ: "time",
        # binary
        sge.DataType.Type.VARBINARY: "binary",
        # BOOLEAN
        sge.DataType.Type.BOOLEAN: "bool",
        # UNKNOWN
        sge.DataType.Type.UNKNOWN: None,
    }

    try:
        mapping[sge.DataType.Type.UDOUBLE] = "decimal"
    except AttributeError:
        pass

    try:
        mapping[sge.DataType.Type.DATETIME2] = "date"
    except AttributeError:
        pass

    try:
        mapping[sge.DataType.Type.SMALLDATETIME] = "date"
    except AttributeError:
        pass

    try:
        mapping[sge.DataType.Type.UNION] = "json"
    except AttributeError:
        pass

    try:
        mapping[sge.DataType.Type.LIST] = "json"
    except AttributeError:
        pass

    try:
        mapping[sge.DataType.Type.VECTOR] = "json"
    except AttributeError:
        pass

    try:
        mapping[sge.DataType.Type.DECIMAL32] = "decimal"
        mapping[sge.DataType.Type.DECIMAL64] = "decimal"
        mapping[sge.DataType.Type.DECIMAL128] = "decimal"
        mapping[sge.DataType.Type.DECIMAL256] = "decimal"
    except AttributeError:
        pass

    # "text" is the default dlt data_type
    return [(sqlglot_type, mapping.get(sqlglot_type, "text")) for sqlglot_type in sge.DataType.Type]


@pytest.mark.parametrize("sqlglot_type, expected_dlt_type", _from_sqlglot_cases())
def test_from_sqlglot(sqlglot_type: sge.DataType.Type, expected_dlt_type: TDataType) -> None:
    """Test SQLGlot enum to dlt type mapping"""
    dlt_hints = from_sqlglot_type(sqlglot_type)
    assert dlt_hints.get("data_type") == expected_dlt_type


@pytest.mark.parametrize(
    "precision, expected_sqlglot_type",
    [
        (-1, sge.DataType.Type.BIGINT),
        (0, sge.DataType.Type.BIGINT),
        (3, sge.DataType.Type.TINYINT),
        (5, sge.DataType.Type.SMALLINT),
        (8, sge.DataType.Type.MEDIUMINT),
        (10, sge.DataType.Type.INT),
        (19, sge.DataType.Type.BIGINT),
        (39, sge.DataType.Type.INT128),
        (78, sge.DataType.Type.INT256),
    ],
)
def test_to_sqlglot_integer_with_precision(
    precision: int,
    expected_sqlglot_type: sge.DataType.Type,
) -> None:
    """Test dlt `bigint` with precision to a named SQLGlot type"""
    sqlglot_type = to_sqlglot_type(dlt_type="bigint", precision=precision, use_named_types=True)
    assert sqlglot_type == sge.DataType.build(expected_sqlglot_type)


@pytest.mark.parametrize("nullable", [None, True, False])
@pytest.mark.parametrize("use_named_type", [True, False])
def test_to_sqlglot_with_nullable(nullable: Optional[bool], use_named_type: bool) -> None:
    """Test dlt `nullable` hint to SQLGlot data type object."""
    sqlglot_type = to_sqlglot_type("bigint", nullable=nullable, use_named_types=use_named_type)
    if nullable is None:
        assert "nullable" not in sqlglot_type.args
    else:
        assert sqlglot_type.args.get("nullable") == nullable


@pytest.mark.parametrize("nullable", [None, True, False])
def test_from_sqlglot_with_nullable(nullable: Optional[bool]) -> None:
    """Test SQLGlot data type object to dlt hints (nullable)"""
    sqlglot_type = (
        sge.DataType.build("bigint")
        if nullable is None
        else sge.DataType.build("bigint", nullable=nullable)
    )

    dlt_type = from_sqlglot_type(sqlglot_type)
    if nullable is None:
        assert "nullable" not in dlt_type
    else:
        assert dlt_type.get("nullable") == nullable


@pytest.mark.parametrize(
    "timezone, expected_sqlglot_type",
    [
        (None, sge.DataType.Type.TIMESTAMP),
        (True, sge.DataType.Type.TIMESTAMPTZ),
        (False, sge.DataType.Type.TIMESTAMPNTZ),
    ],
)
def test_to_sqlglot_timestamp_with_timezone(timezone, expected_sqlglot_type) -> None:
    """Test dlt `timestamp` with timezone to a named SQLGlot type"""
    sqlglot_type = to_sqlglot_type("timestamp", timezone=timezone, use_named_types=False)
    assert sqlglot_type == sge.DataType.build(expected_sqlglot_type)


@pytest.mark.parametrize(
    "sqlglot_type, expected_timezone",
    [
        (sge.DataType.Type.TIMESTAMP, None),  # default value
        (sge.DataType.Type.TIMESTAMPNTZ, False),
        (sge.DataType.Type.TIMESTAMPLTZ, True),
        (sge.DataType.Type.TIMESTAMPTZ, True),
        (sge.DataType.Type.TIMESTAMP_S, False),
        (sge.DataType.Type.TIMESTAMP_MS, False),
        (sge.DataType.Type.TIMESTAMP_NS, False),
    ],
)
def test_from_sqlglot_timestamp_with_timezone(
    sqlglot_type: sge.DataType.Type, expected_timezone: Optional[bool]
) -> None:
    """Test named SQLGlot type to dlt hints (timezone)"""
    dlt_type = from_sqlglot_type(sqlglot_type)
    if expected_timezone is None:
        assert "timezone" not in dlt_type
    else:
        assert dlt_type.get("timezone") == expected_timezone


@pytest.mark.parametrize(
    "timezone, expected_sqlglot_type",
    [
        (None, sge.DataType.Type.TIME),
        (True, sge.DataType.Type.TIMETZ),
        (False, sge.DataType.Type.TIME),
    ],
)
def test_to_sqlglot_time_with_timezone(timezone, expected_sqlglot_type) -> None:
    """Test dlt `time` with timezone to a named SQLGlot type"""
    sqlglot_type = to_sqlglot_type("time", timezone=timezone, use_named_types=False)
    assert sqlglot_type == sge.DataType.build(expected_sqlglot_type)


@pytest.mark.parametrize(
    "sqlglot_type, expected_timezone",
    [
        (sge.DataType.Type.TIME, False),
        (sge.DataType.Type.TIMETZ, True),
    ],
)
def test_from_sqlglot_time_with_timezone(
    sqlglot_type: sge.DataType.Type, expected_timezone: Optional[bool]
) -> None:
    """Test named SQLGlot type to dlt hints (timezone)"""
    dlt_type = from_sqlglot_type(sqlglot_type)
    if expected_timezone is None:
        assert "timezone" not in dlt_type
    else:
        assert dlt_type.get("timezone") == expected_timezone


@pytest.mark.parametrize(
    "precision, expected_sqlglot_type",
    [
        (None, sge.DataType.Type.TIMESTAMP),  # default value
        (0, sge.DataType.Type.TIMESTAMP_S),
        (3, sge.DataType.Type.TIMESTAMP_MS),
        (9, sge.DataType.Type.TIMESTAMP_NS),
    ],
)
def test_to_sqlglot_timestamp_with_precision(
    precision: Optional[int], expected_sqlglot_type: sge.DataType.Type
) -> None:
    """Test dlt `timestamp` with precision to a named SQLGlot type"""
    sqlglot_type = to_sqlglot_type("timestamp", precision=precision, use_named_types=True)
    assert sqlglot_type == sge.DataType.build(expected_sqlglot_type)


@pytest.mark.parametrize(
    "sqlglot_type, expected_precision",
    [
        (sge.DataType.Type.TIMESTAMP, None),  # default value
        (sge.DataType.Type.TIMESTAMP_S, None),
        (sge.DataType.Type.TIMESTAMP_MS, None),
        (sge.DataType.Type.TIMESTAMP_NS, None),
    ],
)
def test_from_sqlglot_timestamp_with_precision(
    sqlglot_type: sge.DataType.Type, expected_precision: Optional[int]
) -> None:
    dlt_type = from_sqlglot_type(sqlglot_type)
    if expected_precision is None:
        assert "precision" not in dlt_type
    else:
        assert dlt_type.get("precision") == expected_precision


@pytest.mark.parametrize(
    "hints, expected_sqlglot_type",
    [
        ({"data_type": "decimal", "precision": 10, "scale": 2}, sge.DataType.Type.DECIMAL),
        ({"data_type": "bigint", "precision": 17}, sge.DataType.Type.INT),
        ({"data_type": "timestamp", "precision": 0}, sge.DataType.Type.TIMESTAMP),
        (
            {"data_type": "timestamp", "precision": 5, "timezone": True},
            sge.DataType.Type.TIMESTAMPTZ,
        ),
        (
            {"data_type": "timestamp", "precision": 4, "timezone": False},
            sge.DataType.Type.TIMESTAMPNTZ,
        ),
        (
            {"data_type": "timestamp", "precision": 4, "timezone": False},
            sge.DataType.Type.TIMESTAMPNTZ,
        ),
        (
            {"data_type": "time", "precision": 4, "timezone": False},
            sge.DataType.Type.TIME,
        ),
        (
            {"data_type": "time", "precision": 5, "timezone": True},
            sge.DataType.Type.TIMETZ,
        ),
        ({"data_type": "text", "precision": 100}, sge.DataType.Type.TEXT),
        ({"data_type": "binary", "precision": 98}, sge.DataType.Type.VARBINARY),
    ],
)
def test_from_and_to_sqlglot_parameterized_types(
    hints: TColumnType,
    expected_sqlglot_type: sge.DataType.Type,
) -> None:
    """Test dlt hints to SQLGlot and SQLGlot to dlt hints
    using parameterized SQLGlot types.
    """
    dlt_type: TDataType = hints.pop("data_type")

    # create a parameterized SQLGlot DataType
    annotated_sqlglot_type = to_sqlglot_type(
        dlt_type=dlt_type,
        nullable=hints.get("nullable"),
        precision=hints.get("precision"),
        scale=hints.get("scale"),
        timezone=hints.get("timezone"),
        use_named_types=False,
    )
    assert annotated_sqlglot_type.this == expected_sqlglot_type

    # retrieve hints from DataType object; hints are not passed to `from_sqlglot_type()`
    inferred_dlt_type = from_sqlglot_type(annotated_sqlglot_type)
    assert inferred_dlt_type == {"data_type": dlt_type, **hints}


NORMALIZE_DIALECTS = [
    "duckdb",
    "bigquery",
    "clickhouse",
    "redshift",
    "sqlite",
    "athena",
    "snowflake",
    "postgres",
    "tsql",
    "databricks",
    "presto",
    "fabric",
]


def _make_columns(
    names: list[str], data_type: TDataType = "text", nullable: bool = True
) -> TTableSchemaColumns:
    return {n: {"name": n, "data_type": data_type, "nullable": nullable} for n in names}


@pytest.mark.parametrize("dialect", NORMALIZE_DIALECTS)
def test_uuid_expr_for_dialect(dialect: TSqlGlotDialect) -> None:
    """Test that uuid_expr_for_dialect returns a valid expression for each dialect."""
    expr = uuid_expr_for_dialect(dialect, "test_load_id_123")
    assert isinstance(expr, sge.Expression)
    sql = expr.sql(dialect)
    assert len(sql) > 0

    if dialect == "redshift":
        assert "test_load_id_123" in sql
    elif dialect == "clickhouse":
        assert "generateuuidv4" in sql.lower()
    elif dialect == "athena":
        assert "CAST" in sql.upper()


@pytest.mark.parametrize(
    "sql, dialect, expected",
    [
        ("SELECT a, b, c FROM t", "duckdb", ["a", "b", "c"]),
        ("SELECT col1 AS a, col2 AS b FROM t", "duckdb", ["a", "b"]),
        ("SELECT t.a, t.b FROM t", "duckdb", ["a", "b"]),
        ("SELECT a, t.b, col AS c FROM t", "duckdb", ["a", "b", "c"]),
        ("SELECT `t`.`col_a`, `t`.`col_b` FROM t", "bigquery", ["col_a", "col_b"]),
    ],
    ids=["simple", "aliased", "qualified", "mixed", "bigquery_dot"],
)
def test_get_select_column_names(sql: str, dialect: str, expected: List[str]) -> None:
    parsed: sge.Select = sqlglot.parse_one(sql, read=dialect)  # type: ignore[assignment]
    assert get_select_column_names(parsed.selects, dialect) == expected  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "sql",
    ["SELECT * FROM t", "SELECT t.* FROM t"],
    ids=["bare_star", "qualified_star"],
)
def test_get_select_column_names_star_raises(sql: str) -> None:
    parsed: sge.Select = sqlglot.parse_one(sql, read="duckdb")  # type: ignore[assignment]
    with pytest.raises(ValueError, match="Star"):
        get_select_column_names(parsed.selects)


@pytest.mark.parametrize(
    "sql, discard, expected",
    [
        ("SELECT a, b, c, d FROM t", {"b", "d"}, ["a", "c"]),
        ("SELECT a, b, c, d FROM t", {"d", "b"}, ["a", "c"]),
        ("SELECT d, c, b, a FROM t", {"b", "d"}, ["c", "a"]),
        ("SELECT col AS a, t.b, c FROM t", {"b"}, ["a", "c"]),
        ("SELECT a, b, c FROM t", set(), ["a", "b", "c"]),
        ("SELECT a, b, c FROM t", {"x"}, ["a", "b", "c"]),
        ("SELECT a, b, c FROM t", {"a", "b", "c"}, []),
    ],
    ids=[
        "middle_columns",
        "discard_set_order_irrelevant",
        "preserves_select_order",
        "mixed_expr_types",
        "empty_discard",
        "no_match",
        "discard_all",
    ],
)
def test_filter_select_column_names(sql: str, discard: Set[str], expected: List[str]) -> None:
    parsed: sge.Select = sqlglot.parse_one(sql, read="duckdb")  # type: ignore[assignment]
    result = filter_select_column_names(parsed.selects, discard, str.lower)
    assert result == expected


def test_validate_no_star_select() -> None:
    """Test that star selects are rejected and explicit column selects pass."""
    star_select: sge.Select = sqlglot.parse_one("SELECT * FROM t", read="duckdb")  # type: ignore[assignment]
    with pytest.raises(ValueError, match="SELECT \\*"):
        validate_no_star_select(star_select, "duckdb")

    qualified_star: sge.Select = sqlglot.parse_one("SELECT t.* FROM t", read="duckdb")  # type: ignore[assignment]
    with pytest.raises(ValueError, match="SELECT \\*"):
        validate_no_star_select(qualified_star, "duckdb")

    explicit_select: sge.Select = sqlglot.parse_one("SELECT a, b FROM t", read="duckdb")  # type: ignore[assignment]
    validate_no_star_select(explicit_select, "duckdb")


@pytest.mark.parametrize("dialect", NORMALIZE_DIALECTS)
def test_build_outer_select_statement(dialect: TSqlGlotDialect) -> None:
    """Test that build_outer_select_statement wraps query in subquery correctly."""
    casefold = str.lower

    # simple case: columns match select order -> no reordering
    parsed: sge.Select = sqlglot.parse_one("SELECT a, b FROM t", read=dialect)  # type: ignore[assignment]
    columns = _make_columns(["a", "b"])
    outer, needs_reorder = build_outer_select_statement(dialect, parsed, columns, casefold)
    assert isinstance(outer, sge.Select)
    assert not needs_reorder
    sql = outer.sql(dialect)
    assert DLT_SUBQUERY_NAME in sql

    # reorder case: columns in different order -> needs reordering
    columns_reordered = _make_columns(["b", "a"])
    outer2, needs_reorder2 = build_outer_select_statement(
        dialect, parsed, columns_reordered, casefold
    )
    assert needs_reorder2

    # aliased input
    parsed_aliased: sge.Select = sqlglot.parse_one("SELECT col AS a FROM t", read=dialect)  # type: ignore[assignment]
    columns_alias = _make_columns(["a"])
    outer3, needs_reorder3 = build_outer_select_statement(
        dialect, parsed_aliased, columns_alias, casefold
    )
    assert not needs_reorder3
    aliases = [sel.alias for sel in outer3.selects]
    assert "a" in aliases


@pytest.mark.parametrize("dialect", NORMALIZE_DIALECTS)
def test_reorder_or_adjust_outer_select(dialect: TSqlGlotDialect) -> None:
    """Test reordering, NULL insertion for missing nullable, and error for non-nullable."""
    casefold = str.lower

    # reorders to schema order
    parsed: sge.Select = sqlglot.parse_one("SELECT a, b FROM t", read=dialect)  # type: ignore[assignment]
    columns_ab = _make_columns(["a", "b"])
    outer, _ = build_outer_select_statement(dialect, parsed, columns_ab, casefold)
    columns_ba = _make_columns(["b", "a"])
    reorder_or_adjust_outer_select(outer, columns_ba, casefold, "test_schema", "test_table")
    aliases = [sel.alias for sel in outer.selects]
    assert aliases == ["b", "a"]

    # missing nullable column -> NULL alias added
    parsed2: sge.Select = sqlglot.parse_one("SELECT a FROM t", read=dialect)  # type: ignore[assignment]
    columns_with_extra = _make_columns(["a", "c"])
    outer2, _ = build_outer_select_statement(dialect, parsed2, columns_with_extra, casefold)
    reorder_or_adjust_outer_select(
        outer2, columns_with_extra, casefold, "test_schema", "test_table"
    )
    aliases2 = [sel.alias for sel in outer2.selects]
    assert "c" in aliases2
    c_expr = outer2.selects[1]
    assert isinstance(c_expr.this, sge.Null)

    # missing non-nullable column -> CannotCoerceNullException
    parsed3: sge.Select = sqlglot.parse_one("SELECT a FROM t", read=dialect)  # type: ignore[assignment]
    columns_non_nullable: TTableSchemaColumns = {
        "a": {"name": "a", "data_type": "text", "nullable": True},
        "required": {"name": "required", "data_type": "text", "nullable": False},
    }
    outer3, _ = build_outer_select_statement(dialect, parsed3, columns_non_nullable, casefold)
    with pytest.raises(CannotCoerceNullException):
        reorder_or_adjust_outer_select(
            outer3, columns_non_nullable, casefold, "test_schema", "test_table"
        )

    # extra column in select but not in schema -> dropped
    parsed4: sge.Select = sqlglot.parse_one("SELECT a, b, extra FROM t", read=dialect)  # type: ignore[assignment]
    columns_only_ab = _make_columns(["a", "b"])
    outer4, needs_reorder4 = build_outer_select_statement(
        dialect, parsed4, columns_only_ab, casefold
    )
    assert needs_reorder4
    reorder_or_adjust_outer_select(outer4, columns_only_ab, casefold, "test_schema", "test_table")
    aliases4 = [sel.alias for sel in outer4.selects]
    assert aliases4 == ["a", "b"]


@pytest.mark.parametrize(
    "naming_convention",
    [
        "snake_case",
        "tests.common.cases.normalizers.sql_upper",
        "tests.common.cases.normalizers.title_case",
    ],
)
def test_normalize_query_identifiers(naming_convention: str) -> None:
    import importlib
    from dlt.common.libs.sqlglot import normalize_query_identifiers

    # import the naming convention module
    if naming_convention == "snake_case":
        naming_module_path = "dlt.common.normalizers.naming.snake_case"
    else:
        naming_module_path = naming_convention
    naming_module = importlib.import_module(naming_module_path)
    naming = naming_module.NamingConvention()

    # test query with mixed case, paths, and aliases (including __ in table/alias names)
    query = cast(
        sge.Query,
        sqlglot.parse_one("""
            SELECT
                MyColumn AS MyAlias,
                nested__column,
                user__comments__id AS User__Comments__Id,
                value__v_double AS value__v__alias,
                t.col1
            FROM my_schema.MyTable
            JOIN my_schema.User__Comments AS User__Table__Alias ON MyTable.id = User__Table__Alias.user_id
            WHERE AnotherColumn > 10
        """),
    )

    normalized = normalize_query_identifiers(query, naming)
    normalized_sql = normalized.sql()

    # verify table name normalized but schema name unchanged
    assert "my_schema" in normalized_sql  # schema name should be unchanged
    expected_table = naming.normalize_table_identifier("MyTable")
    assert expected_table in normalized_sql

    # verify simple column names normalized
    expected_col1 = naming.normalize_identifier("MyColumn")
    expected_col2 = naming.normalize_identifier("AnotherColumn")
    assert expected_col1 in normalized_sql
    assert expected_col2 in normalized_sql

    # verify aliases normalized
    expected_alias = naming.normalize_identifier("MyAlias")
    assert expected_alias in normalized_sql

    # verify paths preserved (using normalize_path, not normalize_identifier)
    expected_nested = naming.normalize_path("nested__column")
    expected_user_path = naming.normalize_path("user__comments__id")
    expected_variant = naming.normalize_path("value__v_double")
    assert expected_nested in normalized_sql
    assert expected_user_path in normalized_sql
    assert expected_variant in normalized_sql

    # verify nested table name preserved
    expected_nested_table = naming.normalize_tables_path("User__Comments")
    assert expected_nested_table in normalized_sql

    # verify aliases with __ preserved (using normalize_path for aliases)
    expected_col_alias_with_path = naming.normalize_path("User__Comments__Id")
    expected_variant_alias = naming.normalize_path("value__v__alias")
    expected_table_alias_with_path = naming.normalize_path("User__Table__Alias")
    assert expected_col_alias_with_path in normalized_sql
    assert expected_variant_alias in normalized_sql
    assert expected_table_alias_with_path in normalized_sql
