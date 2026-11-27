from typing import Any, Union

import sqlglot

import pytest
import sqlglot.expressions as sge
from sqlglot.schema import Schema as SQLGlotSchema, ensure_schema

from dlt import Schema
from dlt.common.libs.sqlglot import TSqlGlotDialect
from dlt.common.schema import TTableSchemaColumns
from dlt.dataset import lineage
from dlt.dataset.exceptions import LineageFailedException


DIALECT: TSqlGlotDialect = "duckdb"


@pytest.fixture
def sqlglot_schema() -> SQLGlotSchema:
    return ensure_schema(
        {
            "db": {
                "table_1": {
                    "col_varchar": sge.DataType.build("VARCHAR", dialect=DIALECT),
                    "col_bool": sge.DataType.build("BOOLEAN", dialect=DIALECT),
                },
                "table_2": {
                    "col_int": sge.DataType.build("BIGINT", dialect=DIALECT),
                    "col_bool": sge.DataType.build("BOOLEAN", dialect=DIALECT),
                },
            }
        }
    )


QUERY_KNOWN_TABLE_STAR_SELECT = "SELECT * FROM table_1"
QUERY_UNKNOWN_TABLE_STAR_SELECT = "SELECT * FROM table_unknown"
QUERY_ANONYMOUS_SELECT = "SELECT LEN(col_varchar) FROM table_1"
QUERY_DROP = "DROP TABLE table_1"
QUERY_UNKNOWN_TABLE_AND_COLUMN_SELECT = "SELECT col_unknown FROM table_unknown"
QUERY_KNOWN_TABLE_AND_UNKNOWN_COLUM_SELECT = "SELECT col_unknown FROM table_1"
QUERY_KNOWN_TABLES_JOIN_STAR_SELECT = """\
    SELECT *
    FROM table_1
    JOIN table_2
    ON table_1.col_bool = table_2.col_bool\
    """
QUERY_KNOWN_AND_UNKNOWN_JOIN_STAR_SELECT = """\
    SELECT *
    FROM table_1
    JOIN table_unknown
    ON table_1.col_bool = table_unknown.col_unknown\
    """
QUERY_KNOWN_AND_UNKNOWN_JOIN_EXPLICIT_COLUMN_SELECT = """\
    SELECT
        table_1.col_varchar,
        table_unknown.col_unknown_1
    FROM table_1
    JOIN table_unknown
    ON table_1.col_bool = table_unknown.col_unknown_2\
    """
QUERY_KNOWN_AND_UNKNOWN_JOIN_STAR_ON_KNOW_TABLE_SELECT = """\
    SELECT
        table_1.*,
        table_unknown.col_unknown_1
    FROM table_1
    JOIN table_unknown
    ON table_1.col_bool = table_unknown.col_unknown_2
    """
# `table_1` qualified with the catalog/db prefix that the sqlglot schema is keyed under
QUERY_DB_QUALIFIED_TABLE_STAR_SELECT = "SELECT * FROM db.table_1"
# the same table qualified with a prefix that is NOT in the sqlglot schema
QUERY_UNKNOWN_DB_QUALIFIED_TABLE_STAR_SELECT = "SELECT * FROM unknown_db.table_1"


@pytest.mark.parametrize(
    "sql_query,expected_dlt_schema",
    [
        (
            QUERY_KNOWN_TABLE_STAR_SELECT,
            {
                "col_varchar": {"name": "col_varchar", "data_type": "text"},
                "col_bool": {"name": "col_bool", "data_type": "bool"},
            },
        ),
        (QUERY_DROP, LineageFailedException()),
        # the qualified query names an anonymous column
        (
            QUERY_ANONYMOUS_SELECT,
            {"_col_0": {"data_type": "bigint", "name": "_col_0"}},
        ),
        # an unqualified column is not attributed to the only unknown table
        (QUERY_UNKNOWN_TABLE_AND_COLUMN_SELECT, LineageFailedException()),
        (QUERY_KNOWN_TABLE_AND_UNKNOWN_COLUM_SELECT, LineageFailedException()),
        # a column qualified with an unknown table resolves without a data type
        (
            QUERY_KNOWN_AND_UNKNOWN_JOIN_EXPLICIT_COLUMN_SELECT,
            {
                "col_unknown_1": {"name": "col_unknown_1"},
                "col_varchar": {"data_type": "text", "name": "col_varchar"},
            },
        ),
        (QUERY_UNKNOWN_TABLE_STAR_SELECT, LineageFailedException()),
        (
            QUERY_KNOWN_TABLES_JOIN_STAR_SELECT,
            {
                "col_varchar": {"name": "col_varchar", "data_type": "text"},
                "col_bool": {"name": "col_bool", "data_type": "bool"},
                "col_int": {"name": "col_int", "data_type": "bigint"},
            },
        ),
        (QUERY_KNOWN_AND_UNKNOWN_JOIN_STAR_SELECT, LineageFailedException()),
        (
            QUERY_KNOWN_AND_UNKNOWN_JOIN_STAR_ON_KNOW_TABLE_SELECT,
            {
                "col_varchar": {"name": "col_varchar", "data_type": "text"},
                "col_bool": {"name": "col_bool", "data_type": "bool"},
                "col_unknown_1": {"name": "col_unknown_1"},
            },
        ),
        # table qualified with the known catalog/db prefix resolves exactly like the unqualified name
        (
            QUERY_DB_QUALIFIED_TABLE_STAR_SELECT,
            {
                "col_varchar": {"name": "col_varchar", "data_type": "text"},
                "col_bool": {"name": "col_bool", "data_type": "bool"},
            },
        ),
        # an unknown prefix does not match `db.table_1`, so the `*` cannot be resolved
        (QUERY_UNKNOWN_DB_QUALIFIED_TABLE_STAR_SELECT, LineageFailedException()),
        # columns without a data type: a column of a table unknown to the schema
        (
            (
                "SELECT u.col_unknown, table_1.col_varchar FROM table_1 JOIN table_unknown AS u"
                " ON table_1.col_bool = u.col_bool"
            ),
            {
                "col_unknown": {"name": "col_unknown"},
                "col_varchar": {"name": "col_varchar", "data_type": "text"},
            },
        ),
        (
            "SELECT q.col_unknown FROM (SELECT * FROM table_unknown) AS q",
            {"col_unknown": {"name": "col_unknown"}},
        ),
        (
            "SELECT u.col_unknown + 1 AS e FROM table_unknown AS u",
            {"e": {"name": "e"}},
        ),
        (
            "SELECT CASE WHEN u.flag THEN u.col_unknown END AS c FROM table_unknown AS u",
            {"c": {"name": "c"}},
        ),
        (
            "SELECT COALESCE(u.col_unknown, NULL) AS c FROM table_unknown AS u",
            {"c": {"name": "c"}},
        ),
        # columns without a data type: an expression whose type sqlglot cannot derive
        ("SELECT NULL AS n FROM table_1", {"n": {"name": "n"}}),
        ("SELECT my_udf(col_varchar) AS f FROM table_1", {"f": {"name": "f"}}),
        ("SELECT json_extract(col_varchar, '$.a') AS j FROM table_1", {"j": {"name": "j"}}),
        # a cast types a column of an unknown table
        (
            "SELECT CAST(u.col_unknown AS BIGINT) AS c FROM table_unknown AS u",
            {"c": {"name": "c", "data_type": "bigint"}},
        ),
    ],
    ids=[
        "known_table_star",
        "drop",
        "anonymous_column",
        "unknown_table_unqualified_column",
        "known_table_unknown_column",
        "unknown_table_qualified_column",
        "unknown_table_star",
        "known_tables_join_star",
        "known_and_unknown_join_star",
        "known_table_star_with_unknown_table_column",
        "known_db_prefix_star",
        "unknown_db_prefix_star",
        "untyped_unknown_table_alias_column",
        "untyped_subquery_over_unknown_table_star",
        "untyped_expression_over_unknown_column",
        "untyped_case_over_unknown_column",
        "untyped_coalesce_unknown_column_and_null",
        "untyped_null_literal",
        "untyped_unknown_function",
        "untyped_dialect_function_without_type",
        "typed_cast_of_unknown_column",
    ],
)
def test_compute_columns_schema(
    sqlglot_schema: SQLGlotSchema,
    sql_query: str,
    expected_dlt_schema: Union[TTableSchemaColumns, Exception],
) -> None:
    expression = sqlglot.parse_one(sql_query)
    if isinstance(expected_dlt_schema, Exception):
        with pytest.raises(LineageFailedException):
            lineage.compute_columns_schema(expression, sqlglot_schema, DIALECT)
    else:
        columns, _ = lineage.compute_columns_schema(expression, sqlglot_schema, DIALECT)
        assert columns == expected_dlt_schema


@pytest.mark.parametrize(
    "sql_query,expected_dlt_schema",
    [
        (QUERY_UNKNOWN_TABLE_AND_COLUMN_SELECT, {"col_unknown": {"name": "col_unknown"}}),
        (QUERY_KNOWN_TABLE_AND_UNKNOWN_COLUM_SELECT, {"col_unknown": {"name": "col_unknown"}}),
        (
            "SELECT col_varchar, col_unknown FROM table_1",
            {
                "col_varchar": {"name": "col_varchar", "data_type": "text"},
                "col_unknown": {"name": "col_unknown"},
            },
        ),
        (QUERY_UNKNOWN_TABLE_STAR_SELECT, {}),
        # the `*` of the unknown table is skipped, other columns are kept
        (
            "SELECT *, 1 AS x FROM table_unknown",
            {"x": {"name": "x", "data_type": "bigint"}},
        ),
        # sqlglot does not expand a `*` when any of the joined tables is unknown
        (QUERY_KNOWN_AND_UNKNOWN_JOIN_STAR_SELECT, {}),
        (
            QUERY_KNOWN_AND_UNKNOWN_JOIN_STAR_ON_KNOW_TABLE_SELECT,
            {
                "col_varchar": {"name": "col_varchar", "data_type": "text"},
                "col_bool": {"name": "col_bool", "data_type": "bool"},
                "col_unknown_1": {"name": "col_unknown_1"},
            },
        ),
        # resolvable queries give the same columns as in strict mode
        (
            QUERY_KNOWN_TABLE_STAR_SELECT,
            {
                "col_varchar": {"name": "col_varchar", "data_type": "text"},
                "col_bool": {"name": "col_bool", "data_type": "bool"},
            },
        ),
        (QUERY_DROP, LineageFailedException()),
    ],
    ids=[
        "unknown_table_unqualified_column",
        "known_table_unknown_column",
        "known_and_unknown_column",
        "unknown_table_star",
        "unknown_table_star_and_literal",
        "known_and_unknown_join_star",
        "known_table_star_with_unknown_table_column",
        "known_table_star",
        "drop",
    ],
)
def test_compute_columns_schema_partial(
    sqlglot_schema: SQLGlotSchema,
    sql_query: str,
    expected_dlt_schema: Union[TTableSchemaColumns, Exception],
) -> None:
    expression = sqlglot.parse_one(sql_query)
    if isinstance(expected_dlt_schema, Exception):
        with pytest.raises(LineageFailedException):
            lineage.compute_columns_schema(expression, sqlglot_schema, DIALECT, allow_partial=True)
    else:
        columns, _ = lineage.compute_columns_schema(
            expression, sqlglot_schema, DIALECT, allow_partial=True
        )
        assert columns == expected_dlt_schema


@pytest.mark.parametrize(
    "names_ref",
    (
        "tests.common.cases.normalizers.sql_upper",
        "tests.common.cases.normalizers.title_case",
    ),
)
def test_star_select_preserves_case_sensitive_identifiers(names_ref: str) -> None:
    """`SELECT *` expansion must keep the case of already normalized dlt identifiers."""
    schema = Schema("d1")
    schema._normalizers_config["names"] = names_ref
    schema.update_normalizers()
    schema.update_table(
        {
            "name": "products",
            "columns": {
                "id": {"data_type": "bigint", "name": "id"},
                "name": {"data_type": "text", "name": "name"},
                "_dlt_load_id": {"data_type": "text", "name": "_dlt_load_id"},
            },
        }
    )
    normalized_table = schema.naming.normalize_tables_path("products")
    expected = [schema.naming.normalize_identifier(c) for c in ("id", "name", "_dlt_load_id")]

    sqlglot_schema = lineage.create_sqlglot_schema({"d1": [schema]}, DIALECT)
    columns, _ = lineage.compute_columns_schema(
        sqlglot.parse_one(f'SELECT * FROM "{normalized_table}"'), sqlglot_schema, DIALECT
    )
    assert list(columns.keys()) == expected
