import pytest
import sqlglot.expressions as sge
from sqlglot.schema import ensure_schema

from dlt.transformations import lineage
from dlt.transformations.exceptions import LineageFailedException


def test_compute_columns_schema() -> None:
    dialect = "duckdb"
    table_name = "table_1"
    sqlglot_schema = ensure_schema(
        {
            "database_name": {
                table_name: {
                    "col_varchar": sge.DataType.build("VARCHAR", dialect=dialect),
                    "col_bool": sge.DataType.build("BOOLEAN", dialect=dialect),
                }
            }
        }
    )

    dlt_table_schema = lineage.compute_columns_schema(
        sql_query=f"SELECT * FROM {table_name};",
        sqlglot_schema=sqlglot_schema,
        dialect=dialect,
    )
    assert dlt_table_schema == {
        "col_varchar": {"name": "col_varchar", "data_type": "text"},
        "col_bool": {"name": "col_bool", "data_type": "bool"},
    }


def test_compute_columns_schema_allow_fail() -> None:
    dialect = "duckdb"
    table_name = "table_1"
    sqlglot_schema = ensure_schema(
        {
            "database_name": {
                table_name: {
                    "col_varchar": sge.DataType.build("VARCHAR", dialect=dialect),
                    "col_bool": sge.DataType.build("BOOLEAN", dialect=dialect),
                }
            }
        }
    )

    # raise on sqlglot.errors.ParserError when allow_fail=False
    invalid_query = "%&/ GIBBERISH"
    invalid_empty_schema = lineage.compute_columns_schema(
        sql_query=invalid_query,
        sqlglot_schema=sqlglot_schema,
        dialect=dialect,
        allow_partial=True,
    )
    assert invalid_empty_schema == {}
    with pytest.raises(LineageFailedException):
        lineage.compute_columns_schema(
            sql_query=invalid_query,
            sqlglot_schema=sqlglot_schema,
            dialect=dialect,
            allow_partial=False,
        )

    # raise on non-select query
    drop_query = "DROP TABLE table_1;"
    drop_empty_schema = lineage.compute_columns_schema(
        sql_query=drop_query,
        sqlglot_schema=sqlglot_schema,
        dialect=dialect,
        allow_partial=True,
    )
    assert drop_empty_schema == {}
    with pytest.raises(LineageFailedException):
        lineage.compute_columns_schema(
            sql_query=drop_query,
            sqlglot_schema=sqlglot_schema,
            dialect=dialect,
            allow_partial=False,
        )


def test_compute_columns_schema_anonymous_columns() -> None:
    """Handle anonymous columns.

    Anonymous columns are commonly produced by operation in SELECT.
    For example: SELECT SUM(cost) FROM items;
    """
    dialect = "duckdb"
    table_name = "table_1"
    sqlglot_schema = ensure_schema(
        {
            "database_name": {
                table_name: {
                    "col_varchar": sge.DataType.build("VARCHAR", dialect=dialect),
                    "col_bool": sge.DataType.build("BOOLEAN", dialect=dialect),
                }
            }
        }
    )

    anonymous_query = f"SELECT LEN(col_varchar) FROM {table_name}"
    dlt_table_schema = lineage.compute_columns_schema(
        sql_query=anonymous_query,
        sqlglot_schema=sqlglot_schema,
        dialect=dialect,
        allow_anonymous_columns=True,
    )
    assert dlt_table_schema == {"_col_0": {"data_type": "bigint", "name": "_col_0"}}
    with pytest.raises(LineageFailedException):
        lineage.compute_columns_schema(
            sql_query=anonymous_query,
            sqlglot_schema=sqlglot_schema,
            dialect=dialect,
            allow_anonymous_columns=False,
        )


def test_compute_columns_schema_infer_sqlglot_schema() -> None:
    """Handle unknown columns and tables.

    A column or table is "unknown" if it's not found in the SQLGlot schema when
    resolving lineage.
    """
    dialect = "duckdb"
    sqlglot_schema = ensure_schema(
        {
            "database_name": {
                "table_1": {
                    "col_varchar": sge.DataType.build("VARCHAR", dialect=dialect),
                    "col_bool": sge.DataType.build("BOOLEAN", dialect=dialect),
                }
            }
        }
    )

    select_query = "SELECT col_unknown FROM table_unknown;"
    expected_unknown_dlt_schema = {"col_unknown": {"name": "col_unknown"}}
    unknown_dlt_schema = lineage.compute_columns_schema(
        sql_query=select_query,
        sqlglot_schema=sqlglot_schema,
        dialect=dialect,
        infer_sqlglot_schema=True,
    )
    assert unknown_dlt_schema == expected_unknown_dlt_schema
    with pytest.raises(LineageFailedException):
        lineage.compute_columns_schema(
            sql_query=select_query,
            sqlglot_schema=sqlglot_schema,
            dialect=dialect,
            infer_sqlglot_schema=False,
        )

    # TODO false-y values `schema={}` or `schema=None` trigger the identical behavior
    # We could guard against that explicitly
    unknown_schema_from_empty = lineage.compute_columns_schema(
        sql_query=select_query,
        sqlglot_schema=ensure_schema({}),
        dialect=dialect,
    )
    assert unknown_schema_from_empty == expected_unknown_dlt_schema

    unknown_schema_from_none = lineage.compute_columns_schema(
        sql_query=select_query,
        sqlglot_schema=None,
        dialect=dialect,
    )
    assert unknown_schema_from_none == expected_unknown_dlt_schema

    join_with_unknown_query = """\
    SELECT
        table_1.col_varchar,
        table_unknown.col_unknown_1
    FROM table_1
    JOIN table_unknown
    ON table_1.col_bool = table_unknown.col_unknown_2\
    """
    expected_unknown_join_dlt_schema = {
        "col_unknown_1": {"name": "col_unknown_1"},
        "col_varchar": {"data_type": "text", "name": "col_varchar"},
    }
    unknown_join_dlt_schema = lineage.compute_columns_schema(
        sql_query=join_with_unknown_query,
        sqlglot_schema=sqlglot_schema,
        dialect=dialect,
        infer_sqlglot_schema=True,
    )
    assert unknown_join_dlt_schema == expected_unknown_join_dlt_schema
    unknown_join_dlt_schema = lineage.compute_columns_schema(
        sql_query=join_with_unknown_query,
        sqlglot_schema=sqlglot_schema,
        dialect=dialect,
        infer_sqlglot_schema=False,
    )
    assert unknown_join_dlt_schema == expected_unknown_join_dlt_schema


def test_compute_columns_schema_unknown_column_selection() -> None:
    """Handle the column selection can't be inferred."""
    dialect = "duckdb"
    sqlglot_schema = ensure_schema(
        {
            "database_name": {
                "table_1": {
                    "col_varchar": sge.DataType.build("VARCHAR", dialect=dialect),
                    "col_bool": sge.DataType.build("BOOLEAN", dialect=dialect),
                },
                "table_2": {
                    "col_int": sge.DataType.build("BIGINT", dialect=dialect),
                    "col_bool": sge.DataType.build("BOOLEAN", dialect=dialect),
                },
            }
        }
    )

    # `table_1` is in schema and `SELECT *` can be resolved
    known_select_star_dlt_schema = lineage.compute_columns_schema(
        sql_query="SELECT * FROM table_1;",
        sqlglot_schema=sqlglot_schema,
        dialect=dialect,
        allow_partial=False,
    )
    assert known_select_star_dlt_schema == {
        "col_varchar": {"name": "col_varchar", "data_type": "text"},
        "col_bool": {"name": "col_bool", "data_type": "bool"},
    }

    # `table_unknown` isn't in the schema and `SELECT *` can't be resolved
    unknown_select_star_query = "SELECT * FROM table_unknown;"
    unknown_star_dlt_schema = lineage.compute_columns_schema(
        sql_query=unknown_select_star_query,
        sqlglot_schema=sqlglot_schema,
        dialect=dialect,
        allow_partial=True,
    )
    assert unknown_star_dlt_schema == {}
    with pytest.raises(LineageFailedException):
        lineage.compute_columns_schema(
            sql_query=unknown_select_star_query,
            sqlglot_schema=sqlglot_schema,
            dialect=dialect,
            allow_partial=False,
        )

    # `SELECT *` can be resolved for joined tables
    known_join_select_star_dlt_schema = lineage.compute_columns_schema(
        sql_query="SELECT * FROM table_1 JOIN table_2 ON table_1.col_bool = table_2.col_bool;",
        sqlglot_schema=sqlglot_schema,
        dialect=dialect,
        allow_partial=False,
    )
    assert known_join_select_star_dlt_schema == {
        "col_varchar": {"name": "col_varchar", "data_type": "text"},
        "col_bool": {"name": "col_bool", "data_type": "bool"},
        "col_int": {"name": "col_int", "data_type": "bigint"},
    }

    # SELECT * can't be resolved on one of the joined table
    unknown_join_select_star_query = (
        "SELECT * FROM table_1 JOIN table_unknown ON table_1.col_bool = table_unknown.col_unknown"
    )
    unknown_join_select_star_dlt_schema = lineage.compute_columns_schema(
        sql_query=unknown_join_select_star_query,
        sqlglot_schema=sqlglot_schema,
        dialect=dialect,
        allow_partial=True,
    )
    # NOTE even columns of known `table_1` can't be resolved
    assert unknown_join_select_star_dlt_schema == {}
    with pytest.raises(LineageFailedException):
        lineage.compute_columns_schema(
            sql_query=unknown_join_select_star_query,
            sqlglot_schema=sqlglot_schema,
            dialect=dialect,
            allow_partial=False,
        )

    # `SELECT known_table.*` can be resolved!
    partially_unknown_join_select_star_dlt_schema = lineage.compute_columns_schema(
        sql_query="""\
        SELECT
            table_1.*,
            table_unknown.col_unknown_1
        FROM table_1
        JOIN table_unknown
        ON table_1.col_bool = table_unknown.col_unknown_2
        """,
        sqlglot_schema=sqlglot_schema,
        dialect=dialect,
        allow_partial=False,
    )
    # only the content of `table_1` is included
    assert partially_unknown_join_select_star_dlt_schema == {
        "col_varchar": {"name": "col_varchar", "data_type": "text"},
        "col_bool": {"name": "col_bool", "data_type": "bool"},
        "col_unknown_1": {"name": "col_unknown_1"},  # TODO no `data_type`
    }
