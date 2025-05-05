import pytest
import sqlglot.expressions as sge
from sqlglot.schema import Schema as SQLGlotSchema, ensure_schema

from dlt.transformations import lineage
from dlt.transformations.exceptions import LineageFailedException


@pytest.fixture
def sqlglot_schema() -> SQLGlotSchema:
    dialect = "duckdb"
    return ensure_schema(
        {
            "db": {
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


QUERY_KNOWN_TABLE_STAR_SELECT = "SELECT * FROM table_1"
QUERY_UNKNOWN_TABLE_STAR_SELECT = "SELECT * FROM table_unknown"
QUERY_ANONYMOUS_SELECT = "SELECT LEN(col_varchar) FROM table_1"
QUERY_GIBBERISH = "%&/ GIBBERISH"
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


def test_compute_columns_schema(sqlglot_schema) -> None:
    dlt_table_schema = lineage.compute_columns_schema(
        sql_query="SELECT * FROM table_1;",
        sqlglot_schema=sqlglot_schema,
        dialect=sqlglot_schema.dialect,
    )
    assert dlt_table_schema == {
        "col_varchar": {"name": "col_varchar", "data_type": "text"},
        "col_bool": {"name": "col_bool", "data_type": "bool"},
    }


@pytest.mark.parametrize("sql_query", ["%&/ GIBBERISH", "DROP TABLE table_1;"])
def test_compute_columns_schema_allow_partial(sql_query, sqlglot_schema) -> None:
    invalid_empty_schema = lineage.compute_columns_schema(
        sql_query=sql_query,
        sqlglot_schema=sqlglot_schema,
        dialect=sqlglot_schema.dialect,
        allow_partial=True,
    )
    assert invalid_empty_schema == {}

    with pytest.raises(LineageFailedException):
        lineage.compute_columns_schema(
            sql_query=sql_query,
            sqlglot_schema=sqlglot_schema,
            dialect=sqlglot_schema.dialect,
            allow_partial=False,
        )


def test_compute_columns_schema_anonymous_columns(sqlglot_schema) -> None:
    """Handle anonymous columns.

    Anonymous columns are commonly produced by operation in SELECT.
    For example: SELECT SUM(cost) FROM items;
    """
    anonymous_query = "SELECT LEN(col_varchar) FROM table_1"
    dlt_table_schema = lineage.compute_columns_schema(
        sql_query=anonymous_query,
        sqlglot_schema=sqlglot_schema,
        dialect=sqlglot_schema.dialect,
        allow_anonymous_columns=True,
    )
    assert dlt_table_schema == {"_col_0": {"data_type": "bigint", "name": "_col_0"}}

    with pytest.raises(LineageFailedException):
        lineage.compute_columns_schema(
            sql_query=anonymous_query,
            sqlglot_schema=sqlglot_schema,
            dialect=sqlglot_schema.dialect,
            allow_anonymous_columns=False,
        )


def test_compute_columns_schema_infer_sqlglot_schema(sqlglot_schema) -> None:
    """Handle unknown columns and tables.

    A column or table is "unknown" if it's not found in the SQLGlot schema when
    resolving lineage.
    """
    select_query = "SELECT col_unknown FROM table_unknown;"
    expected_unknown_dlt_schema = {"col_unknown": {"name": "col_unknown"}}
    unknown_dlt_schema = lineage.compute_columns_schema(
        sql_query=select_query,
        sqlglot_schema=sqlglot_schema,
        dialect=sqlglot_schema.dialect,
        infer_sqlglot_schema=True,
    )
    assert unknown_dlt_schema == expected_unknown_dlt_schema
    with pytest.raises(LineageFailedException):
        lineage.compute_columns_schema(
            sql_query=select_query,
            sqlglot_schema=sqlglot_schema,
            dialect=sqlglot_schema.dialect,
            infer_sqlglot_schema=False,
        )

    # TODO false-y values `schema={}` or `schema=None` trigger the identical behavior
    # We could guard against that explicitly
    unknown_schema_from_empty = lineage.compute_columns_schema(
        sql_query=select_query,
        sqlglot_schema=ensure_schema({}),
        dialect=sqlglot_schema.dialect,
    )
    assert unknown_schema_from_empty == expected_unknown_dlt_schema

    unknown_schema_from_none = lineage.compute_columns_schema(
        sql_query=select_query,
        sqlglot_schema=None,
        dialect=sqlglot_schema.dialect,
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
        dialect=sqlglot_schema.dialect,
        infer_sqlglot_schema=True,
    )
    assert unknown_join_dlt_schema == expected_unknown_join_dlt_schema
    unknown_join_dlt_schema = lineage.compute_columns_schema(
        sql_query=join_with_unknown_query,
        sqlglot_schema=sqlglot_schema,
        dialect=sqlglot_schema.dialect,
        infer_sqlglot_schema=False,
    )
    assert unknown_join_dlt_schema == expected_unknown_join_dlt_schema


def test_compute_columns_schema_unknown_column_selection(sqlglot_schema) -> None:
    """Handle the column selection can't be inferred."""
    # `table_1` is in schema and `SELECT *` can be resolved
    known_select_star_dlt_schema = lineage.compute_columns_schema(
        sql_query="SELECT * FROM table_1;",
        sqlglot_schema=sqlglot_schema,
        dialect=sqlglot_schema.dialect,
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
        dialect=sqlglot_schema.dialect,
        allow_partial=True,
    )
    assert unknown_star_dlt_schema == {}
    with pytest.raises(LineageFailedException):
        lineage.compute_columns_schema(
            sql_query=unknown_select_star_query,
            sqlglot_schema=sqlglot_schema,
            dialect=sqlglot_schema.dialect,
            allow_partial=False,
        )

    # `SELECT *` can be resolved for joined tables
    known_join_select_star_dlt_schema = lineage.compute_columns_schema(
        sql_query="SELECT * FROM table_1 JOIN table_2 ON table_1.col_bool = table_2.col_bool;",
        sqlglot_schema=sqlglot_schema,
        dialect=sqlglot_schema.dialect,
        allow_partial=False,
    )
    assert known_join_select_star_dlt_schema == {
        "col_varchar": {"name": "col_varchar", "data_type": "text"},
        "col_bool": {"name": "col_bool", "data_type": "bool"},
        "col_int": {"name": "col_int", "data_type": "bigint"},
    }

    # SELECT * can't be resolved on one of the joined table
    unknown_join_select_star_query = "SELECT * FROM table_1 JOIN table_unknown ON table_1.col_bool = table_unknown.col_unknown"
    unknown_join_select_star_dlt_schema = lineage.compute_columns_schema(
        sql_query=unknown_join_select_star_query,
        sqlglot_schema=sqlglot_schema,
        dialect=sqlglot_schema.dialect,
        allow_partial=True,
    )
    # NOTE even columns of known `table_1` can't be resolved
    assert unknown_join_select_star_dlt_schema == {}
    with pytest.raises(LineageFailedException):
        lineage.compute_columns_schema(
            sql_query=unknown_join_select_star_query,
            sqlglot_schema=sqlglot_schema,
            dialect=sqlglot_schema.dialect,
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
        dialect=sqlglot_schema.dialect,
        allow_partial=False,
    )
    # only the content of `table_1` is included
    assert partially_unknown_join_select_star_dlt_schema == {
        "col_varchar": {"name": "col_varchar", "data_type": "text"},
        "col_bool": {"name": "col_bool", "data_type": "bool"},
        "col_unknown_1": {"name": "col_unknown_1"},  # TODO no `data_type`
    }
