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


@pytest.mark.parametrize(
    "sql_query,config,expect_raise,expected_schema",
    [
        (
            QUERY_KNOWN_TABLE_STAR_SELECT,
            {},
            False,
            {
                "col_varchar": {"name": "col_varchar", "data_type": "text"},
                "col_bool": {"name": "col_bool", "data_type": "bool"},
            },
        ),
        (QUERY_GIBBERISH, {"allow_partial": True}, False, {}),
        (QUERY_GIBBERISH, {"allow_partial": False}, True, None),
        (QUERY_DROP, {"allow_partial": True}, False, {}),
        (QUERY_DROP, {"allow_partial": False}, True, None),
        (
            QUERY_ANONYMOUS_SELECT,
            {"allow_anonymous_columns": True},
            False,
            {"_col_0": {"data_type": "bigint", "name": "_col_0"}},
        ),
        (
            QUERY_ANONYMOUS_SELECT,
            {"allow_anonymous_columns": False},
            True,
            None,
        ),
        (
            QUERY_UNKNOWN_TABLE_AND_COLUMN_SELECT,
            {"infer_sqlglot_schema": True},
            False,
            {"col_unknown": {"name": "col_unknown"}},
        ),
        (
            QUERY_UNKNOWN_TABLE_AND_COLUMN_SELECT,
            {"infer_sqlglot_schema": False},
            True,
            None,
        ),
        (
            QUERY_KNOWN_AND_UNKNOWN_JOIN_EXPLICIT_COLUMN_SELECT,
            {"infer_sqlglot_schema": True},
            False,
            {
                "col_unknown_1": {"name": "col_unknown_1"},
                "col_varchar": {"data_type": "text", "name": "col_varchar"},
            },
        ),
        (
            QUERY_KNOWN_AND_UNKNOWN_JOIN_EXPLICIT_COLUMN_SELECT,
            {"infer_sqlglot_schema": False},
            False,
            {
                "col_unknown_1": {"name": "col_unknown_1"},
                "col_varchar": {"data_type": "text", "name": "col_varchar"},
            },
        ),
        (
            QUERY_KNOWN_TABLE_STAR_SELECT,
            {"allow_partial": False},
            False,
            {
                "col_varchar": {"name": "col_varchar", "data_type": "text"},
                "col_bool": {"name": "col_bool", "data_type": "bool"},
            },
        ),
        (
            QUERY_UNKNOWN_TABLE_STAR_SELECT,
            {"allow_partial": True},
            False,
            {},
        ),
        (
            QUERY_UNKNOWN_TABLE_STAR_SELECT,
            {"allow_partial": False},
            True,
            None,
        ),
        (
            QUERY_KNOWN_TABLES_JOIN_STAR_SELECT,
            {"allow_partial": False},
            False,
            {
                "col_varchar": {"name": "col_varchar", "data_type": "text"},
                "col_bool": {"name": "col_bool", "data_type": "bool"},
                "col_int": {"name": "col_int", "data_type": "bigint"},
            },
        ),
        (
            QUERY_KNOWN_AND_UNKNOWN_JOIN_STAR_SELECT,
            {"allow_partial": True},
            False,
            {},
        ),
        (
            QUERY_KNOWN_AND_UNKNOWN_JOIN_STAR_SELECT,
            {"allow_partial": False},
            True,
            None,
        ),
        (
            QUERY_KNOWN_AND_UNKNOWN_JOIN_STAR_ON_KNOW_TABLE_SELECT,
            {"allow_partial": False},
            False,
            {
                "col_varchar": {"name": "col_varchar", "data_type": "text"},
                "col_bool": {"name": "col_bool", "data_type": "bool"},
                "col_unknown_1": {"name": "col_unknown_1"},
            },
        ),
    ],
)
def test_compute_columns_schema(
    sql_query: str,
    config: dict,
    expect_raise: bool,
    expected_schema: dict,
    sqlglot_schema,
) -> None:
    if expect_raise is False:
        assert expected_schema == lineage.compute_columns_schema(
            sql_query=sql_query,
            sqlglot_schema=sqlglot_schema,
            dialect=sqlglot_schema.dialect,
            **config,
        )
    else:
        with pytest.raises(LineageFailedException):
            lineage.compute_columns_schema(
                sql_query=sql_query,
                sqlglot_schema=sqlglot_schema,
                dialect=sqlglot_schema.dialect,
                **config,
            )
