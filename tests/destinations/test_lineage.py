from typing import Any, Union

import sqlglot

import pytest
import sqlglot.expressions as sge
from sqlglot.schema import Schema as SQLGlotSchema, ensure_schema

from dlt.common.schema import TTableSchemaColumns
from dlt.dataset import lineage
from dlt.dataset.exceptions import LineageFailedException


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
    "sql_query,config,expected_dlt_schema",
    [
        (
            QUERY_KNOWN_TABLE_STAR_SELECT,
            {},
            {
                "col_varchar": {"name": "col_varchar", "data_type": "text"},
                "col_bool": {"name": "col_bool", "data_type": "bool"},
            },
        ),
        (QUERY_DROP, {"allow_partial": True}, {}),
        (QUERY_DROP, {"allow_partial": False}, LineageFailedException()),
        (
            QUERY_ANONYMOUS_SELECT,
            {"allow_anonymous_columns": True},
            {"_col_0": {"data_type": "bigint", "name": "_col_0"}},
        ),
        # TODO: fix this
        # (
        #     QUERY_ANONYMOUS_SELECT,
        #     {"allow_anonymous_columns": False},
        #     LineageFailedException(),
        # ),
        (
            QUERY_UNKNOWN_TABLE_AND_COLUMN_SELECT,
            {"infer_sqlglot_schema": True},
            {"col_unknown": {"name": "col_unknown"}},
        ),
        (
            QUERY_UNKNOWN_TABLE_AND_COLUMN_SELECT,
            {"infer_sqlglot_schema": False},
            LineageFailedException(),
        ),
        (
            QUERY_KNOWN_AND_UNKNOWN_JOIN_EXPLICIT_COLUMN_SELECT,
            {"infer_sqlglot_schema": True},
            {
                "col_unknown_1": {"name": "col_unknown_1"},
                "col_varchar": {"data_type": "text", "name": "col_varchar"},
            },
        ),
        (
            QUERY_KNOWN_AND_UNKNOWN_JOIN_EXPLICIT_COLUMN_SELECT,
            {"infer_sqlglot_schema": False},
            {
                "col_unknown_1": {"name": "col_unknown_1"},
                "col_varchar": {"data_type": "text", "name": "col_varchar"},
            },
        ),
        (
            QUERY_KNOWN_TABLE_STAR_SELECT,
            {"allow_partial": False},
            {
                "col_varchar": {"name": "col_varchar", "data_type": "text"},
                "col_bool": {"name": "col_bool", "data_type": "bool"},
            },
        ),
        (
            QUERY_UNKNOWN_TABLE_STAR_SELECT,
            {"allow_partial": True},
            {},
        ),
        (
            QUERY_UNKNOWN_TABLE_STAR_SELECT,
            {"allow_partial": False},
            LineageFailedException(),
        ),
        (
            QUERY_KNOWN_TABLES_JOIN_STAR_SELECT,
            {"allow_partial": False},
            {
                "col_varchar": {"name": "col_varchar", "data_type": "text"},
                "col_bool": {"name": "col_bool", "data_type": "bool"},
                "col_int": {"name": "col_int", "data_type": "bigint"},
            },
        ),
        (
            QUERY_KNOWN_AND_UNKNOWN_JOIN_STAR_SELECT,
            {"allow_partial": True},
            {},
        ),
        (
            QUERY_KNOWN_AND_UNKNOWN_JOIN_STAR_SELECT,
            {"allow_partial": False},
            LineageFailedException(),
        ),
        (
            QUERY_KNOWN_AND_UNKNOWN_JOIN_STAR_ON_KNOW_TABLE_SELECT,
            {"allow_partial": False},
            {
                "col_varchar": {"name": "col_varchar", "data_type": "text"},
                "col_bool": {"name": "col_bool", "data_type": "bool"},
                "col_unknown_1": {"name": "col_unknown_1"},
            },
        ),
    ],
)
def test_compute_columns_schema(
    sqlglot_schema: SQLGlotSchema,
    sql_query: str,
    config: dict[str, Any],
    expected_dlt_schema: Union[TTableSchemaColumns, Exception],
) -> None:
    if isinstance(expected_dlt_schema, Exception):
        with pytest.raises(LineageFailedException):
            lineage.compute_columns_schema(
                expression=sqlglot.parse_one(sql_query),
                sqlglot_schema=sqlglot_schema,
                dialect=sqlglot_schema.dialect,  # type: ignore[arg-type]
                **config,
            )
    else:
        assert (
            expected_dlt_schema
            == lineage.compute_columns_schema(
                expression=sqlglot.parse_one(sql_query),
                sqlglot_schema=sqlglot_schema,
                dialect=sqlglot_schema.dialect,  # type: ignore[arg-type]
                **config,
            )[0]
        )
