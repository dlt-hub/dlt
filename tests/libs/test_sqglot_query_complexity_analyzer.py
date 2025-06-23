from typing import Any, Dict, List, Union
import pytest
import sqlglot

VARIOUS_QUERIES: List[Dict[str, Union[str, bool]]] = [
    {"description": "star select", "query": "SELECT * FROM my_table", "complex": False},
    {
        "query": "SELECT t.* FROM my_table AS t",
        "complex": False,
        "description": "star select + aliased table",
    },
    {
        "query": "SELECT * FROM my_table ORDER BY col1",
        "complex": False,
        "description": "star select + order",
    },
    {
        "query": "SELECT * FROM my_table LIMIT 10",
        "complex": False,
        "description": "star select + limit",
    },
    {
        "query": "SELECT * FROM my_table OFFSET 3",
        "complex": False,
        "description": "star select + offset",
    },
    {
        "query": "SELECT * FROM my_table WHERE col1 > 0 AND col2 IS NOT NULL",
        "complex": False,
        "description": "star select + where",
    },
    {
        "query": "SELECT t1.col1 AS blah, t1.col2 AS blubb, t1.col3 FROM my_table AS t1",
        "complex": False,
        "description": "full aliased select",
    },
    {
        "query": "SELECT col1 AS renamed, col2, col3 FROM my_table",
        "complex": False,
        "description": "full partially aliased select",
    },
    {
        "query": "SELECT * FROM t1 WHERE t1.id > 5 OFFSET 5 LIMIT 10",
        "complex": False,
        "description": "full select + offset/limit",
    },
    {
        "query": "SELECT col3, col1, col2 FROM my_table",
        "complex": False,
        "description": "full select in different order",
    },
    {
        "query": "SELECT *, '123' AS static_val FROM my_table",
        "complex": False,
        "description": "star select + literal",
    },
    {
        "query": "SELECT t.*, '123' AS static_val FROM my_table AS t",
        "complex": False,
        "description": "star select + literal + aliased table",
    },
    {
        "query": "SELECT col1, col2, col3, 'static' AS static_val FROM my_table",
        "complex": False,
        "description": "full select + literal",
    },
    {
        "query": (
            "WITH my_table AS (SELECT * FROM my_table WHERE col1 = True) SELECT * FROM my_table;"
        ),
        "complex": True,
        "description": "cte + one table ref",
    },
    {
        "query": "WITH temp_table AS (SELECT * FROM my_table) SELECT * FROM temp_table;",
        "complex": True,
        "description": "cte + two table refs",
    },
    {
        "query": "WITH x AS (SELECT a FROM y) SELECT a FROM x",
        "complex": True,
        "description": "cte + two table refs + sqlglot example",
    },
    {
        "query": "SELECT col1, col2 FROM my_table",
        "complex": True,
        "description": "subset select",
    },
    {
        "query": "SELECT col1, col2, '123' AS static_val FROM my_table",
        "complex": True,
        "description": "subset select + literal",
    },
    {
        "query": "SELECT col1, col2, '123' AS col3 FROM my_table",
        "complex": True,
        "description": "subset select + literal as existing col",
    },
    {
        "query": (
            "SELECT * FROM my_table JOIN my_other_table ON my_table.col1 = my_other_table.col1"
        ),
        "complex": True,
        "description": "join",
    },
    {
        "query": "SELECT col1, col2, 'static' || col3 AS col3 FROM my_table",
        "complex": True,
        "description": "concat",
    },
    {
        "query": "SELECT CASE WHEN col1 > 0 THEN 1 ELSE 0 END AS flag FROM my_table",
        "complex": True,
        "description": "case",
    },
    {
        "query": "SELECT MAX(col1) AS max_val FROM my_table",
        "complex": True,
        "description": "aggregation",
    },
    {
        "query": "SELECT col1, col2, col3, UUID() AS new_col FROM my_table",
        "complex": True,
        "description": "function",
    },
    {
        "query": "SELECT col1 FROM my_table GROUP BY col1",
        "complex": True,
        "description": "group",
    },
    {
        "query": "SELECT DISTINCT col1 FROM my_table",
        "complex": True,
        "description": "distinct",
    },
    {
        "query": "SELECT ROW_NUMBER() OVER () AS row_num FROM my_table",
        "complex": True,
        "description": "window",
    },
    {
        "query": "SELECT * FROM my_table UNION ALL SELECT * FROM my_table",
        "complex": True,
        "description": "union",
    },
    {
        "query": "SELECT col1, col1, col2, col3 FROM my_table",
        "complex": True,
        "description": "column selected twice",
    },
    {
        "query": "SELECT col1, col2, col2 AS col3 FROM my_table",
        "complex": True,
        "description": "partial select with alias forming full select",
    },
]


@pytest.mark.parametrize(
    "case",
    VARIOUS_QUERIES,
    ids=[case["description"] for case in VARIOUS_QUERIES],
)
def test_query_complexity_analyzer(case: Dict[str, Any]) -> None:
    from dlt.common.libs.sqlglot import query_is_complex

    columns = {"col1", "col2", "col3", "_dlt_load_id", "_dlt_id"}
    parsed_select = sqlglot.parse_one(case["query"], read="duckdb")
    assert isinstance(parsed_select, (sqlglot.exp.Select, sqlglot.exp.Union))
    assert query_is_complex(parsed_select=parsed_select, columns=columns) == case["complex"]
