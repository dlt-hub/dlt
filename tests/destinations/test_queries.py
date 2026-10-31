from typing import Any, Callable, Iterator, List, Optional, cast

import duckdb
import pytest
import sqlglot
from sqlglot import exp as sge
from sqlglot.schema import MappingSchema as SQLGlotSchema

import dlt
from dlt.common.libs.sqlglot import IdentifiersBinding, bind_query
from dlt.common.schema.typing import C_DLT_LOAD_ID
from dlt.dataset.lineage import compute_columns_schema
from dlt.destinations.queries import build_row_counts_expr, build_select_expr
from dlt.destinations.sql_client import SqlClientBase
from dlt.destinations.impl.duckdb.configuration import DuckDbClientConfiguration


@pytest.fixture
def duckdb_sql_client() -> Iterator[SqlClientBase[Any]]:
    """In-memory duckdb sql client bound to `dataset_name`."""
    con = duckdb.connect(":memory:")
    destination_client = dlt.destinations.duckdb(con).client(
        dlt.Schema("foobar"), DuckDbClientConfiguration()._bind_dataset_name("dataset_name")
    )
    with destination_client.sql_client as sql_client:
        yield sql_client


def test_basic() -> None:
    stmt = build_row_counts_expr("my_table", quoted_identifiers=True)
    expected = (
        """SELECT 'my_table' AS table_name, """ """COUNT(*) AS row_count """ """FROM "my_table\""""
    )
    assert stmt.sql() == expected

    stmt = build_row_counts_expr("my_table", quoted_identifiers=False)
    expected = "SELECT 'my_table' AS table_name, COUNT(*) AS row_count FROM my_table"
    assert stmt.sql() == expected


def test_with_load_id_filter():
    with pytest.raises(ValueError) as py_exc:
        _ = build_row_counts_expr(
            table_name="my_table",
            dlt_load_id_col=C_DLT_LOAD_ID,
        )
    assert "Both `load_id` and `dlt_load_id_col` must be provided together." in py_exc.value.args

    stmt = build_row_counts_expr(
        table_name="my_table", dlt_load_id_col=C_DLT_LOAD_ID, load_id="abcd-123"
    )
    expected = (
        "SELECT 'my_table' AS table_name, "
        "COUNT(*) AS row_count "
        'FROM "my_table" '
        "WHERE \"_dlt_load_id\" = 'abcd-123'"
    )
    assert stmt.sql() == expected


def test_select_star():
    stmt = build_select_expr("events", ["*"])
    expected = 'SELECT * FROM "events"'
    assert stmt.sql() == expected

    stmt = build_select_expr("events")
    assert stmt.sql() == expected


def test_selected_columns():
    stmt = build_select_expr(
        table_name="events",
        selected_columns=["event_id", "created_at"],
        quoted_identifiers=True,
    )
    expected = 'SELECT "event_id", "created_at" FROM "events"'
    assert stmt.sql() == expected
    stmt = build_select_expr(
        table_name="events",
        selected_columns=["event_id", "created_at"],
        quoted_identifiers=False,
    )
    expected = "SELECT event_id, created_at FROM events"
    assert stmt.sql() == expected


def test_qualified_query():
    sqlglot_schema = SQLGlotSchema(
        {"dataset_name": {"items": {"id": str}, "double_items": {"double_id": str, "id": str}}}
    )
    query_expr = sqlglot.parse_one("""
SELECT
    i.id AS id,
    di.double_id AS double_id
FROM dataset_name.items AS i
JOIN dataset_name.double_items as di
ON (i.id = di.id)
WHERE i.id < 20
ORDER BY i.id ASC
""")

    expected_qualified_query = (
        "SELECT i.id AS id, di.double_id AS double_id FROM dataset_name.items AS i JOIN"
        " dataset_name.double_items AS di ON (i.id = di.id) WHERE i.id < 20 ORDER BY i.id ASC"
    )

    _, qualified_query_expr = compute_columns_schema(
        expression=query_expr,
        sqlglot_schema=sqlglot_schema,
        dialect="duckdb",
    )
    qualified_query = qualified_query_expr.sql()

    assert qualified_query == expected_qualified_query


def test_normalize_query(duckdb_sql_client: SqlClientBase[Any]) -> None:
    sqlglot_schema = SQLGlotSchema(
        {"dataset_name": {"items": {"id": str}, "double_items": {"double_id": str, "id": str}}}
    )
    qualified_query_expr = sqlglot.parse_one("""
SELECT
    i.id AS id,
    di.double_id AS double_id
FROM dataset_name.items AS i
JOIN dataset_name.double_items as di
ON (i.id = di.id)
WHERE i.id < 20
ORDER BY i.id ASC
""")

    expected_normalized_query = (
        'SELECT "i"."id" AS "id", "di"."double_id" AS "double_id" FROM "dataset_name"."items" AS'
        ' "i" JOIN "dataset_name"."double_items" AS "di" ON ("i"."id" = "di"."id") WHERE'
        ' "i"."id" < 20 ORDER BY "i"."id" ASC'
    )

    default_binding = IdentifiersBinding(
        (None, duckdb_sql_client.dataset_name),
        duckdb_sql_client.make_qualified_table_name_path,
        duckdb_sql_client.capabilities.casefold_identifier,
    )
    normalized_query_expr = bind_query(
        qualified_query=cast(sge.Query, qualified_query_expr),
        sqlglot_schema=sqlglot_schema,
        bindings={"dataset_name": default_binding},
        default_binding=default_binding,
    )

    assert normalized_query_expr.sql() == expected_normalized_query


def test_bind_query_per_dataset_casefold() -> None:
    """Each identifier folds with the rules of the dataset that owns its table."""
    sqlglot_schema = SQLGlotSchema(
        {
            "crm": {"users": {"id": str}},
            "sales": {"orders": {"id": str, "user_id": str, "total": str}},
        }
    )
    qualified_query_expr = sqlglot.parse_one(
        "SELECT u.id AS id, o.total FROM crm.users AS u JOIN sales.orders AS o ON u.id ="
        " o.user_id WHERE o.total > 100"
    )

    def _path_builder(
        table_name: Optional[str],
        quote: bool = True,
        casefold: bool = True,
        dataset_name: Optional[str] = None,
        catalog: Optional[str] = None,
    ) -> List[str]:
        path = [] if catalog is None else [catalog]
        path.append(dataset_name)
        path.append(table_name)
        return path

    default_binding = IdentifiersBinding((None, "crm_ds"), _path_builder, str)
    foreign_binding = IdentifiersBinding(("attach_sales", "sales_ds"), _path_builder, str.upper)

    bound = bind_query(
        qualified_query=cast(sge.Query, qualified_query_expr),
        sqlglot_schema=sqlglot_schema,
        bindings={"crm": default_binding, "sales": foreign_binding},
        default_binding=default_binding,
    )

    # the foreign path and the foreign columns fold to upper case. The primary side does not
    # change. An alias keeps the logical name of the foreign-folded output column
    assert (
        bound.sql()
        == 'SELECT "u"."id" AS "id", "o"."TOTAL" AS "total" FROM "crm_ds"."users" AS "u" JOIN'
        ' "attach_sales"."SALES_DS"."ORDERS" AS "o" ON "u"."id" = "o"."USER_ID" WHERE'
        ' "o"."TOTAL" > 100'
    )


@pytest.mark.parametrize("casefold", (str.upper, str.lower), ids=("upper", "lower"))
def test_bind_query_keeps_declared_output_names(
    duckdb_sql_client: SqlClientBase[Any], casefold: Callable[[str], str]
) -> None:
    """A case-folding binding restores the output names that the query declares. A projection
    over a literal keeps its alias and does not take the text of the literal."""
    binding = IdentifiersBinding(
        (None, duckdb_sql_client.dataset_name),
        duckdb_sql_client.make_qualified_table_name_path,
        casefold,
    )
    sqlglot_schema = SQLGlotSchema({"my_table": {"id": str}})

    bound = bind_query(
        qualified_query=build_row_counts_expr("my_table"),
        sqlglot_schema=sqlglot_schema,
        bindings={},
        default_binding=binding,
    )
    assert [proj.output_name for proj in bound.selects] == ["table_name", "row_count"]

    # a literal or NULL projection in a user query keeps its alias as well
    bound = bind_query(
        qualified_query=cast(
            sge.Query, sqlglot.parse_one("SELECT 'items' AS kind, NULL AS missing FROM my_table")
        ),
        sqlglot_schema=sqlglot_schema,
        bindings={},
        default_binding=binding,
    )
    assert [proj.output_name for proj in bound.selects] == ["kind", "missing"]


def test_bind_query_with_legacy_path_signature(
    duckdb_sql_client: SqlClientBase[Any],
) -> None:
    """Sql clients overriding `make_qualified_table_name_path` without the `dataset_name`
    parameter keep working for tables without a dataset qualifier."""

    def _legacy_path_builder(
        table_name: Optional[str], quote: bool = True, casefold: bool = True
    ) -> List[str]:
        return duckdb_sql_client.make_qualified_table_name_path(
            table_name, quote=quote, casefold=casefold
        )

    binding = IdentifiersBinding(
        (None, duckdb_sql_client.dataset_name),
        _legacy_path_builder,
        duckdb_sql_client.capabilities.casefold_identifier,
    )
    sqlglot_schema = SQLGlotSchema({"items": {"id": str}})
    bound = bind_query(
        qualified_query=cast(sge.Query, sqlglot.parse_one("SELECT id FROM items")),
        sqlglot_schema=sqlglot_schema,
        bindings={},
        default_binding=binding,
    )
    assert bound.sql() == 'SELECT "id" FROM "dataset_name"."items"'

    # a dataset qualifier requires the `dataset_name` parameter on the override
    qualified_schema = SQLGlotSchema({"other_dataset": {"items": {"id": str}}})
    with pytest.raises(TypeError, match="dataset_name"):
        bind_query(
            qualified_query=cast(
                sge.Query, sqlglot.parse_one("SELECT id FROM other_dataset.items")
            ),
            sqlglot_schema=qualified_schema,
            bindings={"other_dataset": binding},
            default_binding=binding,
        )
