import pytest

from dlt.destinations.queries import build_row_counts_expr, build_select_expr
from dlt.common.schema.typing import C_DLT_LOAD_ID


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
