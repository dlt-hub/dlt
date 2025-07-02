import pytest

from dlt.destinations.queries import (
    build_row_counts_expr,
    build_select_expr,
    build_insert_expr,
    build_stored_state_expr,
    build_stored_schema_expr,
    build_info_schema_columns_expr,
    replace_placeholders,
)
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


def test_with_load_id_filter() -> None:
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


def test_select_star() -> None:
    stmt = build_select_expr("events", ["*"])
    expected = 'SELECT * FROM "events"'
    assert stmt.sql() == expected

    stmt = build_select_expr("events")
    assert stmt.sql() == expected


def test_selected_columns() -> None:
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


def test_build_insert_expr() -> None:
    stmt = build_insert_expr("my_table", ["col1", "col2", "col3"], quoted_identifiers=True)
    expected = 'INSERT INTO "my_table" ("col1", "col2", "col3") VALUES (?, ?, ?)'
    assert stmt.sql() == expected

    stmt = build_insert_expr("my_table", ["col1", "col2"], quoted_identifiers=False)
    expected = "INSERT INTO my_table (col1, col2) VALUES (?, ?)"
    assert stmt.sql() == expected

    # Test single column
    stmt = build_insert_expr("users", ["name"], quoted_identifiers=True)
    expected = 'INSERT INTO "users" ("name") VALUES (?)'
    assert stmt.sql() == expected


def test_build_stored_state_expr() -> None:
    stmt = build_stored_state_expr(
        pipeline_name="test_pipeline",
        state_table_name="_dlt_pipeline_state",
        state_table_cols=["Version", "engine_versioN", "pipeline_Name", "sTate", "created_at"],
        loads_table_name="_dlt_loads",
        c_load_id="load_id",
        c_dlt_load_id="_dlt_load_id",
        c_pipeline_name="pipeline_name",
        c_status="status",
    )

    expected = (
        'SELECT "Version", "engine_versioN", "pipeline_Name", "sTate", "created_at" '
        'FROM "_dlt_pipeline_state" AS s '
        'JOIN "_dlt_loads" AS l ON l."load_id" = s."_dlt_load_id" '
        'WHERE "pipeline_name" = \'test_pipeline\' AND l."status" = 0 '
        'ORDER BY "load_id" DESC '
        "LIMIT 1"
    )
    assert stmt.sql() == expected


def test_build_stored_schema_expr() -> None:
    # With version hash
    stmt = build_stored_schema_expr(
        table_name="_dlt_version",
        version_table_schema_columns=["version_hash", "schema", "inserted_at"],
        version_hash="abc123def",
        c_version_hash="version_hash",
    )

    expected = (
        'SELECT "version_hash", "schema", "inserted_at" '
        'FROM "_dlt_version" '
        "WHERE \"version_hash\" = 'abc123def' "
        "LIMIT 1"
    )
    assert stmt.sql() == expected

    # With schema name
    stmt = build_stored_schema_expr(
        table_name="_dlt_version",
        version_table_schema_columns=["version_hash", "schema", "inserted_at"],
        schema_name="test_schema",
        c_inserted_at="inserted_at",
        c_schema_name="schema_name",
    )

    expected = (
        'SELECT "version_hash", "schema", "inserted_at" '
        'FROM "_dlt_version" '
        "WHERE \"schema_name\" = 'test_schema' "
        'ORDER BY "inserted_at" DESC'
    )
    assert stmt.sql() == expected


@pytest.mark.parametrize("with_catalog_name", [True, False])
@pytest.mark.parametrize("with_folded_tables", [True, False])
def test_build_info_schema_columns_expr(with_catalog_name: bool, with_folded_tables: bool) -> None:
    # Test with catalog and table names
    catalog_name = "test_catalog" if with_catalog_name else None
    folded_table_names = ["table1", "table2"] if with_folded_tables else None

    stmt, params = build_info_schema_columns_expr(
        schema_name="test_schema",
        storage_table_query_columns=["table_name", "column_name", "data_type"],
        catalog_name=catalog_name,
        folded_table_names=folded_table_names,
    )

    if with_catalog_name and with_folded_tables:
        expected = (  # NULLS LAST is added by sqlglot for the default dialect and some others
            "SELECT table_name, column_name, data_type "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE table_catalog = ? AND table_schema = ? AND table_name IN (?, ?) "
            "ORDER BY table_name NULLS LAST, ordinal_position NULLS LAST"
        )
        expected_params = ["test_catalog", "test_schema", "table1", "table2"]
    elif not with_catalog_name and not with_folded_tables:
        expected = (
            "SELECT table_name, column_name, data_type "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE table_schema = ? "
            "ORDER BY table_name NULLS LAST, ordinal_position NULLS LAST"
        )
        expected_params = ["test_schema"]
    elif with_folded_tables:
        expected = (
            "SELECT table_name, column_name, data_type "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE table_schema = ? AND table_name IN (?, ?) "
            "ORDER BY table_name NULLS LAST, ordinal_position NULLS LAST"
        )
        expected_params = ["test_schema", "table1", "table2"]
    else:
        expected = (
            "SELECT table_name, column_name, data_type "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE table_catalog = ? AND table_schema = ? "
            "ORDER BY table_name NULLS LAST, ordinal_position NULLS LAST"
        )
        expected_params = ["test_catalog", "test_schema"]
    assert stmt.sql() == expected
    assert params == expected_params


def test_replace_placeholders() -> None:
    # Test basic tuple replacement
    query = "INSERT INTO table (col1, col2, col3) VALUES (?, ?, ?)"
    result = replace_placeholders(query, "postgres")
    expected = "INSERT INTO table (col1, col2, col3) VALUES (%s, %s, %s)"
    assert result == expected

    # Test multiple tuples
    query = "INSERT INTO table (col1, col2) VALUES (?, ?), (?, ?)"
    result = replace_placeholders(query, "postgres")
    expected = "INSERT INTO table (col1, col2) VALUES (%s, %s), (%s, %s)"
    assert result == expected

    # Test standalone placeholder
    query = "SELECT * FROM table WHERE id = ?"
    result = replace_placeholders(query, "postgres")
    expected = "SELECT * FROM table WHERE id = %s"
    assert result == expected

    # Test ClickHouse format
    query = "INSERT INTO table (col1, col2) VALUES ({?: }, {?: })"
    result = replace_placeholders(query, "clickhouse")
    expected = "INSERT INTO table (col1, col2) VALUES (%s, %s)"
    assert result == expected

    # Test ClickHouse standalone
    query = "SELECT * FROM table WHERE id = {?: }"
    result = replace_placeholders(query, "clickhouse")
    expected = "SELECT * FROM table WHERE id = %s"
    assert result == expected

    # Test mixed case with spaces
    query = "INSERT INTO table (col1, col2, col3) VALUES ( ? , ? , ? )"
    result = replace_placeholders(query, "postgres")
    expected = "INSERT INTO table (col1, col2, col3) VALUES (%s, %s, %s)"
    assert result == expected

    # Test no placeholders
    query = "SELECT * FROM table"
    result = replace_placeholders(query, "postgres")
    expected = "SELECT * FROM table"
    assert result == expected

    # Test placeholder token appearing elsewhere
    query = "INSERT INTO my??table (col?, col?col) VALUES (?, ?)"
    result = replace_placeholders(query, "postgres")
    expected = "INSERT INTO my??table (col?, col?col) VALUES (%s, %s)"
    assert result == expected
