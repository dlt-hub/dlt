import json
import re

import pytest
from fastmcp.exceptions import ToolError

from dlt.common.configuration.specs.pluggable_run_context import RunContextBase

from dlt._workspace.mcp.tools.data_tools import (
    list_pipelines,
    list_tables,
    get_table_schema,
    get_table_create_sql,
    preview_table,
    execute_sql_query,
    get_row_counts,
    display_schema,
    get_local_pipeline_state,
)

from tests.workspace.utils import pokemon_pipeline_context as pokemon_pipeline_context


def test_list_pipelines(pokemon_pipeline_context: RunContextBase) -> None:
    pipelines = list_pipelines()
    assert "rest_api_pokemon" in pipelines


def test_list_pipelines_project_dir_filter(pokemon_pipeline_context: RunContextBase) -> None:
    # the workspace context has profiles so list_pipelines returns all
    # test filtering via list_local_pipelines directly
    from dlt._workspace.cli.utils import list_local_pipelines

    local_dir = pokemon_pipeline_context.local_dir
    _, pipelines = list_local_pipelines(sort_by_trace=False, run_dir=local_dir)
    assert any(p["name"] == "rest_api_pokemon" for p in pipelines)

    # a different project_dir should filter it out
    _, pipelines = list_local_pipelines(sort_by_trace=False, run_dir="/nonexistent/path")
    assert not any(p["name"] == "rest_api_pokemon" for p in pipelines)


def test_list_tables(pokemon_pipeline_context: RunContextBase) -> None:
    result = list_tables("rest_api_pokemon")
    assert result == {"schemas": {"pokemon": ["pokemon", "berry", "location"]}}


def test_get_table_schema(pokemon_pipeline_context: RunContextBase) -> None:
    schema = get_table_schema("rest_api_pokemon", "pokemon")

    # sql_identifier present, not normalized_name
    assert schema["sql_identifier"] == '"pokemon"'
    assert "normalized_name" not in schema

    assert "columns" in schema
    col_name = schema["columns"]["name"]
    assert col_name["data_type"] == "text"
    assert col_name["sql_identifier"] == '"name"'
    assert "normalized_name" not in col_name
    assert "arrow_data_type" not in col_name

    assert schema["sql_dialect"] == "duckdb"


def test_get_table_create_sql(pokemon_pipeline_context: RunContextBase) -> None:
    ddl = get_table_create_sql("rest_api_pokemon", "pokemon")
    assert "CREATE TABLE" in ddl
    # table name present
    assert '"pokemon"' in ddl
    # column names present
    assert '"name"' in ddl
    assert '"url"' in ddl
    assert '"_dlt_load_id"' in ddl
    assert '"_dlt_id"' in ddl
    # has data types
    assert "TEXT" in ddl.upper() or "VARCHAR" in ddl.upper()


def test_preview_table_md(pokemon_pipeline_context: RunContextBase) -> None:
    result = preview_table("rest_api_pokemon", "pokemon")
    lines = result.strip().splitlines()

    # header + separator + 10 data rows + blank + row count = at least 13 lines
    assert len(lines) >= 12, f"Expected at least 12 lines, got {len(lines)}"

    # header has column names
    header = lines[0]
    assert "name" in header
    assert "url" in header

    # separator line
    assert re.match(r"^\|[\s\-|]+\|$", lines[1])

    # first data row
    assert "bulbasaur" in lines[2]

    # row count
    assert "(10 row(s))" in lines[-1]


def test_preview_table_jsonl(pokemon_pipeline_context: RunContextBase) -> None:
    result = preview_table("rest_api_pokemon", "pokemon", output_format="jsonl")
    lines = result.strip().splitlines()
    assert len(lines) == 10

    # each line is valid JSON with expected keys
    for line in lines:
        obj = json.loads(line)
        assert "name" in obj
        assert "url" in obj
        assert "_dlt_load_id" in obj
        assert "_dlt_id" in obj

    # first row is bulbasaur
    first = json.loads(lines[0])
    assert first["name"] == "bulbasaur"


def test_execute_sql_query(pokemon_pipeline_context: RunContextBase) -> None:
    result = execute_sql_query("rest_api_pokemon", "SELECT * FROM pokemon LIMIT 5")
    assert "(5 row(s))" in result
    assert "bulbasaur" in result


def test_execute_sql_query_jsonl(pokemon_pipeline_context: RunContextBase) -> None:
    result = execute_sql_query(
        "rest_api_pokemon", "SELECT name FROM pokemon LIMIT 3", output_format="jsonl"
    )
    lines = result.strip().splitlines()
    assert len(lines) == 3
    first = json.loads(lines[0])
    assert "name" in first


def test_execute_sql_query_rejects_dml(pokemon_pipeline_context: RunContextBase) -> None:
    with pytest.raises(ToolError, match="modification"):
        execute_sql_query("rest_api_pokemon", "DELETE FROM pokemon")


def test_get_row_counts(pokemon_pipeline_context: RunContextBase) -> None:
    result = get_row_counts("rest_api_pokemon")
    assert "table_name" in result
    assert "row_count" in result
    assert "pokemon" in result
    assert "berry" in result
    assert "location" in result


def test_get_row_counts_jsonl(pokemon_pipeline_context: RunContextBase) -> None:
    result = get_row_counts("rest_api_pokemon", output_format="jsonl")
    lines = result.strip().splitlines()
    assert len(lines) >= 3
    first = json.loads(lines[0])
    assert "table_name" in first
    assert "row_count" in first


def test_display_schema(pokemon_pipeline_context: RunContextBase) -> None:
    result = display_schema("rest_api_pokemon")
    assert "erDiagram" in result
    assert "pokemon" in result


def test_display_schema_hide_columns(pokemon_pipeline_context: RunContextBase) -> None:
    result = display_schema("rest_api_pokemon", hide_columns=True)
    assert "erDiagram" in result
    assert "pokemon" in result


def test_display_schema_dbml(pokemon_pipeline_context: RunContextBase) -> None:
    result = display_schema("rest_api_pokemon", output_format="dbml")
    assert "Table" in result
    assert "pokemon" in result


def test_display_schema_yaml(pokemon_pipeline_context: RunContextBase) -> None:
    result = display_schema("rest_api_pokemon", output_format="yaml")
    assert "pokemon:" in result
    assert "columns:" in result


def test_get_local_pipeline_state(pokemon_pipeline_context: RunContextBase) -> None:
    state = get_local_pipeline_state("rest_api_pokemon")
    assert isinstance(state, dict)
    assert "_state_version" in state
