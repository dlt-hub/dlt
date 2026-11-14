import json
import os
import re
from pathlib import Path
from typing import Generator, List
from unittest.mock import patch

import pytest
from fastmcp.exceptions import ToolError

import dlt
from dlt.common.configuration.specs.pluggable_run_context import RunContextBase

from dlt._workspace._known_env import WORKSPACE__PROFILE
from dlt._workspace.cli.exceptions import AiContextApiError
from dlt._workspace.cli.utils import list_local_pipelines
from dlt._workspace.mcp.context import TEnvironmentArgument, with_mcp_tool_telemetry
from dlt._workspace.mcp.tools.data_tools import TMcpSchemaFormat, TResultFormat
from dlt._workspace.mcp.tools.context_tools import search_dlthub_sources
from dlt._workspace.mcp.tools.toolkit_tools import list_toolkits, toolkit_info
from dlt._workspace.mcp.tools.data_tools import (
    _ensure_read_only,
    get_workspace_info,
    list_pipelines,
    list_profiles,
    list_tables,
    get_table_schema,
    get_table_create_sql,
    preview_table,
    execute_sql_query,
    get_row_counts,
    export_schema,
    get_local_pipeline_state,
)

from tests.workspace.utils import (
    declare_environment,
    isolated_workspace,
    pokemon_pipeline_context as pokemon_pipeline_context,
)


def test_list_profiles() -> None:
    with isolated_workspace("default") as ctx:
        profiles = list_profiles()
        assert [p["name"] for p in profiles] == ctx.available_profiles()
        assert [p["name"] for p in profiles if p["is_current"]] == ["dev"]
        # the call runs with the profile named in the argument
        assert [p["name"] for p in list_profiles(profile="prod") if p["is_current"]] == ["prod"]


def test_tool_profile_argument() -> None:
    """A tool runs with the profile named in the call. The next call names none and runs with
    the active profile of the process again."""
    with isolated_workspace("default"):
        info = get_workspace_info(profile="prod")
        assert info["profile"]["name"] == "prod"
        assert info["profile"]["data_dir"].endswith("prod")
        assert dlt.current.workspace().profile == "prod"
        assert get_workspace_info()["profile"]["name"] == "dev"
        with pytest.raises(ToolError, match="not a profile name"):
            get_workspace_info(profile="no/such")
        # a tool works only with declared profiles, an environment is not one
        with pytest.raises(ToolError, match="`staging` is not a profile declared"):
            get_workspace_info(profile="staging")
        declare_environment(dlt.current.workspace(), "ci")
        with pytest.raises(ToolError, match="`ci` is an environment"):
            get_workspace_info(profile="ci")
        assert get_workspace_info(profile="ci-write")["profile"]["name"] == "ci-write"


def test_tool_environment_argument(monkeypatch: pytest.MonkeyPatch) -> None:
    @with_mcp_tool_telemetry()
    def deployment_tool(environment: TEnvironmentArgument = None) -> str:
        ctx = dlt.current.workspace()
        return f"{ctx.environment}/{ctx.profile}/{os.environ[WORKSPACE__PROFILE]}"

    monkeypatch.delenv(WORKSPACE__PROFILE, raising=False)
    with isolated_workspace("default"):
        # dev is active: the default environment is used with its default write profile, which
        # `WORKSPACE__PROFILE` names to the processes that the tool starts
        assert deployment_tool() == "prod/prod/prod"
        assert deployment_tool(environment="prod") == "prod/prod/prod"
        declare_environment(dlt.current.workspace(), "ci")
        assert deployment_tool(environment="ci") == "ci/ci-write/ci-write"
        # the variable is set for the call only
        assert WORKSPACE__PROFILE not in os.environ
        # a profile is not an environment: a profile of one, its default write profile or a
        # local one
        for profile in ("access", "ci-write", "dev"):
            with pytest.raises(ToolError, match=f"`{profile}` is not an environment declared"):
                deployment_tool(environment=profile)
        with pytest.raises(ToolError, match="not an environment name"):
            deployment_tool(environment="ci/prod")


def test_tool_profile_argument_outside_workspace() -> None:
    with isolated_workspace("legacy", required=None):
        with pytest.raises(ToolError, match="needs a dltHub workspace"):
            get_workspace_info(profile="prod")
        # without the argument the tool works in the OSS context
        assert get_workspace_info()["profile"] is None


def test_list_pipelines(pokemon_pipeline_context: RunContextBase) -> None:
    pipelines = list_pipelines()
    assert "rest_api_pokemon" in pipelines


def test_list_pipelines_project_dir_filter(pokemon_pipeline_context: RunContextBase) -> None:
    local_dir = pokemon_pipeline_context.local_dir
    _, pipelines, _ = list_local_pipelines(sort_by_trace=False, run_dir=local_dir)
    assert any(p["name"] == "rest_api_pokemon" for p in pipelines)

    # a different project_dir should filter it out
    _, pipelines, _ = list_local_pipelines(sort_by_trace=False, run_dir="/nonexistent/path")
    assert not any(p["name"] == "rest_api_pokemon" for p in pipelines)


def test_list_tables(pokemon_pipeline_context: RunContextBase) -> None:
    result = list_tables("rest_api_pokemon")
    assert result == {"tables": ["pokemon", "berry", "location"]}


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
    assert '"pokemon"' in ddl
    assert '"name"' in ddl
    assert '"url"' in ddl
    assert '"_dlt_load_id"' in ddl
    assert '"_dlt_id"' in ddl
    assert "TEXT" in ddl.upper() or "VARCHAR" in ddl.upper()


@pytest.mark.parametrize(
    "output_format",
    ["markdown", "jsonl"],
)
def test_preview_table(
    pokemon_pipeline_context: RunContextBase, output_format: TResultFormat
) -> None:
    result = preview_table("rest_api_pokemon", "pokemon", output_format=output_format)
    if output_format == "markdown":
        lines = result.strip().splitlines()
        assert len(lines) >= 12
        assert "name" in lines[0]
        assert re.match(r"^\|[\s\-|]+\|$", lines[1])
        assert "bulbasaur" in lines[2]
        assert "(10 row(s))" in lines[-1]
    else:
        lines = result.strip().splitlines()
        assert len(lines) == 10
        for line in lines:
            obj = json.loads(line)
            assert "name" in obj
            assert "url" in obj
        assert json.loads(lines[0])["name"] == "bulbasaur"


@pytest.mark.parametrize(
    "output_format",
    ["markdown", "jsonl"],
)
def test_execute_sql_query(
    pokemon_pipeline_context: RunContextBase, output_format: TResultFormat
) -> None:
    result = execute_sql_query(
        "rest_api_pokemon",
        "SELECT name FROM pokemon LIMIT 5",
        output_format=output_format,
    )
    if output_format == "markdown":
        assert "(5 row(s))" in result
        assert "bulbasaur" in result
    else:
        lines = result.strip().splitlines()
        assert len(lines) == 5
        assert "name" in json.loads(lines[0])


@pytest.mark.parametrize(
    "query",
    [
        "DELETE FROM pokemon",
        "UPDATE pokemon SET name = 'x'",
        "INSERT INTO pokemon VALUES (1)",
        # a mutating statement nested in a CTE has a SELECT at the root
        "WITH x AS (DELETE FROM pokemon RETURNING *) SELECT * FROM x",
        "DROP TABLE pokemon",
        "CREATE TABLE evil AS SELECT * FROM pokemon",
        "TRUNCATE TABLE pokemon",
        "ALTER TABLE pokemon ADD COLUMN x INT",
        "COPY (SELECT 1) TO '/tmp/out.csv'",
        "ATTACH 'other.db'",
        # table functions that attach a database or run a statement on an attached one
        "SELECT * FROM postgres_execute('pg', 'DROP TABLE pokemon')",
        "SELECT * FROM sqlite_attach('/tmp/other.db')",
        "SELECT 1; DROP TABLE pokemon",
    ],
    ids=[
        "delete",
        "update",
        "insert",
        "cte-delete",
        "drop",
        "create-as-select",
        "truncate",
        "alter",
        "copy-to",
        "attach",
        "postgres-execute",
        "sqlite-attach",
        "multi-statement",
    ],
)
def test_execute_sql_query_is_read_only(
    pokemon_pipeline_context: RunContextBase, query: str
) -> None:
    """`data: [read]` must not reach the host or write the dataset — GHSA-xxqf-4m3r-qx6c."""
    with pytest.raises(ToolError, match="allowed"):
        execute_sql_query("rest_api_pokemon", query)


def test_execute_sql_query_still_runs_a_select(pokemon_pipeline_context: RunContextBase) -> None:
    assert "pokemon" in execute_sql_query("rest_api_pokemon", "SELECT * FROM pokemon LIMIT 1")
    # a CTE that only reads is not collateral damage
    assert execute_sql_query(
        "rest_api_pokemon", "WITH x AS (SELECT 1 AS a) SELECT * FROM x", output_format="jsonl"
    )


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM read_csv('/tmp/rows.csv')",
        "SELECT * FROM read_parquet('s3://bucket/x.parquet')",
        "SELECT * FROM '/tmp/rows.parquet'",
        "SELECT * FROM postgres_query('pg', 'SELECT 1')",
    ],
    ids=["read-csv", "read-parquet", "bare-path", "postgres-query"],
)
def test_read_only_guard_lets_reads_through(query: str) -> None:
    """Reading a file or a foreign database is not a write; the local access axis is not enforced here."""
    _ensure_read_only(query, "duckdb")


@pytest.mark.parametrize(
    "output_format",
    ["markdown", "jsonl"],
)
def test_get_row_counts(
    pokemon_pipeline_context: RunContextBase, output_format: TResultFormat
) -> None:
    result = get_row_counts("rest_api_pokemon", output_format=output_format)
    if output_format == "markdown":
        assert "table_name" in result
        assert "row_count" in result
        assert "pokemon" in result
        assert "berry" in result
        assert "location" in result
    else:
        lines = result.strip().splitlines()
        assert len(lines) >= 3
        first = json.loads(lines[0])
        assert "table_name" in first
        assert "row_count" in first


@pytest.mark.parametrize(
    "output_format,expected_substr",
    [
        ("mermaid", "erDiagram"),
        ("yaml", "columns:"),
        ("dbml", "Table"),
    ],
)
def test_export_schema(
    pokemon_pipeline_context: RunContextBase,
    output_format: TMcpSchemaFormat,
    expected_substr: str,
) -> None:
    result = export_schema("rest_api_pokemon", output_format=output_format)
    assert expected_substr in result
    assert "pokemon" in result


def test_export_schema_save_to_file(
    pokemon_pipeline_context: RunContextBase, tmp_path: Path
) -> None:
    out = tmp_path / "schema.yaml"
    result = export_schema("rest_api_pokemon", output_format="yaml", save_to_file=str(out))
    assert "Schema saved to" in result
    content = out.read_text(encoding="utf-8")
    assert "pokemon:" in content
    assert "columns:" in content


def test_get_local_pipeline_state(pokemon_pipeline_context: RunContextBase) -> None:
    state = get_local_pipeline_state("rest_api_pokemon")
    assert isinstance(state, dict)
    assert "_state_version" in state


@dlt.source(name="fruits")
def _fruits_source():
    @dlt.resource
    def fruit():
        yield [{"name": "apple", "color": "red"}, {"name": "banana", "color": "yellow"}]

    return fruit


@dlt.source(name="veggies")
def _veggies_source():
    @dlt.resource
    def vegetable():
        yield [{"name": "carrot", "color": "orange"}, {"name": "pea", "color": "green"}]

    return vegetable


@pytest.fixture
def multi_schema_pipeline_context() -> Generator[RunContextBase, None, None]:
    with isolated_workspace(name="pipelines") as ctx:
        p = dlt.pipeline("multi_schema", destination="duckdb", dataset_name="multi_ds")
        p.run(_fruits_source())
        p.run(_veggies_source())
        yield ctx


def test_list_tables_multi_schema(multi_schema_pipeline_context: RunContextBase) -> None:
    result = list_tables("multi_schema")
    tables = result["tables"]
    assert "fruit" in tables
    assert "vegetable" in tables


def test_get_table_schema_unified(
    multi_schema_pipeline_context: RunContextBase,
) -> None:
    schema = get_table_schema("multi_schema", "vegetable")
    assert "columns" in schema
    assert "name" in schema["columns"]


def test_preview_table_unified(
    multi_schema_pipeline_context: RunContextBase,
) -> None:
    result = preview_table("multi_schema", "vegetable")
    assert "carrot" in result


def test_export_schema_unified(
    multi_schema_pipeline_context: RunContextBase,
) -> None:
    result = export_schema("multi_schema", output_format="yaml")
    assert "vegetable" in result
    assert "fruit" in result


def test_list_toolkits_returns_dict() -> None:
    mock_toolkits = {"init": {"name": "init", "version": "1.0"}}
    with (
        patch("dlt._workspace.mcp.tools.toolkit_tools.fetch_workbench_base") as mock_base,
        patch(
            "dlt._workspace.mcp.tools.toolkit_tools.fetch_workbench_toolkits",
            return_value=(mock_toolkits, []),
        ),
    ):
        mock_base.return_value = Path("/fake")
        result = list_toolkits()
    assert result == mock_toolkits


def test_list_toolkits_raises_tool_error_on_missing_repo() -> None:
    with patch(
        "dlt._workspace.mcp.tools.toolkit_tools.fetch_workbench_base",
        side_effect=FileNotFoundError("repo not found"),
    ):
        with pytest.raises(ToolError, match="repo not found"):
            list_toolkits()


def test_toolkit_info_returns_details() -> None:
    mock_info = {"name": "rest-api", "version": "1.0", "skills": [], "commands": [], "rules": []}
    with patch(
        "dlt._workspace.mcp.tools.toolkit_tools.fetch_workbench_toolkit_info",
        return_value=mock_info,
    ):
        result = toolkit_info("rest-api")
    assert result == mock_info


def test_toolkit_info_raises_tool_error_when_not_found() -> None:
    with patch(
        "dlt._workspace.mcp.tools.toolkit_tools.fetch_workbench_toolkit_info",
        return_value=None,
    ):
        with pytest.raises(ToolError, match="not found"):
            toolkit_info("nonexistent")


def test_search_dlthub_sources_returns_results() -> None:
    mock_results = [{"source_name": "github", "description": "GitHub API"}]
    with patch(
        "dlt._workspace.mcp.tools.context_tools.search_sources",
        return_value=mock_results,
    ):
        result = search_dlthub_sources(query="github")
    assert result == mock_results


def test_search_dlthub_sources_wraps_api_error() -> None:
    with patch(
        "dlt._workspace.mcp.tools.context_tools.search_sources",
        side_effect=AiContextApiError("API down"),
    ):
        with pytest.raises(ToolError, match="API down"):
            search_dlthub_sources(query="test")
