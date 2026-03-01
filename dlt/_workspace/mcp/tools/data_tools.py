# mypy: disable-error-code="return-value, no-any-return"
from typing import Any, Dict, List, Literal

import sqlglot
import sqlglot.expressions as sge
from pydantic import Field
from fastmcp.exceptions import ToolError

import dlt
from dlt.common.typing import Annotated
from dlt._workspace.cli import formatters
from dlt._workspace.cli.utils import fetch_workspace_info
from dlt._workspace.mcp.tools._context import with_mcp_tool_telemetry

TResultFormat = Literal["markdown", "jsonl"]


def _attach(pipeline_name: str) -> dlt.Pipeline:
    """Attach to a pipeline by name."""
    return dlt.attach(pipeline_name)


def _get_dataset(pipeline_name: str) -> dlt.Dataset:
    """Attach to a pipeline and return its dataset."""
    return _attach(pipeline_name).dataset()


@with_mcp_tool_telemetry()
def list_pipelines() -> List[str]:
    """List all dlt pipelines available in this workspace"""
    from dlt.common.runtime.run_context import active as active_run_context
    from dlt.common.configuration.specs.pluggable_run_context import ProfilesRunContext
    from dlt._workspace.cli.utils import list_local_pipelines

    ctx = active_run_context()
    # in OSS context (no profiles), filter to pipelines created in this project
    project_dir = None if isinstance(ctx, ProfilesRunContext) else ctx.local_dir
    _, pipelines = list_local_pipelines(sort_by_trace=False, run_dir=project_dir)
    return [p["name"] for p in pipelines]


@with_mcp_tool_telemetry()
def get_workspace_info() -> Dict[str, Any]:
    """Get information about the current dlt workspace or project.

    Returns workspace name, directories, active profile, configured profiles,
    and configuration provider locations with their status."""

    info = fetch_workspace_info()
    # prune bulky file-tracking data from toolkit entries
    for entry in info.get("installed_toolkits", {}).values():
        entry.pop("files", None)
    return info


@with_mcp_tool_telemetry()
def list_tables(
    pipeline_name: Annotated[str, Field(description="Name of the dlt pipeline")],
) -> Dict[str, Any]:
    """List all schemas and their data tables for a pipeline. Use the returned table
    names with get_table_schema, get_table_create_sql, and preview_table tools."""
    pipeline = _attach(pipeline_name)
    return {
        "schemas": {
            schema_name: [table["name"] for table in schema.data_tables()]
            for schema_name, schema in pipeline.schemas.items()
        }
    }


@with_mcp_tool_telemetry()
def get_table_schema(
    pipeline_name: Annotated[str, Field(description="Name of the dlt pipeline")],
    table_name: Annotated[str, Field(description="Name of the table")],
) -> Dict[str, Any]:
    """Get the schema of a table including column names, data types, and SQL identifiers.

    Each column includes a sql_identifier field with the properly escaped column name
    for use in SQL queries. The table-level sql_identifier is the escaped table name.
    Use the sql_dialect field to determine the SQL dialect for query construction.
    """
    try:
        dataset = _get_dataset(pipeline_name)
        schema = dataset.schema
        table_dict: Dict[str, Any] = schema.get_table(table_name)  # type: ignore[assignment]

        dialect = dataset.sql_client.capabilities.sqlglot_dialect or "duckdb"
        table_dict["sql_dialect"] = dialect

        table_dict["sql_identifier"] = dataset.sql_client.escape_column_name(
            schema.naming.normalize_tables_path(table_dict["name"])
        )

        for col_schema in table_dict["columns"].values():
            col_schema["sql_identifier"] = dataset.sql_client.escape_column_name(
                schema.naming.normalize_tables_path(col_schema["name"])
            )

        stored_schema = schema.to_dict(remove_defaults=True, bump_version=False)
        return stored_schema["tables"][table_name]
    except Exception as e:
        raise ToolError(
            "Tool `get_table_schema` failed. Verify `pipeline_name` and `table_name`. "
            "If the error persists, try starting a new conversation."
        ) from e


@with_mcp_tool_telemetry()
def get_table_create_sql(
    pipeline_name: Annotated[str, Field(description="Name of the dlt pipeline")],
    table_name: Annotated[str, Field(description="Name of the table")],
) -> str:
    """Get a CREATE TABLE SQL statement for the table in the destination's SQL dialect.

    The DDL includes column names, data types, NOT NULL constraints, and COMMENT
    annotations for columns and tables that have a description in the schema.
    Use this to understand the exact SQL types and write precise queries.
    """
    from dlt.common.libs.sqlglot import to_sqlglot_type

    try:
        dataset = _get_dataset(pipeline_name)
        schema = dataset.schema
        table_dict = schema.get_table(table_name)
        dialect = dataset.sql_client.capabilities.sqlglot_dialect or "duckdb"

        col_defs: List[sge.ColumnDef] = []
        for col in table_dict["columns"].values():
            col_name = dataset.sql_client.escape_column_name(
                schema.naming.normalize_tables_path(col["name"]),
                quote=False,
            )
            sg_type = to_sqlglot_type(
                col["data_type"],
                precision=col.get("precision"),
                scale=col.get("scale"),
                timezone=col.get("timezone"),
                nullable=col.get("nullable", True),
            )
            col_def = sge.ColumnDef(
                this=sge.to_identifier(col_name, quoted=True),
                kind=sg_type,
            )
            col_desc = col.get("description")
            if col_desc:
                col_def.args["constraints"] = [
                    sge.ColumnConstraint(
                        kind=sge.CommentColumnConstraint(this=sge.Literal.string(col_desc))
                    )
                ]
            col_defs.append(col_def)

        table_id = dataset.sql_client.escape_column_name(
            schema.naming.normalize_tables_path(table_dict["name"]),
            quote=False,
        )
        create = sge.Create(
            this=sge.Schema(
                this=sge.to_identifier(table_id, quoted=True),
                expressions=col_defs,
            ),
            kind="TABLE",
        )
        table_desc = table_dict.get("description")
        if table_desc:
            create.args["properties"] = sge.Properties(
                expressions=[sge.SchemaCommentProperty(this=sge.Literal.string(table_desc))]
            )
        return create.sql(dialect=dialect, pretty=True)
    except Exception as e:
        raise ToolError(
            "Tool `get_table_create_sql` failed. Verify `pipeline_name` and `table_name`. "
            "If the error persists, try starting a new conversation."
        ) from e


@with_mcp_tool_telemetry()
def preview_table(
    pipeline_name: Annotated[str, Field(description="Name of the dlt pipeline")],
    table_name: Annotated[str, Field(description="Name of the table to preview")],
    output_format: Annotated[
        TResultFormat,
        Field(description="Output format: 'markdown' table or 'jsonl' (JSON-lines)"),
    ] = "markdown",
) -> str:
    """Get the first 10 rows from a table. Default output is a Markdown table;
    use output_format='jsonl' for structured JSON-lines output with proper type encoding."""
    try:
        relation = _get_dataset(pipeline_name)[table_name].limit(10)
        columns = relation.columns
        rows = relation.fetchall()
        if output_format == "jsonl":
            return formatters.jsonl(columns, rows)
        return formatters.md_table(columns, rows)
    except Exception as e:
        raise ToolError(
            "Tool `preview_table` failed. Verify `pipeline_name` and `table_name`. "
            "If the error persists, try starting a new conversation."
        ) from e


@with_mcp_tool_telemetry()
def execute_sql_query(
    pipeline_name: Annotated[str, Field(description="Name of the dlt pipeline")],
    sql_select_query: Annotated[
        str,
        Field(
            description=(
                "SQL SELECT query to execute. Use column and table names from get_table_schema "
                "or get_table_create_sql tools. Only SELECT statements are allowed."
            )
        ),
    ],
    output_format: Annotated[
        TResultFormat,
        Field(description="Output format: 'markdown' table or 'jsonl' (JSON-lines)"),
    ] = "markdown",
) -> str:
    """Execute a SELECT SQL query and return results. Use get_table_schema to discover
    column names and sql_identifier values. Only SELECT is allowed; INSERT, UPDATE,
    and DELETE are rejected."""
    parsed = sqlglot.parse(sql_select_query)
    if any(
        isinstance(expr, (sqlglot.exp.Insert, sqlglot.exp.Update, sqlglot.exp.Delete))
        for expr in parsed
    ):
        raise ToolError("Data modification statements are not allowed")

    dataset = _get_dataset(pipeline_name)
    relation = dataset(sql_select_query)
    columns = relation.columns
    rows = relation.fetchall()
    if output_format == "jsonl":
        return formatters.jsonl(columns, rows)
    return formatters.md_table(columns, rows)


@with_mcp_tool_telemetry()
def get_row_counts(
    pipeline_name: Annotated[str, Field(description="Name of the dlt pipeline")],
    output_format: Annotated[
        TResultFormat,
        Field(description="Output format: 'markdown' table or 'jsonl' (JSON-lines)"),
    ] = "markdown",
) -> str:
    """Get row counts for all data tables in a pipeline. Default output is a
    Markdown table; use output_format='jsonl' for structured JSON-lines output."""
    try:
        dataset = _get_dataset(pipeline_name)
        relation = dataset.row_counts()
        columns = relation.columns
        rows = relation.fetchall()
        if output_format == "jsonl":
            return formatters.jsonl(columns, rows)
        return formatters.md_table(columns, rows)
    except Exception as e:
        raise ToolError(
            "Tool `get_row_counts` failed. Verify `pipeline_name`. "
            "If the error persists, try starting a new conversation."
        ) from e


TSchemaFormat = Literal["mermaid", "yaml", "dbml"]


@with_mcp_tool_telemetry()
def display_schema(
    pipeline_name: Annotated[str, Field(description="Name of the dlt pipeline")],
    hide_columns: Annotated[
        bool,
        Field(description="Hide column details for better readability of large schemas"),
    ] = False,
    output_format: Annotated[
        TSchemaFormat,
        Field(
            description=(
                "Output format: 'mermaid' ER diagram (default), 'yaml' schema dump, "
                "or 'dbml' database markup (requires dlt[dbml] extra)"
            )
        ),
    ] = "mermaid",
) -> str:
    """Display the pipeline schema. Default is a Mermaid ER diagram; use
    output_format='yaml' for a human-readable schema dump, or 'dbml' for
    database markup language (requires dlt[dbml] extra)."""
    try:
        pipeline = _attach(pipeline_name)
        schema = pipeline.default_schema
        if output_format == "yaml":
            return schema.to_pretty_yaml()
        if output_format == "dbml":
            return schema.to_dbml()
        return schema.to_mermaid(hide_columns=hide_columns)
    except Exception as e:
        raise ToolError(
            "Tool `display_schema` failed. Verify `pipeline_name`. "
            "If the error persists, try starting a new conversation."
        ) from e


@with_mcp_tool_telemetry()
def get_local_pipeline_state(
    pipeline_name: Annotated[str, Field(description="Name of the dlt pipeline")],
) -> Dict[str, Any]:
    """Get the pipeline local state including incremental cursors, resource state,
    and source state. Use this to understand what data has been loaded and the
    current incremental loading positions."""
    try:
        pipeline = _attach(pipeline_name)
        return pipeline.state  # type: ignore[return-value]
    except Exception as e:
        raise ToolError(
            "Tool `get_local_pipeline_state` failed. Verify `pipeline_name`. "
            "If the error persists, try starting a new conversation."
        ) from e


@with_mcp_tool_telemetry()
def pipeline_trace(
    pipeline_name: Annotated[str, Field(description="Name of the dlt pipeline")],
) -> Dict[str, Any]:
    """Get the full trace of the last pipeline run as a JSON dictionary.

    The trace includes timing information, step outcomes (extract, normalize, load),
    resolved configuration values (secrets are filtered), and exception details
    for failed steps.
    """
    pipeline = _attach(pipeline_name)
    trace = pipeline.last_trace
    if trace is None:
        raise ToolError(
            f"No trace found for pipeline '{pipeline_name}'. "
            "The pipeline may not have been run yet."
        )
    return trace.asdict()


__tools__ = (
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
