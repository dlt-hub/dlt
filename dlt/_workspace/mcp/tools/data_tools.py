# mypy: disable-error-code="return-value, no-any-return"
from typing import Any, Dict, List, Literal, Optional

import sqlglot
import sqlglot.expressions as sge
from pydantic import Field
from fastmcp.exceptions import ToolError

import dlt
from dlt.common.schema.exceptions import IncompatibleSchemaException
from dlt.common.schema.schema import Schema
from dlt.common.typing import Annotated
from dlt._workspace.cli import formatters
from dlt._workspace.cli.utils import fetch_workspace_info
from dlt._workspace.mcp.context import with_mcp_tool_telemetry

TResultFormat = Literal["markdown", "jsonl"]


def _attach(pipeline_name: str) -> dlt.Pipeline:
    """Attach to a pipeline by name."""
    return dlt.attach(pipeline_name)


def _get_unified_schema(pipeline: dlt.Pipeline) -> Schema:
    """Build a unified schema from all pipeline schemas.

    Falls back to default schema if naming conventions are incompatible.
    """
    schema_names = list(pipeline.schemas)
    if len(schema_names) <= 1:
        return pipeline.default_schema
    default = pipeline.default_schema
    others = [pipeline.schemas[n] for n in schema_names if n != default.name]
    try:
        return default.unify_schemas(others)
    except IncompatibleSchemaException:
        return default


def _get_dataset(pipeline_name: str) -> dlt.Dataset:
    """Attach to a pipeline and return its dataset with unified schema."""
    pipeline = _attach(pipeline_name)
    return pipeline.dataset(schema=_get_unified_schema(pipeline))


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
    """Get workspace info: name, directories, active profile, and config providers."""

    info = fetch_workspace_info()
    # prune bulky file-tracking data from toolkit entries
    for entry in info.get("installed_toolkits", {}).values():
        entry.pop("files", None)
    return info


@with_mcp_tool_telemetry()
def list_tables(
    pipeline_name: str,
) -> Dict[str, Any]:
    """List all data tables for a pipeline."""
    pipeline = _attach(pipeline_name)
    schema = _get_unified_schema(pipeline)
    return {"tables": [table["name"] for table in schema.data_tables()]}


@with_mcp_tool_telemetry()
def get_table_schema(
    pipeline_name: str,
    table_name: str,
) -> Dict[str, Any]:
    """Get table schema with column names, data types, and escaped sql_identifier fields."""
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
    pipeline_name: str,
    table_name: str,
) -> str:
    """Get CREATE TABLE DDL for the table in the destination's SQL dialect."""
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
    pipeline_name: str,
    table_name: str,
    output_format: Annotated[
        TResultFormat,
        Field(description="Output format: 'markdown' or 'jsonl'"),
    ] = "markdown",
) -> str:
    """Get the first 10 rows from a table."""
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
    pipeline_name: str,
    sql_select_query: Annotated[
        str,
        Field(description="SQL SELECT query to execute (only SELECT is allowed)"),
    ],
    output_format: Annotated[
        TResultFormat,
        Field(description="Output format: 'markdown' or 'jsonl'"),
    ] = "markdown",
) -> str:
    """Execute a read-only SQL query against the pipeline's destination dataset."""
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
    pipeline_name: str,
    output_format: Annotated[
        TResultFormat,
        Field(description="Output format: 'markdown' or 'jsonl'"),
    ] = "markdown",
) -> str:
    """Get row counts for all data tables in a pipeline."""
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
    pipeline_name: str,
    schema_name: Optional[str] = None,
    hide_columns: Annotated[
        bool,
        Field(description="Hide column details for better readability of large schemas"),
    ] = False,
    output_format: Annotated[
        TSchemaFormat,
        Field(description="Output format: 'mermaid', 'yaml', or 'dbml'"),
    ] = "mermaid",
) -> str:
    """Display the pipeline schema as a diagram or structured dump."""
    try:
        pipeline = _attach(pipeline_name)
        schema = pipeline.schemas[schema_name] if schema_name else _get_unified_schema(pipeline)
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
    pipeline_name: str,
) -> Dict[str, Any]:
    """Get pipeline state: incremental cursors, resource state, and source state."""
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
    pipeline_name: str,
) -> Dict[str, Any]:
    """Get the trace of the last pipeline run: timing, step outcomes, and errors."""
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
    # TODO: pipeline_trace returns raw trace.asdict() which is too large for MCP.
    #   Return a summary (timing, step outcomes, errors) like the CLI trace command does.
)

