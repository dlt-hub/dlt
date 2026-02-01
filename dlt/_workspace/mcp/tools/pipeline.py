# mypy: disable-error-code="return-value, no-any-return"
from mcp.server.fastmcp.exceptions import ToolError
from typing import Any

import dlt
from dlt.common.pipeline import get_dlt_pipelines_dir
from dlt.common.storages.file_storage import FileStorage

from dlt._workspace.mcp.tools import helpers


def available_pipelines() -> list[str]:
    """List all available dlt pipelines. Each pipeline has several tables."""
    pipelines_dir = get_dlt_pipelines_dir()
    storage = FileStorage(pipelines_dir)
    dirs = storage.list_folder_dirs(".", to_root=False)
    return dirs


def available_tables(pipeline_name: str) -> dict[str, Any]:
    """List all available tables in the specified pipeline."""
    pipeline = dlt.attach(pipeline_name)
    return {
        "schemas": {
            schema_name: [table["name"] for table in schema.data_tables()]
            for schema_name, schema in pipeline.schemas.items()
        }
    }


def table_preview(pipeline_name: str, table_name: str) -> dict[str, Any]:
    """Get the first row from the specified table."""
    pipeline = dlt.attach(pipeline_name)

    # TODO refactor try/except to specific line or at the tool manager level
    # the inconsistent errors are probably due to database locking
    try:
        return pipeline.dataset()[table_name].limit(1).df().to_dict()
    except Exception as e:
        raise ToolError(
            "Tool `table_preview()` failed. Verify the `pipeline_name` and the `table_name`. "
            "If the error persist, try starting a new conversation."
        ) from e


def table_schema(pipeline_name: str, table_name: str) -> dict[str, Any]:
    """Get the schema of the specified table."""
    from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient

    # TODO refactor try/except to specific line or at the tool manager level
    # the inconsistent errors are probably due to database locking
    try:
        pipeline = dlt.attach(pipeline_name)
        dataset = pipeline.dataset()

        # get schema and clone the table
        schema = dataset.schema
        table_schema: dict[str, Any] = schema.get_table(table_name)  # type: ignore[assignment]
        # add sql dialect which is destination type
        if isinstance(dataset.sql_client, DuckDbSqlClient):
            dialect = "duckdb"
        else:
            assert dataset._destination is not None
            dialect = dataset._destination.destination_type
        table_schema["sql_dialect"] = dialect
        # normalize names
        table_schema["normalized_name"] = dataset.sql_client.escape_column_name(
            schema.naming.normalize_tables_path(table_schema["name"])
        )
        for col_schema in table_schema["columns"].values():
            col_schema["normalized_name"] = dataset.sql_client.escape_column_name(
                schema.naming.normalize_tables_path(col_schema["name"])
            )
        stored_schema = schema.to_dict(remove_defaults=True, bump_version=False)
        return stored_schema["tables"][table_name]  # type: ignore[return-value]
    except Exception as e:
        raise ToolError(
            "Tool `table_schema()` failed. Verify the `pipeline_name` and the `table_name`. "
            "If the error persist, try starting a new conversation."
        ) from e


def execute_sql_query(pipeline_name: str, sql_select_query: str) -> str:
    """Executes SELECT SQL statement for simple data analysis.

    Use the `table_schema()` tool to get the available columns and use their fully qualified name in the SQL query.
    """
    pipeline = dlt.attach(pipeline_name)
    dataset = pipeline.dataset()
    result = dataset(sql_select_query).arrow()

    return helpers.format_csv(result)


__tools__ = (
    available_pipelines,
    available_tables,
    table_preview,
    table_schema,
    execute_sql_query,
)
