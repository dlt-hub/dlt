"""SQL query execution, row counts, and load history retrieval."""

import traceback
from typing import Any, Dict, List, Tuple

import marimo as mo

import dlt
import pyarrow
import sqlglot

from dlt.common.destination.exceptions import SqlClientNotAvailable
from dlt.destinations.exceptions import DatabaseUndefinedRelation, DestinationUndefinedEntity
from dlt.pipeline.exceptions import PipelineConfigMissing

from dlt._workspace.helpers.dashboard import strings
from dlt._workspace.helpers.dashboard.config import DashboardConfiguration
from dlt._workspace.helpers.dashboard.utils.formatters import (
    format_exception_message,
    humanize_datetime_values,
)
from dlt._workspace.helpers.dashboard.utils.schema import get_schema_by_version


def clear_query_cache() -> None:
    """Clear the cached query results."""
    _query_result_cache.clear()


def get_default_query_for_table(
    pipeline: dlt.Pipeline, schema_name: str, table_name: str, limit: bool
) -> Tuple[str, str, str]:
    """Build a default SELECT query for a table. Returns (sql_query, error_message, traceback)."""
    try:
        _dataset = pipeline.dataset(schema=schema_name)
        _sql_query = (
            _dataset.table(table_name)
            .limit(1000 if limit else None)
            .to_sql(pretty=True, _raw_query=True)
        )
        return _sql_query, None, None
    except Exception as exc:
        return "", format_exception_message(exc), traceback.format_exc()


def get_example_query_for_dataset(pipeline: dlt.Pipeline, schema_name: str) -> Tuple[str, str, str]:
    """Return an example query for the first data table in the schema.

    Returns (sql_query, error_message, traceback).
    """
    schema = pipeline.schemas.get(schema_name)
    if schema and (tables := schema.data_tables()):
        return get_default_query_for_table(pipeline, schema_name, tables[0]["name"], True)
    return "", "Schema does not contain any tables.", None


def get_query_result(pipeline: dlt.Pipeline, query: str) -> Tuple[pyarrow.Table, str, str]:
    """Get the result of a query.

    Parses the query to ensure it is valid SQL before sending it to the destination.
    """
    try:
        sqlglot.parse_one(
            query,
            dialect=pipeline.destination.capabilities().sqlglot_dialect,
        )
        return (
            _execute_query_cached(pipeline.pipeline_name, pipeline.dataset_name, query, pipeline),
            None,
            None,
        )
    except Exception as exc:
        return pyarrow.table({}), format_exception_message(exc), traceback.format_exc()


def _execute_query_cached(
    pipeline_name: str, dataset_name: str, query: str, pipeline: dlt.Pipeline
) -> pyarrow.Table:
    """Execute a query, returning cached result if the same pipeline/query was seen before.

    The cache is keyed on (pipeline_name, dataset_name, query) strings so that
    results survive pipeline re-attachment across refreshes.  The pipeline object
    is passed through for execution but is *not* part of the cache key.
    """
    cache_key = (pipeline_name, dataset_name, query)
    if cache_key in _query_result_cache:
        return _query_result_cache[cache_key]
    result = pipeline.dataset()(query, _execute_raw_query=True).arrow()
    # evict oldest entry when cache is full
    if len(_query_result_cache) >= _QUERY_CACHE_MAX_SIZE:
        _query_result_cache.pop(next(iter(_query_result_cache)))
    _query_result_cache[cache_key] = result
    return result


_QUERY_CACHE_MAX_SIZE = 64
_query_result_cache: Dict[Tuple[str, str, str], pyarrow.Table] = {}


def get_row_counts(
    pipeline: dlt.Pipeline, selected_schema_name: str = None, load_id: str = None
) -> Dict[str, Any]:
    """Get the row counts for a pipeline as a dict of {table_name: count}."""
    row_counts = {}
    try:
        row_counts = {
            i["table_name"]: i["row_count"]
            for i in pipeline.dataset(schema=selected_schema_name)
            .row_counts(dlt_tables=True, load_id=load_id)
            .arrow()
            .to_pylist()
        }
    except (
        DatabaseUndefinedRelation,
        DestinationUndefinedEntity,
        SqlClientNotAvailable,
        PipelineConfigMissing,
        ConnectionError,
    ):
        # TODO: somehow propagate errors to the user here
        pass

    return row_counts


def get_row_counts_list(
    pipeline: dlt.Pipeline, selected_schema_name: str = None, load_id: str = None
) -> List[Dict[str, Any]]:
    """Get the row counts for a pipeline as a list of {name, row_count} dicts."""
    row_counts_dict = get_row_counts(pipeline, selected_schema_name, load_id)
    row_counts = [{"name": k, "row_count": v} for k, v in row_counts_dict.items()]
    row_counts.sort(key=lambda x: str(x["name"]))
    return row_counts


def get_loads(
    c: DashboardConfiguration, pipeline: dlt.Pipeline, limit: int = 100
) -> Tuple[Any, str, str]:
    """Get the loads of a pipeline. Returns (loads_list, error_message, traceback)."""
    try:
        loads = (
            pipeline.dataset()
            ._dlt_loads.filter("schema_name", "in", pipeline.schema_names)
            .order_by("inserted_at", "desc")
        )
        if limit:
            loads = loads.limit(limit)

        loads_list = loads.arrow().to_pylist()
        loads_list = [humanize_datetime_values(c, load) for load in loads_list]
        return loads_list, None, None
    except Exception as exc:
        return [], format_exception_message(exc), traceback.format_exc()


def build_load_details(
    pipeline: dlt.Pipeline,
    schema_name: str,
    version_hash: str,
    load_id: str,
) -> List[Any]:
    """Build the load detail widgets: row counts and schema version accordion."""
    from dlt._workspace.helpers.dashboard.utils import ui

    result: List[Any] = []

    with mo.status.spinner(title=strings.loads_details_loading_spinner_text):
        _schema = get_schema_by_version(pipeline, version_hash)
        _row_counts = get_row_counts_list(pipeline, schema_name, load_id)

    result.append(
        ui.title_and_subtitle(
            strings.loads_details_row_counts_title,
            strings.loads_details_row_counts_subtitle,
            3,
        ),
    )
    result.append(ui.dlt_table(_row_counts))

    if _schema:
        result.append(
            ui.title_and_subtitle(
                strings.loads_details_schema_version_title,
                strings.loads_details_schema_version_subtitle.format(
                    (
                        "is not"
                        if _schema.version_hash != pipeline.default_schema.version_hash
                        else "is"
                    ),
                ),
                3,
            )
        )
        result.append(
            mo.accordion(
                {
                    strings.schema_show_raw_yaml_text: mo.ui.code_editor(
                        _schema.to_pretty_yaml(),
                        language="yaml",
                    )
                }
            )
        )

    return result
