"""
Trace analysis utilities for PipelineRunner callbacks.

This module provides helper functions to analyze DLT pipeline traces and extract
meaningful insights about schema changes, row counts, and pipeline execution.
"""

from typing import Dict, Any

from dlt.pipeline.trace import PipelineTrace
from dlt.common import logger


def _has_schema_changes(trace: PipelineTrace) -> bool:
    """
    Check if the trace has schema changes.
    """
    return any(step.step_info.schema_update for step in trace.steps)


def _updates_are_different(column1: Dict[str, Any], column2: Dict[str, Any]) -> bool:
    """
    Compare two column definitions to check if they are different.

    Args:
        column1: First column definition
        column2: Second column definition

    Returns:
        True if columns are different, False if they are the same
    """
    # Fast path: if key sets are different, columns are different
    if column1.keys() != column2.keys():
        return True

    # Keys are identical, so compare values using any() for early exit
    return any(column1[key] != column2[key] for key in column1)


def _get_column_update_without_dlt_columns(
    update: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """
    Return a clone of the update dict with DLT internal columns removed.
    """
    new_update = {}
    for column_name, column_update in update.items():
        if column_name.startswith("_dlt_"):
            continue
        new_update[column_name] = column_update
    return new_update


def _add_updates_from_load_package_into_schema_changes(
    package: Dict[str, Any],
    schema_changes: Dict[str, Dict[str, Any]],
    include_dlt_tables: bool,
    include_dlt_columns: bool,
) -> None:
    """Process a single package's schema updates and merge them into schema_changes."""
    if not hasattr(package, "schema_update") or not package.schema_update:
        return

    for table_name, table_update in package.schema_update.items():
        if not include_dlt_tables and table_name.startswith("_dlt_"):
            continue

        # Process schema updates for this table
        if table_name in schema_changes:
            # Merge updates into existing table
            for column_name, column_update in table_update["columns"].items():
                if not include_dlt_columns and column_name.startswith("_dlt_"):
                    continue

                existing_columns = schema_changes[table_name]["columns"]
                if earlier_update := existing_columns.get(column_name):
                    if _updates_are_different(earlier_update, column_update):
                        logger.warning(f"""
Column {column_name} of table {table_name} has received multiple schema changes.
Only the final one will be returned here.
Previous update: {earlier_update}
Latest update: {column_update}""")

                existing_columns[column_name] = column_update
        else:
            # Add new table
            update = table_update["columns"]
            if not include_dlt_columns:
                update = _get_column_update_without_dlt_columns(update)
            schema_changes[table_name] = {"columns": update}


def get_schema_changes(
    trace: PipelineTrace, include_dlt_tables: bool = False, include_dlt_columns: bool = True
) -> Dict[str, Dict[str, Any]]:
    """
    Analyze trace data to identify schema changes to tables and columns from all load packages.
    todo: say what the change was (add, remove, modify)
    todo: notify about schema_evolution
    todo: say what tables have been created

    Args:
        trace: The PipelineTrace object from pipeline execution

    Returns:
        Dict mapping table names to lists of changed/added columns
    """
    schema_changes: Dict[str, Dict[str, Any]] = {}

    # Look at all load steps in the trace
    for step in getattr(trace, "steps", []):
        step_info = getattr(step, "step_info", None)
        if not step_info or not hasattr(step_info, "load_packages"):
            continue

        for package in step_info.load_packages:
            _add_updates_from_load_package_into_schema_changes(
                package, schema_changes, include_dlt_tables, include_dlt_columns
            )

    return schema_changes


def rows_processed_per_table(trace: PipelineTrace) -> Dict[str, int]:
    """
    Extract row counts per table from trace data.

    Args:
        trace: The PipelineTrace object from pipeline execution

    Returns:
        Dict mapping table names to row counts processed in this run
        (excluding internal tables and child tables)

    """
    rows_per_table: Dict[str, int] = {}
    if trace is None:
        return rows_per_table
    # Get the last extract info from the trace
    extract_info = trace.last_extract_info
    if not extract_info or not hasattr(extract_info, "metrics"):
        return rows_per_table
    # Aggregate table metrics from all metrics dicts
    metrics = extract_info.metrics
    for metrics_list in metrics.values():
        for metric_dict in metrics_list:
            table_metrics = metric_dict.get("table_metrics", {})
            for table_name, metric in table_metrics.items():
                if table_name and not table_name.startswith("_dlt_"):
                    rows_per_table[table_name] = getattr(metric, "items_count", 0)
    return rows_per_table


# def extract_environment_info(trace: PipelineTrace) -> Dict[str, Any]:
#     """
#     Extract environment and pipeline information from trace.

#     Args:
#         trace: The PipelineTrace object from pipeline execution

#     Returns:
#         Dict containing environment info like destination, dataset, timing, etc.
#     """
#     env_info = {
#         "pipeline_name": None,
#         "started_at": None,
#         "finished_at": None,
#         "execution_context": None,
#         "destination": None,
#         "dataset": None,
#         "load_id": None,
#         "duration_seconds": None,
#     }
#     if trace is None:
#         return env_info
#     env_info["pipeline_name"] = trace.pipeline_name
#     env_info["started_at"] = trace.started_at
#     env_info["finished_at"] = trace.finished_at
#     env_info["execution_context"] = trace.execution_context
#     # Calculate duration
#     if env_info["started_at"] and env_info["finished_at"]:
#         start = env_info["started_at"]
#         end = env_info["finished_at"]
#         if hasattr(start, 'timestamp') and hasattr(end, 'timestamp'):
#             env_info["duration_seconds"] = end.timestamp() - start.timestamp()
#     # Extract destination and dataset info from load info
#     load_info = trace.last_load_info
#     if load_info:
#         env_info["destination"] = load_info.destination_name
#         env_info["dataset"] = load_info.dataset_name
#         # Get load_id from load info
#         if load_info.loads_ids:
#             env_info["load_id"] = load_info.loads_ids[0]
#     return env_info


# def get_table_names(trace: PipelineTrace) -> Set[str]:
#     """
#     Extract all table names that were processed in this pipeline run.

#     Args:
#         trace: The PipelineTrace object from pipeline execution

#     Returns:
#         Set of table names processed (excluding DLT internal tables)
#     """
#     table_names = set()
#     if trace is None:
#         return table_names
#     # Get the last extract info from the trace
#     extract_info = trace.last_extract_info
#     if not extract_info or not hasattr(extract_info, "metrics"):
#         return table_names
#     metrics = extract_info.metrics
#     for metrics_list in metrics.values():
#         for metric_dict in metrics_list:
#             table_metrics = metric_dict.get("table_metrics", {})
#             for table_name in table_metrics.keys():
#                 if table_name and not table_name.startswith("_dlt_"):
#                     table_names.add(table_name)
#     return table_names


# def format_trace_summary(trace: PipelineTrace) -> str:
#     """
#     Create a human-readable summary of the trace data.

#     Args:
#         trace: The PipelineTrace object from pipeline execution

#     Returns:
#         Formatted string summary of the pipeline run
#     """
#     env_info = extract_environment_info(trace)
#     rows_per_table = summarize_rows_per_table(trace)
#     schema_changes = summarize_schema_changes(trace)
#     summary_lines = [
#         f"Pipeline: {env_info['pipeline_name']}",
#         f"Destination: {env_info['destination']}",
#         f"Dataset: {env_info['dataset']}",
#         f"Load ID: {env_info['load_id']}",
#         f"Duration: {env_info['duration_seconds']:.2f}s" if env_info['duration_seconds'] else "",
#         "",
#         "Tables processed:",
#     ]
#     for table_name, row_count in rows_per_table.items():
#         summary_lines.append(f"  - {table_name}: {row_count} rows")
#     if schema_changes:
#         summary_lines.extend(["", "Schema changes:"])
#         for table_name, new_columns in schema_changes.items():
#             summary_lines.append(f"  - {table_name}: added columns {new_columns}")
#     return "\n".join(summary_lines)
