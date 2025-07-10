from typing import Any, List, Dict, Union
from contextlib import nullcontext as does_not_raise
import pytest

from dlt.pipeline import helpers, Pipeline
from dlt.pipeline.drop import DISQUALIFYING_HINTS
from dlt.common.schema.utils import (
    is_nullable_column,
    group_tables_by_resource,
    compile_simple_regexes,
)
from dlt.common.schema.typing import TSimpleRegex, DLT_NAME_PREFIX
from dlt.destinations.job_client_impl import SqlJobClientBase

from tests.load.pipeline.test_drop import droppable_source
from tests.load.utils import destinations_configs, DestinationTestConfiguration
from tests.pipeline.utils import assert_load_info, load_table_counts
from dlt.pipeline.drop import _FromTableDropCols


def assert_dropped_columns(pipeline: Pipeline, must_drop_infos: List[_FromTableDropCols]):
    # Verify requested columns are dropped from destination
    client: SqlJobClientBase
    with pipeline.destination_client(pipeline.default_schema_name) as client:  # type: ignore
        assert client.is_storage_initialized()
        for must_drop_info in must_drop_infos:
            from_table = must_drop_info["from_table"]
            dropped_columns = must_drop_info["drop_columns"]

            physical_columns: Union[Any] = None

            # Check if this is a filesystem client with open table support
            if hasattr(client, "is_open_table") and hasattr(client, "load_open_table"):
                if client.is_open_table("iceberg", from_table):
                    ice_tbl = client.load_open_table("iceberg", from_table)
                    physical_columns = [field.name for field in ice_tbl.schema().fields]
                elif client.is_open_table("delta", from_table):
                    delta_tbl = client.load_open_table("delta", from_table)
                    physical_columns = [f.name for f in delta_tbl.schema().fields]
                else:
                    # For regular filesystem tables, verify by reading actual data files
                    if hasattr(client, "list_table_files"):
                        table_files = client.list_table_files(from_table)
                        physical_columns = _get_columns_from_data_files(client, table_files)
            else:
                # Fallback: information schema coming from destination
                col_schema = list(client.get_storage_tables([from_table]))[0][1]
                physical_columns = (
                    list(col_schema.keys()) if isinstance(col_schema, dict) else list(col_schema)
                )

            assert all(dropped_col not in physical_columns for dropped_col in dropped_columns)


def _get_columns_from_data_files(client: Any, table_files: List[str]) -> List[str]:
    """Get actual columns from data files by reading the first file."""
    import os

    if not table_files:
        return []

    # Use the first file to get column names
    first_file = table_files[0]
    file_ext = os.path.splitext(first_file)[1].lower()

    if file_ext == ".parquet":
        from dlt.common.libs.pyarrow import pyarrow as pa

        table = pa.parquet.read_table(client.make_remote_url(first_file))
        return list(table.column_names)

    elif file_ext == ".jsonl":
        content = client.fs_client.read_text(first_file, encoding="utf-8")
        lines = content.strip().split("\n")
        if lines and lines[0].strip():
            import json

            record = json.loads(lines[0])
            return list(record.keys())

    elif file_ext == ".csv":
        from dlt.common.libs.pyarrow import pyarrow as pa

        table = pa.csv.read_csv(client.make_remote_url(first_file))
        return list(table.column_names)

    return []


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        all_buckets_filesystem_configs=True,
        table_format_filesystem_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_drop_column_command_resources(destination_config: DestinationTestConfiguration) -> None:
    """Test the drop command with resource and verify correct data is deleted from destination"""
    source: Any = droppable_source()

    pipeline = destination_config.setup_pipeline(
        "test_drop_column_command_resources", dev_mode=True
    )
    info = pipeline.run(source, **destination_config.run_kwargs)
    assert_load_info(info)

    all_tables = list(pipeline.default_schema.tables.keys())
    assert load_table_counts(pipeline, *all_tables) == {
        "_dlt_version": 1,
        "_dlt_loads": 1,
        "droppable_a": 2,
        "droppable_b": 1,
        "droppable_c": 1,
        "droppable_d": 2,
        "droppable_no_state": 3,
        "_dlt_pipeline_state": 1,
        "droppable_b__items": 2,
        "droppable_c__items": 1,
        "droppable_c__items__labels": 2,
    }

    from_resources = ["droppable_a", "droppable_b", "droppable_c", "droppable_d"]

    # This should drop all droppable columns
    # If the table's droppable columns leave only dlt tables, nothing will be dropped
    drop_cmd = helpers.DropCommand(
        pipeline,
        from_resources=from_resources,
    )

    must_drop_infos = drop_cmd.from_tables_drop_cols

    assert (
        "After dropping matched droppable columns ['name', 'value'] from table"
        " 'droppable_c__items__labels' only internal dlt columns will remain. This is not allowed."
        in drop_cmd.info["warnings"]
    )
    assert (
        "After dropping matched droppable columns ['k', 'r'] from table 'droppable_c__items' only"
        " internal dlt columns will remain. This is not allowed."
        in drop_cmd.info["warnings"]
    )
    assert (
        "After dropping matched droppable columns ['m', 'n'] from table 'droppable_b__items' only"
        " internal dlt columns will remain. This is not allowed."
        in drop_cmd.info["warnings"]
    )

    drop_cmd()

    for must_drop_info in must_drop_infos:
        remaining_cols = pipeline.default_schema.get_table_columns(must_drop_info["from_table"])
        for dropped_col in must_drop_info["drop_columns"]:
            assert dropped_col not in remaining_cols

    assert_dropped_columns(pipeline, must_drop_infos)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        all_buckets_filesystem_configs=True,
        table_format_local_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_drop_column_from_child_table(destination_config: DestinationTestConfiguration) -> None:
    """Test the drop command with column and child table and verify correct data is deleted from destination"""
    source: Any = droppable_source()

    pipeline = destination_config.setup_pipeline("test_drop_column_from_child_table", dev_mode=True)
    info = pipeline.run(source, **destination_config.run_kwargs)
    assert_load_info(info)

    original_parent_tbl_cols = pipeline.default_schema.get_table_columns("droppable_b")
    assert "m" in pipeline.default_schema.get_table_columns("droppable_b__items")

    drop_cmd = helpers.DropCommand(
        pipeline, from_resources=["droppable_b"], from_tables=["droppable_b__items"], columns=["m"]
    )
    must_drop_infos = drop_cmd.from_tables_drop_cols

    assert [{"from_table": "droppable_b__items", "drop_columns": ["m"]}] == must_drop_infos

    drop_cmd()

    assert "m" not in pipeline.default_schema.get_table_columns("droppable_b__items")
    assert original_parent_tbl_cols == pipeline.default_schema.get_table_columns("droppable_b")
    assert_dropped_columns(pipeline, must_drop_infos)
