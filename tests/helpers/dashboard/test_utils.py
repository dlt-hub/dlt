import os
import tempfile
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pytest
import pandas as pd

from dlt.sources._single_file_templates.fruitshop_pipeline import (
    fruitshop as fruitshop_source,
)
import pendulum


import dlt
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.specs import known_sections
from dlt.common.destination.client import WithStateSync
from dlt.common.json import json
from dlt.common.pendulum import pendulum as dlt_pendulum
from dlt.common.pipeline import get_dlt_pipelines_dir
from dlt.common.schema import Schema
from dlt.common.storages import FileStorage
from dlt.helpers.dashboard.config import DashboardConfiguration
from dlt.helpers.dashboard import utils
from dlt.helpers.dashboard.utils import (
    PICKLE_TRACE_FILE,
    resolve_dashboard_config,
    get_local_pipelines,
    get_pipeline,
    pipeline_details,
    create_table_list,
    create_column_list,
    clear_query_cache,
    get_query_result,
    get_row_counts,
    get_loads,
    get_schema_by_version,
    trace_overview,
    trace_execution_context,
    trace_steps_overview,
    trace_resolved_config_values,
    trace_step_details,
    style_cell,
    _without_none_or_empty_string,
    _align_dict_keys,
    _humanize_datetime_values,
    _dict_to_table_items,
)


@pytest.fixture
def temp_pipelines_dir():
    """Create a temporary directory structure for testing pipelines"""
    with tempfile.TemporaryDirectory() as temp_dir:
        pipelines_dir = Path(temp_dir) / "pipelines"
        pipelines_dir.mkdir()

        # Create some test pipeline directories
        (pipelines_dir / "test_pipeline_1").mkdir()
        (pipelines_dir / "test_pipeline_2").mkdir()
        (pipelines_dir / "_dlt_internal").mkdir()

        # Create trace files with different timestamps
        trace_file_1 = pipelines_dir / "test_pipeline_1" / PICKLE_TRACE_FILE
        trace_file_1.touch()
        # Set modification time to 2 days ago
        os.utime(trace_file_1, (1000000, 1000000))

        trace_file_2 = pipelines_dir / "test_pipeline_2" / PICKLE_TRACE_FILE
        trace_file_2.touch()
        # Set modification time to 1 day ago (more recent)
        os.utime(trace_file_2, (2000000, 2000000))

        yield str(pipelines_dir)


@pytest.fixture(scope="session")
def test_pipeline():
    """Create a test pipeline with in memory duckdb destination in temp folder"""
    import duckdb

    with tempfile.TemporaryDirectory() as temp_dir:
        pipeline = dlt.pipeline(
            pipeline_name="test_pipeline",
            pipelines_dir=temp_dir,
            destination=dlt.destinations.duckdb(credentials=duckdb.connect(":memory:")),
            dataset_name="test_dataset",
            dev_mode=True,
        )

        pipeline.run(fruitshop_source())

        # we overwrite the purchases table to have a child table and an incomplete column
        @dlt.resource(primary_key="id", write_disposition="merge", columns={"incomplete": {}})
        def purchases():
            """Load purchases data from a simple python list."""
            yield [
                {"id": 1, "customer_id": 1, "inventory_id": 1, "quantity": 1, "child": [1, 2, 3]},
                {"id": 2, "customer_id": 1, "inventory_id": 2, "quantity": 2},
                {"id": 3, "customer_id": 2, "inventory_id": 3, "quantity": 3},
            ]

        pipeline.run(purchases())

        yield pipeline


def test_resolve_dashboard_config(test_pipeline):
    """Test resolving dashboard config with a real pipeline"""

    os.environ["DASHBOARD__TEST_PIPELINE__DATETIME_FORMAT"] = "some format"
    os.environ["DASHBOARD__DATETIME_FORMAT"] = "other format"

    config = resolve_dashboard_config(test_pipeline)

    assert isinstance(config, DashboardConfiguration)
    assert isinstance(config.datetime_format, str)
    assert config.datetime_format == "some format"

    other_pipeline = dlt.pipeline(pipeline_name="other_pipeline", destination="duckdb")
    config = resolve_dashboard_config(other_pipeline)
    assert config.datetime_format == "other format"


def test_get_local_pipelines_with_temp_dir(temp_pipelines_dir):
    """Test getting local pipelines with temporary directory"""
    pipelines_dir, pipelines = get_local_pipelines(temp_pipelines_dir)

    assert pipelines_dir == temp_pipelines_dir
    assert len(pipelines) == 3  # test_pipeline_1, test_pipeline_2, _dlt_internal

    # Should be sorted by timestamp (descending)
    pipeline_names = [p["name"] for p in pipelines]
    assert "test_pipeline_2" in pipeline_names
    assert "test_pipeline_1" in pipeline_names
    assert "_dlt_internal" in pipeline_names

    # Check timestamps are present
    for pipeline in pipelines:
        assert "timestamp" in pipeline
        assert isinstance(pipeline["timestamp"], (int, float))


def test_get_local_pipelines_empty_dir():
    """Test getting local pipelines from empty directory"""
    with tempfile.TemporaryDirectory() as temp_dir:
        pipelines_dir, pipelines = get_local_pipelines(temp_dir)

        assert pipelines_dir == temp_dir
        assert pipelines == []


def test_get_local_pipelines_nonexistent_dir():
    """Test getting local pipelines from nonexistent directory"""
    nonexistent_dir = "/nonexistent/directory"
    pipelines_dir, pipelines = get_local_pipelines(nonexistent_dir)

    assert pipelines_dir == nonexistent_dir
    assert pipelines == []


def test_get_pipeline(test_pipeline):
    """Test getting a real pipeline by name"""
    pipeline = get_pipeline(test_pipeline.pipeline_name, test_pipeline.pipelines_dir)

    assert pipeline.pipeline_name == test_pipeline.pipeline_name
    assert pipeline.dataset_name == test_pipeline.dataset_name


def test_pipeline_details(test_pipeline, temp_pipelines_dir):
    """Test getting pipeline details from a real pipeline"""
    config = DashboardConfiguration()
    result = pipeline_details(config, test_pipeline, temp_pipelines_dir)

    assert isinstance(result, list)
    assert len(result) == 8

    # Convert to dict for easier testing
    details_dict = {item["name"]: item["value"] for item in result}

    assert details_dict["pipeline_name"] == "test_pipeline"
    assert details_dict["destination"] == "duckdb (dlt.destinations.duckdb)"
    assert details_dict["dataset_name"] == test_pipeline.dataset_name
    assert details_dict["schemas"].startswith("fruitshop")
    assert details_dict["working_dir"].endswith(test_pipeline.pipeline_name)


def test_create_table_list(test_pipeline):
    """Test creating a basic table list with real schema"""
    config = DashboardConfiguration()

    result = create_table_list(
        config,
        test_pipeline,
        selected_schema_name=test_pipeline.default_schema_name,
        show_child_tables=False,
    )

    # Should exclude _dlt_loads by default
    table_names = {table["name"] for table in result}
    assert table_names == {"inventory", "purchases", "customers", "inventory_categories"}

    result = create_table_list(
        config,
        test_pipeline,
        selected_schema_name=test_pipeline.default_schema_name,
        show_child_tables=True,
    )
    table_names = {table["name"] for table in result}
    assert table_names == {
        "inventory",
        "purchases",
        "customers",
        "purchases__child",
        "inventory_categories",
    }

    result = create_table_list(
        config,
        test_pipeline,
        selected_schema_name=test_pipeline.default_schema_name,
        show_internals=True,
        show_child_tables=False,
    )
    table_names = {table["name"] for table in result}
    assert table_names == {
        "customers",
        "purchases",
        "_dlt_loads",
        "_dlt_pipeline_state",
        "inventory",
        "_dlt_version",
        "inventory_categories",
    }


def test_create_column_list_basic(test_pipeline):
    """Test creating a basic column list with real schema"""
    config = DashboardConfiguration()

    # Should exclude _dlt columns by default, will also not show incomplete columns
    result = create_column_list(
        config,
        test_pipeline,
        selected_schema_name=test_pipeline.default_schema_name,
        table_name="purchases",
    )
    column_names = {col["name"] for col in result}
    assert column_names == {"customer_id", "quantity", "id", "inventory_id", "date"}

    result = create_column_list(
        config,
        test_pipeline,
        selected_schema_name=test_pipeline.default_schema_name,
        table_name="purchases",
        show_internals=True,
    )
    column_names = {col["name"] for col in result}
    assert column_names == {
        "_dlt_load_id",
        "customer_id",
        "quantity",
        "id",
        "inventory_id",
        "_dlt_id",
        "date",
    }


def test_create_column_list_type_hints(test_pipeline):
    """Test creating column list with type hints"""
    config = DashboardConfiguration()
    result = create_column_list(
        config,
        test_pipeline,
        selected_schema_name=test_pipeline.default_schema_name,
        table_name="purchases",
        show_type_hints=True,
    )

    # Find the id column
    id_column = next(col for col in result if col["name"] == "id")
    assert id_column["data_type"] == "bigint"
    assert id_column["nullable"] is False


def test_get_query_result(test_pipeline):
    """Test getting query result from real pipeline"""
    # Clear cache first
    get_query_result.cache_clear()

    result = get_query_result(test_pipeline, "SELECT COUNT(*) as count FROM purchases")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result.iloc[0]["count"] == 100


def test_get_row_counts_real(test_pipeline):
    """Test getting row counts from real pipeline"""
    result = get_row_counts(test_pipeline)
    assert result == {
        "customers": 13,
        "inventory": 6,
        "purchases": 100,
        "purchases__child": 3,
        "inventory_categories": 3,
        "_dlt_version": 2,
        "_dlt_loads": 2,
        "_dlt_pipeline_state": 1,
    }


def test_get_loads(test_pipeline):
    """Test getting loads from real pipeline"""
    config = DashboardConfiguration()

    # Clear cache first
    get_loads.cache_clear()
    result = get_loads(config, test_pipeline, limit=100)

    assert isinstance(result, list)
    assert len(result) >= 1  # Should have at least one load

    if result:
        load = result[0]
        assert "load_id" in load


def test_trace(test_pipeline):
    """Test trace overview with real trace data"""
    config = DashboardConfiguration()
    trace = test_pipeline.last_trace

    # overview
    result = trace_overview(config, trace)
    assert {item["name"] for item in result} == {
        "pipeline_name",
        "started_at",
        "finished_at",
        "transaction_id",
        "duration",
    }
    values_dict = {item["name"]: item["value"] for item in result}
    assert values_dict["pipeline_name"] == "test_pipeline"

    # execution context
    result = trace_execution_context(config, trace)
    assert len(result) == 7
    assert {item["name"] for item in result} == {
        "cpu",
        "os",
        "library",
        "run_context",
        "python",
        "ci_run",
        "exec_info",
    }

    # steps overview
    result = trace_steps_overview(config, trace)

    assert len(result) == 3
    assert result[0]["step"] == "extract"
    assert result[1]["step"] == "normalize"
    assert result[2]["step"] == "load"

    # TODO: deeper test...


def test_style_cell():
    """Test style cell function"""
    # Even row
    result = style_cell("0", "test_col", "test_value")
    assert result["background-color"] == "white"

    # Odd row
    result = style_cell("1", "test_col", "test_value")
    assert result["background-color"] == "#f4f4f9"

    # Name column (case insensitive)
    result = style_cell("0", "name", "test_value")
    assert result["font-weight"] == "bold"

    result = style_cell("0", "NAME", "test_value")
    assert result["font-weight"] == "bold"


def test_without_none_or_empty_string():
    """Test removing None and empty string values"""
    input_dict = {
        "key1": "value1",
        "key2": None,
        "key3": "",
        "key4": "value4",
        "key5": 0,  # Should be kept
        "key6": False,  # Should be kept
        "key7": [],  # Should be kept
    }

    result = _without_none_or_empty_string(input_dict)

    expected = {"key1": "value1", "key4": "value4", "key5": 0, "key6": False, "key7": []}
    assert result == expected


def test_align_dict_keys():
    """Test aligning dictionary keys"""
    items = [
        {"key1": "value1", "key2": "value2"},
        {"key1": "value3", "key3": "value4"},
        {"key2": "value5", "key4": "value6"},
    ]

    result = _align_dict_keys(items)

    # All items should have all keys
    expected_keys = {"key1", "key2", "key3", "key4"}
    for item in result:
        assert set(item.keys()) == expected_keys

    # Missing keys should be filled with "-"
    assert result[0]["key3"] == "-"
    assert result[0]["key4"] == "-"
    assert result[1]["key2"] == "-"
    assert result[1]["key4"] == "-"
    assert result[2]["key1"] == "-"
    assert result[2]["key3"] == "-"


def test_align_dict_keys_with_none_values():
    """Test aligning dictionary keys with None values filtered out"""
    items = [
        {"key1": "value1", "key2": None, "key3": ""},
        {"key1": None, "key2": "value2", "key4": "value4"},
    ]

    result = _align_dict_keys(items)

    # None and empty string values should be removed before alignment
    assert "key3" not in result[0]  # Was empty string

    # Missing keys should be filled with "-"
    assert result[0]["key4"] == "-"
    assert result[0]["key2"] == "-"
    assert result[1]["key1"] == "-"


def test_humanize_datetime_values():
    """Test humanizing datetime values"""
    config = DashboardConfiguration()
    config.datetime_format = "YYYY-MM-DD HH:mm:ss Z"

    input_dict = {
        "started_at": pendulum.parse("2023-01-01T12:00:00"),
        "finished_at": pendulum.parse("2023-01-01T12:30:00"),
        "created": 1672574400,  # Unix timestamp
        "last_modified": "1672574400.123",  # String timestamp
        "inserted_at": datetime(2023, 1, 1, 12, 0, 0),
        "load_id": 1672574400,  # Unix timestamp for 2023-01-01T12:00:00
        "other_field": "unchanged",
        "numeric_field": 42,
    }

    result = _humanize_datetime_values(config, input_dict)

    # Should have duration calculated
    assert "duration" in result

    # Original non-datetime fields should be preserved
    assert result["other_field"] == "unchanged"
    assert result["numeric_field"] == 42

    assert result["created"] == "2023-01-01 12:00:00 +00:00"
    assert result["last_modified"] == "2023-01-01 12:00:00 +00:00"
    assert result["inserted_at"] == "2023-01-01 12:00:00 +00:00"
    assert result["load_package_created_at"] == "2023-01-01 12:00:00 +00:00"


def test_dict_to_table_items():
    """Test converting dict to table items"""
    input_dict = {"pipeline_name": "test_pipeline", "destination": "duckdb", "status": "completed"}

    result = _dict_to_table_items(input_dict)

    expected = [
        {"name": "pipeline_name", "value": "test_pipeline"},
        {"name": "destination", "value": "duckdb"},
        {"name": "status", "value": "completed"},
    ]

    # Sort both by name for comparison since dict order may vary
    result_sorted = sorted(result, key=lambda x: x["name"])
    expected_sorted = sorted(expected, key=lambda x: x["name"])

    assert result_sorted == expected_sorted


def test_integration_get_local_pipelines_with_sorting(temp_pipelines_dir):
    """Test integration scenario with multiple pipelines sorted by timestamp"""
    pipelines_dir, pipelines = get_local_pipelines(temp_pipelines_dir, sort_by_trace=True)

    assert pipelines_dir == temp_pipelines_dir
    assert len(pipelines) == 3

    # Should be sorted by timestamp (descending - most recent first)
    timestamps = [p["timestamp"] for p in pipelines]
    assert timestamps == sorted(timestamps, reverse=True)

    # Verify the most recent pipeline is first
    most_recent = pipelines[0]
    assert most_recent["name"] == "test_pipeline_2"
    assert most_recent["timestamp"] == 2000000


def test_integration_pipeline_workflow(test_pipeline, temp_pipelines_dir):
    """Test integration scenario with complete pipeline workflow"""
    # Test pipeline details
    config = DashboardConfiguration()

    details = pipeline_details(config, test_pipeline, temp_pipelines_dir)
    details_dict = {item["name"]: item["value"] for item in details}
    assert details_dict["pipeline_name"] == "test_pipeline"

    # Test row counts
    row_counts = get_row_counts(test_pipeline)
    assert row_counts["customers"] == 13

    # Test query execution
    query_result = get_query_result(test_pipeline, "SELECT name FROM customers ORDER BY id")
    assert len(query_result) == 13
    assert query_result.iloc[0]["name"] == "simon"

    # Test loads
    config = DashboardConfiguration()
    loads = get_loads(config, test_pipeline)
    assert len(loads) >= 1
