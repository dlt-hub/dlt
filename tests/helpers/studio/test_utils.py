import os
import tempfile
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any, List
from datetime import datetime

import dlt
from dlt.common.schema import Schema
from dlt.common.storages import FileStorage
from dlt.common.pendulum import pendulum
from dlt.helpers.studio.config import StudioConfiguration
from dlt.helpers.studio import utils
from dlt.sources._single_file_templates.fruitshop_pipeline import fruitshop as fruitshop_source


pytest.skip(allow_module_level=True)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test pipelines"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Cleanup
    import shutil

    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def pipelines_dir(temp_dir):
    """Create pipelines directory"""
    pipelines_dir = os.path.join(temp_dir, "pipelines")
    os.makedirs(pipelines_dir, exist_ok=True)
    return pipelines_dir


@pytest.fixture
def simple_pipeline(pipelines_dir):
    """Create a simple test pipeline"""
    pipeline = dlt.pipeline(
        pipeline_name="test_simple", destination="duckdb", pipelines_dir=pipelines_dir
    )
    pipeline.run([1, 2, 3], table_name="numbers")
    return pipeline


@pytest.fixture
def fruit_pipeline(pipelines_dir):
    """Create a fruit shop test pipeline"""
    pipeline = dlt.pipeline(
        pipeline_name="test_fruit", destination="duckdb", pipelines_dir=pipelines_dir
    )
    pipeline.run(fruitshop_source())
    return pipeline


@pytest.fixture
def never_run_pipeline(pipelines_dir):
    """Create a pipeline that was never run"""
    return dlt.pipeline(
        pipeline_name="test_never_run", destination="duckdb", pipelines_dir=pipelines_dir
    )


@pytest.fixture
def no_dest_pipeline(pipelines_dir):
    """Create a pipeline with no destination"""
    pipeline = dlt.pipeline(pipeline_name="test_no_dest", pipelines_dir=pipelines_dir)
    pipeline.extract(fruitshop_source())
    return pipeline


@pytest.fixture
def studio_config():
    """Create a StudioConfiguration instance"""
    return StudioConfiguration()


def test_resolve_studio_config(simple_pipeline):
    """Test resolve_studio_config function"""
    # Test with valid pipeline
    config = utils.resolve_studio_config(simple_pipeline)
    assert isinstance(config, StudioConfiguration)
    assert hasattr(config, "table_list_fields")
    assert hasattr(config, "column_type_hints")
    assert hasattr(config, "datetime_format")

    # Test with None pipeline
    config_none = utils.resolve_studio_config(None)
    assert isinstance(config_none, StudioConfiguration)


@pytest.mark.parametrize("sort_by_trace", [True, False])
def test_get_local_pipelines(
    pipelines_dir,
    simple_pipeline,
    fruit_pipeline,
    never_run_pipeline,
    no_dest_pipeline,
    sort_by_trace,
):
    """Test get_local_pipelines function with different sorting options"""
    pipelines_dir_result, pipelines = utils.get_local_pipelines(
        pipelines_dir, sort_by_trace=sort_by_trace
    )

    assert pipelines_dir_result == pipelines_dir
    assert isinstance(pipelines, list)
    assert len(pipelines) >= 4  # At least our test pipelines

    # Check structure of pipeline entries
    for pipeline in pipelines:
        assert isinstance(pipeline, dict)
        assert "name" in pipeline
        assert "timestamp" in pipeline
        assert isinstance(pipeline["timestamp"], (int, float))

    # Check sorting
    if sort_by_trace and len(pipelines) > 1:
        timestamps = [p["timestamp"] for p in pipelines]
        assert timestamps == sorted(timestamps, reverse=True)


def test_get_local_pipelines_nonexistent_dir():
    """Test get_local_pipelines with non-existent directory"""
    nonexistent_dir = "/path/that/does/not/exist"
    pipelines_dir, pipelines = utils.get_local_pipelines(nonexistent_dir)

    assert pipelines_dir == nonexistent_dir
    assert isinstance(pipelines, list)
    assert len(pipelines) == 0


def test_get_local_pipelines_default_dir(pipelines_dir):
    """Test get_local_pipelines with default directory"""
    with patch("dlt.common.pipeline.get_dlt_pipelines_dir") as mock_get_dir:
        mock_get_dir.return_value = pipelines_dir
        pipelines_dir_result, pipelines = utils.get_local_pipelines()
        assert pipelines_dir_result == pipelines_dir


def test_get_pipeline(pipelines_dir, simple_pipeline):
    """Test get_pipeline function"""
    pipeline = utils.get_pipeline("test_simple", pipelines_dir)
    assert isinstance(pipeline, dlt.Pipeline)
    assert pipeline.pipeline_name == "test_simple"


def test_pipeline_details(simple_pipeline):
    """Test pipeline_details function"""
    details = utils.pipeline_details(simple_pipeline)

    assert isinstance(details, list)
    assert len(details) > 0

    # Convert to dict for easier testing
    details_dict = {item["name"]: item["value"] for item in details}

    assert "pipeline_name" in details_dict
    assert details_dict["pipeline_name"] == "test_simple"
    assert "destination" in details_dict
    assert "dataset_name" in details_dict
    assert "working_dir" in details_dict


def test_pipeline_details_no_destination(no_dest_pipeline):
    """Test pipeline_details with pipeline that has no destination"""
    details = utils.pipeline_details(no_dest_pipeline)
    details_dict = {item["name"]: item["value"] for item in details}

    assert "destination" in details_dict
    assert details_dict["destination"] == "No destination set"


@pytest.mark.parametrize("show_internals", [True, False])
@pytest.mark.parametrize("show_child_tables", [True, False])
@pytest.mark.parametrize("show_row_counts", [True, False])
def test_create_table_list(
    studio_config, simple_pipeline, show_internals, show_child_tables, show_row_counts
):
    """Test create_table_list function with different parameters"""
    with patch.object(utils, "get_row_counts") as mock_get_row_counts:
        mock_get_row_counts.return_value = {"numbers": 3, "_dlt_loads": 1}

        table_list = utils.create_table_list(
            studio_config,
            simple_pipeline,
            show_internals=show_internals,
            show_child_tables=show_child_tables,
            show_row_counts=show_row_counts,
        )

        assert isinstance(table_list, list)
        assert len(table_list) > 0

        # Check structure
        for table in table_list:
            assert isinstance(table, dict)
            assert "name" in table

            if show_row_counts:
                assert "row_count" in table

        # Check internal tables filtering
        table_names = [t["name"] for t in table_list]
        if show_internals:
            # Should include _dlt tables
            assert any(name.startswith("_dlt") for name in table_names)
        else:
            # Should not include _dlt tables
            assert not any(name.startswith("_dlt") for name in table_names)


@pytest.mark.parametrize("show_internals", [True, False])
@pytest.mark.parametrize("show_type_hints", [True, False])
@pytest.mark.parametrize("show_other_hints", [True, False])
@pytest.mark.parametrize("show_custom_hints", [True, False])
def test_create_column_list(
    studio_config,
    fruit_pipeline,
    show_internals,
    show_type_hints,
    show_other_hints,
    show_custom_hints,
):
    """Test create_column_list function with different parameters"""
    # Use fruit pipeline as it has more complex schema
    table_name = "customers"

    column_list = utils.create_column_list(
        studio_config,
        fruit_pipeline,
        table_name,
        show_internals=show_internals,
        show_type_hints=show_type_hints,
        show_other_hints=show_other_hints,
        show_custom_hints=show_custom_hints,
    )

    assert isinstance(column_list, list)
    assert len(column_list) > 0

    # Check structure
    for column in column_list:
        assert isinstance(column, dict)
        assert "name" in column

        if show_type_hints:
            # Should have type hint fields
            for hint in studio_config.column_type_hints:
                assert hint in column

        if show_other_hints:
            # Should have other hint fields
            for hint in studio_config.column_other_hints:
                assert hint in column

    # Check internal columns filtering
    column_names = [c["name"] for c in column_list]
    if show_internals:
        # Should include _dlt columns
        assert any(name.startswith("_dlt") for name in column_names)
    else:
        # Should not include _dlt columns
        assert not any(name.startswith("_dlt") for name in column_names)


def test_create_column_list_invalid_table(studio_config, simple_pipeline):
    """Test create_column_list with invalid table name"""
    with pytest.raises(KeyError):
        utils.create_column_list(studio_config, simple_pipeline, "nonexistent_table")


def test_clear_query_cache(simple_pipeline):
    """Test clear_query_cache function"""
    # This function clears caches, so we just test it doesn't raise
    utils.clear_query_cache(simple_pipeline)

    # Verify caches are cleared by checking they're empty
    assert (
        not hasattr(utils.get_query_result, "cache_info")
        or utils.get_query_result.cache_info().currsize == 0
    )


def test_get_query_result(simple_pipeline):
    """Test get_query_result function"""
    query = "SELECT COUNT(*) as count FROM numbers"

    result = utils.get_query_result(simple_pipeline, query)

    assert isinstance(result, pd.DataFrame)
    assert len(result) > 0
    assert "count" in result.columns


def test_get_query_result_invalid_query(simple_pipeline):
    """Test get_query_result with invalid SQL"""
    with pytest.raises(Exception):
        utils.get_query_result(simple_pipeline, "INVALID SQL QUERY")


def test_get_row_counts(simple_pipeline):
    """Test get_row_counts function"""
    row_counts = utils.get_row_counts(simple_pipeline)

    assert isinstance(row_counts, dict)
    assert "numbers" in row_counts
    assert isinstance(row_counts["numbers"], int)
    assert row_counts["numbers"] >= 0


def test_get_row_counts_with_load_id(simple_pipeline):
    """Test get_row_counts with specific load_id"""
    # Get a load_id from the pipeline
    loads = simple_pipeline.dataset()._dlt_loads.df()
    if len(loads) > 0:
        load_id = loads.iloc[0]["load_id"]
        row_counts = utils.get_row_counts(simple_pipeline, load_id)
        assert isinstance(row_counts, dict)


def test_get_loads(studio_config, simple_pipeline):
    """Test get_loads function"""
    loads = utils.get_loads(studio_config, simple_pipeline)

    assert isinstance(loads, list)
    assert len(loads) > 0

    # Check structure of load entries
    for load in loads:
        assert isinstance(load, dict)
        # Should have humanized datetime fields
        if "inserted_at" in load:
            assert isinstance(load["inserted_at"], str)


@pytest.mark.parametrize("limit", [None, 10, 100])
def test_get_loads_with_limit(studio_config, simple_pipeline, limit):
    """Test get_loads function with different limits"""
    loads = utils.get_loads(studio_config, simple_pipeline, limit=limit)

    assert isinstance(loads, list)
    if limit is not None:
        assert len(loads) <= limit


def test_get_schema_by_version(simple_pipeline):
    """Test get_schema_by_version function"""
    # Get current schema version hash
    version_hash = simple_pipeline.default_schema.version_hash

    schema = utils.get_schema_by_version(simple_pipeline, version_hash)

    if schema is not None:  # May be None if destination doesn't support state sync
        assert isinstance(schema, Schema)
        assert schema.version_hash == version_hash


def test_get_schema_by_version_invalid_hash(simple_pipeline):
    """Test get_schema_by_version with invalid hash"""
    schema = utils.get_schema_by_version(simple_pipeline, "invalid_hash")
    assert schema is None


def test_trace_overview(studio_config, fruit_pipeline):
    """Test trace_overview function"""
    # Create mock trace data
    overview = utils.trace_overview(studio_config, fruit_pipeline.last_trace.asdict())

    assert isinstance(overview, list)
    assert len(overview) == 5  # Only the 4 expected fields

    # Convert to dict for easier testing
    overview_dict = {item["name"]: item["value"] for item in overview}
    assert "transaction_id" in overview_dict
    assert "pipeline_name" in overview_dict
    assert "started_at" in overview_dict
    assert "finished_at" in overview_dict
    assert "extra_field" not in overview_dict


def test_trace_execution_context(studio_config):
    """Test trace_execution_context function"""
    trace_data = {"execution_context": {"user": "test_user", "environment": "test"}}

    context = utils.trace_execution_context(studio_config, trace_data)

    assert isinstance(context, list)
    assert len(context) == 2

    context_dict = {item["name"]: item["value"] for item in context}
    assert "user" in context_dict
    assert "environment" in context_dict


def test_trace_execution_context_empty(studio_config):
    """Test trace_execution_context with empty context"""
    trace_data = {}

    context = utils.trace_execution_context(studio_config, trace_data)

    assert isinstance(context, list)
    assert len(context) == 0


def test_trace_steps_overview(studio_config):
    """Test trace_steps_overview function"""
    trace_data = {
        "steps": [
            {
                "step": "extract",
                "started_at": "2024-01-01T10:00:00Z",
                "finished_at": "2024-01-01T10:02:00Z",
                "extra_field": "should_be_ignored",
            },
            {
                "step": "normalize",
                "started_at": "2024-01-01T10:02:00Z",
                "finished_at": "2024-01-01T10:03:00Z",
            },
        ]
    }

    steps = utils.trace_steps_overview(studio_config, trace_data)

    assert isinstance(steps, list)
    assert len(steps) == 2

    # Check first step
    step1 = steps[0]
    assert "step" in step1
    assert step1["step"] == "extract"
    assert "started_at" in step1
    assert "finished_at" in step1
    assert "duration" in step1  # Should be added by humanize function
    assert "extra_field" not in step1


def test_trace_resolved_config_values(studio_config):
    """Test trace_resolved_config_values function"""
    # Mock config value objects
    mock_config_value = Mock()
    mock_config_value.asdict.return_value = {"key": "value", "section": "test"}

    trace_data = {"resolved_config_values": [mock_config_value]}

    config_values = utils.trace_resolved_config_values(studio_config, trace_data)

    assert isinstance(config_values, list)
    assert len(config_values) == 1
    assert config_values[0] == {"key": "value", "section": "test"}


def test_trace_step_details(studio_config):
    """Test trace_step_details function"""
    trace_data = {
        "steps": [
            {
                "step": "load",
                "load_info": {
                    "table_metrics": [{"table_name": "test_table", "rows": 100}],
                    "job_metrics": [{"table_name": "test_table", "jobs": 1}],
                },
            }
        ]
    }

    details = utils.trace_step_details(studio_config, trace_data, "load")

    assert isinstance(details, list)
    # Should contain title elements and table elements
    assert len(details) > 0


def test_trace_step_details_no_step(studio_config):
    """Test trace_step_details with non-existent step"""
    trace_data = {"steps": []}

    details = utils.trace_step_details(studio_config, trace_data, "nonexistent")

    assert isinstance(details, list)
    assert len(details) == 0


@pytest.mark.parametrize(
    "row_id,name,expected_bg",
    [
        ("0", "name", "white"),
        ("1", "name", "#f4f4f9"),
        ("2", "other", "white"),
        ("3", "other", "#f4f4f9"),
    ],
)
def test_style_cell(row_id, name, expected_bg):
    """Test style_cell function"""
    style = utils.style_cell(row_id, name, "dummy_value")

    assert isinstance(style, dict)
    assert "background-color" in style
    assert style["background-color"] == expected_bg

    if name.lower() == "name":
        assert "font-weight" in style
        assert style["font-weight"] == "bold"


def test_without_none_or_empty_string():
    """Test _without_none_or_empty_string function"""
    input_dict = {
        "valid": "value",
        "none_value": None,
        "empty_string": "",
        "zero": 0,
        "false": False,
    }

    result = utils._without_none_or_empty_string(input_dict)

    assert "valid" in result
    assert "zero" in result
    assert "false" in result
    assert "none_value" not in result
    assert "empty_string" not in result


def test_align_dict_keys():
    """Test _align_dict_keys function"""
    input_list = [{"a": 1, "b": 2}, {"a": 3, "c": 4}, {"b": 5, "d": 6}]

    result = utils._align_dict_keys(input_list)

    assert len(result) == 3

    # All dicts should have the same keys
    all_keys = {"a", "b", "c", "d"}
    for item in result:
        assert set(item.keys()) == all_keys

    # Missing values should be "-"
    assert result[0]["c"] == "-"
    assert result[0]["d"] == "-"
    assert result[1]["b"] == "-"
    assert result[1]["d"] == "-"


@pytest.mark.parametrize(
    "dt_input,expected_type",
    [
        ("2024-01-01T10:00:00Z", str),
        (1704110400, str),  # Unix timestamp
        (1704110400.0, str),  # Float timestamp
        ("1704110400", str),  # String timestamp
    ],
)
def test_humanize_datetime_values(studio_config, dt_input, expected_type):
    """Test _humanize_datetime_values function"""
    input_dict = {"started_at": dt_input, "finished_at": dt_input, "other_field": "unchanged"}

    result = utils._humanize_datetime_values(studio_config, input_dict)

    assert isinstance(result["started_at"], expected_type)
    assert isinstance(result["finished_at"], expected_type)
    assert result["other_field"] == "unchanged"
    assert "duration" in result  # Should be added when both start and end are present


def test_humanize_datetime_values_invalid_datetime(studio_config):
    """Test _humanize_datetime_values with invalid datetime"""
    input_dict = {"started_at": "invalid_datetime"}

    with pytest.raises(Exception):
        utils._humanize_datetime_values(studio_config, input_dict)


def test_dict_to_table_items():
    """Test _dict_to_table_items function"""
    input_dict = {"key1": "value1", "key2": "value2", "key3": 123}

    result = utils._dict_to_table_items(input_dict)

    assert isinstance(result, list)
    assert len(result) == 3

    # Check structure
    for item in result:
        assert isinstance(item, dict)
        assert "name" in item
        assert "value" in item

    # Check content
    names = [item["name"] for item in result]
    values = [item["value"] for item in result]

    assert "key1" in names
    assert "key2" in names
    assert "key3" in names
    assert "value1" in values
    assert "value2" in values
    assert 123 in values


def test_caching_behavior(simple_pipeline):
    """Test that cached functions work correctly"""
    # Test get_query_result caching
    query = "SELECT 1 as test"
    result1 = utils.get_query_result(simple_pipeline, query)
    result2 = utils.get_query_result(simple_pipeline, query)

    # Results should be identical (cached)
    pd.testing.assert_frame_equal(result1, result2)

    # Clear cache and test again
    utils.clear_query_cache(simple_pipeline)
    result3 = utils.get_query_result(simple_pipeline, query)
    pd.testing.assert_frame_equal(result1, result3)


def test_error_handling_pipeline_details():
    """Test error handling in pipeline_details"""
    # Create a mock pipeline that raises an exception when accessing credentials
    mock_pipeline = Mock()
    mock_pipeline.pipeline_name = "test"
    mock_pipeline.destination = None
    mock_pipeline.dataset_name = "test_dataset"
    mock_pipeline.default_schema_name = "test_schema"
    mock_pipeline.working_dir = "/test/dir"

    # Mock dataset to raise exception
    mock_dataset = Mock()
    mock_dataset.destination_client.config.credentials = Mock(
        side_effect=Exception("Connection error")
    )
    mock_pipeline.dataset.return_value = mock_dataset

    details = utils.pipeline_details(mock_pipeline)

    # Should handle the exception gracefully
    details_dict = {item["name"]: item["value"] for item in details}
    assert details_dict["credentials"] == "Could not resolve credentials"


def test_integration_with_real_pipeline_trace(studio_config, simple_pipeline):
    """Test functions with real pipeline trace data"""
    if simple_pipeline.last_trace:
        trace_dict = simple_pipeline.last_trace.asdict()

        # Test trace functions with real data
        overview = utils.trace_overview(studio_config, trace_dict)
        assert isinstance(overview, list)

        context = utils.trace_execution_context(studio_config, trace_dict)
        assert isinstance(context, list)

        steps = utils.trace_steps_overview(studio_config, trace_dict)
        assert isinstance(steps, list)

        config_values = utils.trace_resolved_config_values(studio_config, trace_dict)
        assert isinstance(config_values, list)


def test_studio_configuration_hash():
    """Test that StudioConfiguration is hashable (needed for caching)"""
    config1 = StudioConfiguration()
    config2 = StudioConfiguration()

    # Should be hashable
    hash1 = hash(config1)
    hash2 = hash(config2)

    assert isinstance(hash1, int)
    assert isinstance(hash2, int)
    assert hash1 == hash2  # Same default config should have same hash


@pytest.mark.parametrize(
    "pipeline_fixture_name",
    ["simple_pipeline", "fruit_pipeline", "never_run_pipeline", "no_dest_pipeline"],
)
def test_functions_with_different_pipelines(studio_config, request, pipeline_fixture_name):
    """Test key functions work with different types of pipelines"""
    pipeline = request.getfixturevalue(pipeline_fixture_name)

    # Test resolve_studio_config
    config = utils.resolve_studio_config(pipeline)
    assert isinstance(config, StudioConfiguration)

    # Test pipeline_details
    details = utils.pipeline_details(pipeline)
    assert isinstance(details, list)

    # Test create_table_list (only for pipelines with schemas)
    if pipeline.default_schema_name:
        table_list = utils.create_table_list(studio_config, pipeline)
        assert isinstance(table_list, list)
