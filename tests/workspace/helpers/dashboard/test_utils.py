from typing import cast, Set, List, Dict, Any
import os
import tempfile
from datetime import datetime
from pathlib import Path
import re

import marimo as mo
import pyarrow
import pytest

import dlt
from dlt.common import pendulum
from dlt._workspace.helpers.dashboard.config import DashboardConfiguration
from dlt._workspace.helpers.dashboard.utils import (
    PICKLE_TRACE_FILE,
    get_dashboard_config_sections,
    get_query_result_cached,
    resolve_dashboard_config,
    get_local_pipelines,
    get_pipeline,
    pipeline_details,
    create_table_list,
    get_row_counts_list,
    create_column_list,
    get_query_result,
    get_row_counts,
    get_loads,
    trace_overview,
    trace_execution_context,
    trace_steps_overview,
    style_cell,
    _without_none_or_empty_string,
    _align_dict_keys,
    _humanize_datetime_values,
    _dict_to_table_items,
    build_exception_section,
    get_local_data_path,
    remote_state_details,
    sanitize_trace_for_display,
    get_pipeline_last_run,
    trace_resolved_config_values,
    trace_step_details,
    get_source_and_resouce_state_for_table,
    get_default_query_for_table,
    get_example_query_for_dataset,
    _get_steps_data_and_status,
    _get_migrations_count,
    build_pipeline_execution_visualization,
    _collect_load_packages_from_trace,
    load_package_status_labels,
    TPipelineRunStatus,
    TVisualPipelineStep,
)

from tests.workspace.helpers.dashboard.example_pipelines import (
    SUCCESS_PIPELINE_DUCKDB,
    SUCCESS_PIPELINE_FILESYSTEM,
    EXTRACT_EXCEPTION_PIPELINE,
    NORMALIZE_EXCEPTION_PIPELINE,
    NEVER_RAN_PIPELINE,
    LOAD_EXCEPTION_PIPELINE,
    NO_DESTINATION_PIPELINE,
    create_success_pipeline_duckdb,
)
from tests.workspace.helpers.dashboard.example_pipelines import (
    ALL_PIPELINES,
    PIPELINES_WITH_EXCEPTIONS,
    PIPELINES_WITH_LOAD,
)
from tests.workspace.utils import isolated_workspace


@pytest.fixture
def temp_pipelines_dir():
    """Create a temporary directory structure for testing pipelines"""
    with tempfile.TemporaryDirectory() as temp_dir:
        pipelines_dir = Path(temp_dir) / "pipelines"
        pipelines_dir.mkdir()

        # Create some test pipeline directories
        (pipelines_dir / "success_pipeline_1").mkdir()
        (pipelines_dir / "success_pipeline_2").mkdir()
        (pipelines_dir / "_dlt_internal").mkdir()

        # Create trace files with different timestamps
        trace_file_1 = pipelines_dir / "success_pipeline_1" / PICKLE_TRACE_FILE
        trace_file_1.touch()
        # Set modification time to 2 days ago
        os.utime(trace_file_1, (1000000, 1000000))

        trace_file_2 = pipelines_dir / "success_pipeline_2" / PICKLE_TRACE_FILE
        trace_file_2.touch()
        # Set modification time to 1 day ago (more recent)
        os.utime(trace_file_2, (2000000, 2000000))

        yield str(pipelines_dir)


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_get_pipeline_last_run(pipeline: dlt.Pipeline):
    """Test getting the last run of a pipeline"""
    if pipeline.pipeline_name in [NEVER_RAN_PIPELINE, NO_DESTINATION_PIPELINE]:
        assert get_pipeline_last_run(pipeline.pipeline_name, pipeline.pipelines_dir) == 0
    else:
        assert get_pipeline_last_run(pipeline.pipeline_name, pipeline.pipelines_dir) > 1000000


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_build_exception_section(pipeline: dlt.Pipeline):
    if pipeline.pipeline_name in PIPELINES_WITH_EXCEPTIONS:
        assert "Show full stacktrace" in build_exception_section(pipeline)[0].text
    else:
        assert not build_exception_section(pipeline)


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_get_local_data_path(pipeline: dlt.Pipeline):
    if pipeline.pipeline_name in [LOAD_EXCEPTION_PIPELINE, NO_DESTINATION_PIPELINE]:
        # custom destination does not support local data path
        assert get_local_data_path(pipeline) is None
    else:
        assert get_local_data_path(pipeline)


def test_get_dashboard_config_sections(success_pipeline_duckdb) -> None:
    # NOTE: "dashboard" obligatory section comes from configuration __section__
    assert get_dashboard_config_sections(success_pipeline_duckdb) == (
        "pipelines",
        "success_pipeline_duckdb",
    )
    assert get_dashboard_config_sections(None) == ()

    # create workspace context
    with isolated_workspace("configured_workspace"):
        assert get_dashboard_config_sections(None) == ("workspace",)


def test_resolve_dashboard_config(success_pipeline_duckdb) -> None:
    """Test resolving dashboard config with a real pipeline"""

    os.environ["PIPELINES__SUCCESS_PIPELINE_DUCKDB__DASHBOARD__DATETIME_FORMAT"] = "some format"
    os.environ["DASHBOARD__DATETIME_FORMAT"] = "other format"

    config = resolve_dashboard_config(success_pipeline_duckdb)

    assert isinstance(config, DashboardConfiguration)
    assert isinstance(config.datetime_format, str)
    assert config.datetime_format == "some format"

    other_pipeline = dlt.pipeline(pipeline_name="other_pipeline", destination="duckdb")
    config = resolve_dashboard_config(other_pipeline)
    assert config.datetime_format == "other format"

    # create workspace context
    with isolated_workspace("configured_workspace"):
        os.environ["WORKSPACE__DASHBOARD__DATETIME_FORMAT"] = "workspace format"
        config = resolve_dashboard_config(None)
        assert config.datetime_format == "workspace format"


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_get_pipelines(pipeline: dlt.Pipeline):
    """Test getting local pipelines"""
    pipelines_dir, pipelines = get_local_pipelines(pipeline.pipelines_dir)
    assert pipelines_dir == pipeline.pipelines_dir
    assert len(pipelines) == 1
    assert pipelines[0]["name"] == pipeline.pipeline_name


@pytest.mark.parametrize("pipeline", PIPELINES_WITH_LOAD, indirect=True)
def test_get_source_and_resouce_state_for_table(pipeline: dlt.Pipeline):
    """Test getting source and resource state for a table"""
    table = pipeline.default_schema.tables["purchases"]
    resource_name, source_state, resource_state = get_source_and_resouce_state_for_table(
        table, pipeline, pipeline.default_schema_name
    )
    assert resource_name
    assert source_state == {}
    assert resource_state.get("incremental").get("id") is not None

    # check it can be rendered with marimo
    assert mo.json(resource_state).text
    assert mo.json(source_state).text


def test_get_local_pipelines_with_temp_dir(temp_pipelines_dir):
    """Test getting local pipelines with temporary directory"""
    pipelines_dir, pipelines = get_local_pipelines(temp_pipelines_dir)

    assert pipelines_dir == temp_pipelines_dir
    assert len(pipelines) == 3  # success_pipeline_1, success_pipeline_2, _dlt_internal

    # Should be sorted by timestamp (descending)
    pipeline_names = [p["name"] for p in pipelines]
    assert "success_pipeline_2" in pipeline_names
    assert "success_pipeline_1" in pipeline_names
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


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_get_pipeline(pipeline: dlt.Pipeline):
    """Test getting a real pipeline by name"""
    pipeline = get_pipeline(pipeline.pipeline_name, pipeline.pipelines_dir)

    assert pipeline.pipeline_name == pipeline.pipeline_name
    assert pipeline.dataset_name == pipeline.dataset_name


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_pipeline_details(pipeline, temp_pipelines_dir):
    """Test getting pipeline details from a real pipeline"""
    config = DashboardConfiguration()
    result = pipeline_details(config, pipeline, temp_pipelines_dir)

    assert isinstance(result, list)
    if pipeline.pipeline_name in PIPELINES_WITH_LOAD:
        assert len(result) == 9
    elif pipeline.pipeline_name in [LOAD_EXCEPTION_PIPELINE, NORMALIZE_EXCEPTION_PIPELINE]:
        # custom destination does not support remote data info
        assert len(result) == 8
    else:
        # no remote data info
        assert len(result) == 7

    # Convert to dict for easier testing
    details_dict = {item["name"]: item["value"] for item in result}

    assert details_dict["pipeline_name"] == pipeline.pipeline_name
    if pipeline.pipeline_name == NO_DESTINATION_PIPELINE:
        assert details_dict["destination"] == "No destination set"
    elif pipeline.pipeline_name == SUCCESS_PIPELINE_FILESYSTEM:
        assert details_dict["destination"] == "filesystem (dlt.destinations.filesystem)"
    elif pipeline.pipeline_name == LOAD_EXCEPTION_PIPELINE:
        assert details_dict["destination"] == "dummy (dlt.destinations.dummy)"
    else:
        assert details_dict["destination"] == "duckdb (dlt.destinations.duckdb)"
    assert details_dict["dataset_name"] == pipeline.dataset_name
    if pipeline.pipeline_name in PIPELINES_WITH_LOAD or pipeline.pipeline_name in [
        LOAD_EXCEPTION_PIPELINE,
        NORMALIZE_EXCEPTION_PIPELINE,
    ]:
        assert details_dict["schemas"].startswith("fruitshop")
    else:
        assert "schemas" not in details_dict

    assert details_dict["working_dir"].endswith(pipeline.pipeline_name)


@pytest.mark.parametrize("pipeline", PIPELINES_WITH_LOAD, indirect=True)
@pytest.mark.parametrize("show_internals", [True, False])
@pytest.mark.parametrize("show_child_tables", [True, False])
def test_create_table_list(pipeline, show_internals, show_child_tables):
    """Test creating a basic table list with real schema"""
    config = DashboardConfiguration()

    result = create_table_list(
        config,
        pipeline,
        selected_schema_name=pipeline.default_schema_name,
        show_internals=show_internals,
        show_child_tables=show_child_tables,
    )
    # check it can be rendered as table with marimo
    assert mo.ui.table(result).text is not None

    base_table_names = {"inventory", "purchases", "customers", "inventory_categories"}
    dlt_table_names = {"_dlt_loads", "_dlt_version", "_dlt_pipeline_state"}
    child_table_names = {"purchases__child"}

    expected_table_names = {*base_table_names}
    if show_internals:
        expected_table_names.update(dlt_table_names)
    if show_child_tables:
        expected_table_names.update(child_table_names)

    table_names = {table["name"] for table in result}
    assert set(table_names) == expected_table_names


@pytest.mark.parametrize("pipeline", PIPELINES_WITH_LOAD, indirect=True)
@pytest.mark.parametrize("show_internals", [True, False])
@pytest.mark.parametrize("show_type_hints", [True, False])
@pytest.mark.parametrize("show_other_hints", [True, False])
@pytest.mark.parametrize("show_custom_hints", [True, False])
def test_create_column_list_basic(
    pipeline, show_internals, show_type_hints, show_other_hints, show_custom_hints
):
    """Test creating a basic column list with real schema"""
    config = DashboardConfiguration()

    # Should exclude _dlt columns by default, will also not show incomplete columns
    result = create_column_list(
        config,
        pipeline,
        selected_schema_name=pipeline.default_schema_name,
        table_name="purchases",
        show_internals=show_internals,
        show_type_hints=show_type_hints,
        show_other_hints=show_other_hints,
        show_custom_hints=show_custom_hints,
    )

    # check it can be rendered as table with marimo
    assert mo.ui.table(result).text is not None

    # check visible columns
    base_column_names = {"customer_id", "quantity", "id", "inventory_id", "date"}
    dlt_column_names = {"_dlt_load_id", "_dlt_id"}

    expected_column_names = {*base_column_names}
    if show_internals:
        expected_column_names.update(dlt_column_names)

    column_names = {col["name"] for col in result}
    assert column_names == expected_column_names

    # Find the id column
    id_column = next(col for col in result if col["name"] == "id")

    # check type hints
    if show_type_hints:
        assert id_column["data_type"] == "bigint"
        assert id_column["nullable"] is False
    else:
        assert "data_type" not in id_column
        assert "nullable" not in id_column

    if show_other_hints:
        assert id_column["primary_key"] is True
    else:
        assert "primary_key" not in id_column

    if show_custom_hints:
        assert id_column["x-custom"] == "foo"
    else:
        assert "x-custom" not in id_column


@pytest.mark.parametrize("pipeline", PIPELINES_WITH_LOAD, indirect=True)
def test_get_query_result(pipeline: dlt.Pipeline):
    """Test getting query result from real pipeline"""
    # Clear cache first
    get_query_result_cached.cache_clear()

    result, error_message, traceback_string = get_query_result(
        pipeline, "SELECT COUNT(*) as count FROM purchases"
    )

    if pipeline.pipeline_name in PIPELINES_WITH_LOAD:
        assert isinstance(result, pyarrow.Table)
        assert len(result) == 1
        assert (
            result[0][0].as_py() == 100
            if pipeline.pipeline_name == SUCCESS_PIPELINE_DUCKDB
            else 103
        )  #  merge does not work on filesystem
    else:
        assert len(result) == 0
        assert error_message
        assert traceback_string


@pytest.mark.parametrize("pipeline", PIPELINES_WITH_LOAD, indirect=True)
def test_get_default_query_for_table(pipeline: dlt.Pipeline):
    query, error_message, traceback_string = get_default_query_for_table(
        pipeline, pipeline.default_schema_name, "purchases", True
    )
    assert query == 'SELECT\n  *\nFROM "purchases"\nLIMIT 1000'
    assert not error_message
    assert not traceback_string
    assert query


@pytest.mark.parametrize("pipeline", PIPELINES_WITH_LOAD, indirect=True)
def test_get_example_query_for_dataset(pipeline: dlt.Pipeline):
    query, error_message, traceback_string = get_example_query_for_dataset(
        pipeline, pipeline.default_schema_name
    )
    assert query == 'SELECT\n  *\nFROM "customers"\nLIMIT 1000'
    assert not error_message
    assert not traceback_string
    assert query


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_get_row_counts_list(pipeline: dlt.Pipeline):
    """Test getting row counts from real pipeline"""
    result = get_row_counts_list(pipeline)

    # check it can be rendered as table with marimo
    assert mo.ui.table(result).text is not None

    reverted_result = {i["name"]: i["row_count"] for i in result}

    if pipeline.pipeline_name in PIPELINES_WITH_LOAD:
        assert reverted_result == {
            "customers": 13,
            "inventory": 6,
            "purchases": (
                100 if pipeline.pipeline_name == SUCCESS_PIPELINE_DUCKDB else 103
            ),  #  merge does not work on filesystem
            "purchases__child": 3,
            "inventory_categories": 3,
            "_dlt_version": 3,
            "_dlt_loads": 4,
            "_dlt_pipeline_state": 3,
        }
    else:
        reverted_result = {}


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_get_loads(pipeline: dlt.Pipeline):
    """Test getting loads from real pipeline"""
    config = DashboardConfiguration()

    # Clear cache first
    result, error_message, traceback_string = get_loads(config, pipeline, limit=100)

    # check it can be rendered as table with marimo
    assert mo.ui.table(result).text is not None

    if pipeline.pipeline_name in PIPELINES_WITH_LOAD:
        assert isinstance(result, list)
        assert len(result) >= 1  # Should have at least one load
        assert not error_message
        assert not traceback_string
        if result:
            load = result[0]
            assert "load_id" in load
    else:
        assert result == []
        assert error_message
        assert traceback_string


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_trace(pipeline: dlt.Pipeline):
    """Test trace overview with real trace data"""
    config = DashboardConfiguration()
    trace = pipeline.last_trace

    if pipeline.pipeline_name in [NEVER_RAN_PIPELINE, NO_DESTINATION_PIPELINE]:
        assert trace is None
        return

    # overview
    result = trace_overview(config, trace)
    # check it can be rendered as table with marimo
    assert mo.ui.table(result).text is not None

    assert {item["name"] for item in result} == {
        "pipeline_name",
        "started_at",
        "finished_at",
        "transaction_id",
        "duration",
    }
    values_dict = {item["name"]: item["value"] for item in result}
    assert values_dict["pipeline_name"] == pipeline.pipeline_name

    # execution context
    result = trace_execution_context(config, trace)
    # check it can be rendered as table with marimo
    assert mo.ui.table(result).text is not None

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
    # TODO: inspect values

    # steps overview
    result = trace_steps_overview(config, trace)
    # check it can be rendered as table with marimo
    assert mo.ui.table(result).text is not None

    if pipeline.pipeline_name == EXTRACT_EXCEPTION_PIPELINE:
        assert len(result) == 1
        assert result[0]["step"] == "extract"
    elif pipeline.pipeline_name == NORMALIZE_EXCEPTION_PIPELINE:
        assert len(result) == 2
        assert result[0]["step"] == "extract"
        assert result[1]["step"] == "normalize"
    else:
        assert len(result) == 3
        assert result[0]["step"] == "extract"
        assert result[1]["step"] == "normalize"
        assert result[2]["step"] == "load"

    # TODO: inspect values of trace steps overview

    for item in result:
        result = trace_step_details(config, trace, item["step"])
        # TODO: inspect trace step details

    # resolved config values (TODO: add at least one config value)
    result = trace_resolved_config_values(config, trace)
    # check it can be rendered as table with marimo
    assert mo.ui.table(result).text is not None


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_get_remote_state_details(pipeline: dlt.Pipeline):
    remote_state = remote_state_details(pipeline)
    # check it can be rendered as table with marimo
    assert mo.ui.table(remote_state).text is not None

    if pipeline.pipeline_name in PIPELINES_WITH_LOAD:
        assert remote_state[0] == {"name": "state_version", "value": 3}
        assert remote_state[1]["name"] == "schemas"
        assert remote_state[1]["value"].startswith("fruitshop")
        assert remote_state[2]["name"] == ""
        assert remote_state[2]["value"].startswith("fruitshop_customers")
    else:
        assert remote_state[0] == {
            "name": "Info",
            "value": "Could not restore state from destination",
        }


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
    input_dict = {
        "pipeline_name": "success_pipeline",
        "destination": "duckdb",
        "status": "completed",
    }

    result = _dict_to_table_items(input_dict)

    expected = [
        {"name": "pipeline_name", "value": "success_pipeline"},
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
    assert most_recent["name"] == "success_pipeline_2"
    assert most_recent["timestamp"] == 2000000


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_integration_pipeline_workflow(pipeline, temp_pipelines_dir):
    """Test integration scenario with complete pipeline workflow"""
    # Test pipeline details
    config = DashboardConfiguration()

    details = pipeline_details(config, pipeline, temp_pipelines_dir)

    # check it can be rendered as table with marimo
    assert mo.ui.table(details).text is not None

    details_dict = {item["name"]: item["value"] for item in details}
    assert details_dict["pipeline_name"] == pipeline.pipeline_name

    # Test row counts
    row_counts = get_row_counts(pipeline)

    if pipeline.pipeline_name in PIPELINES_WITH_LOAD:
        assert row_counts["customers"] == 13
    else:
        assert row_counts == {}

    # Test query execution
    query_result, error_message, traceback_string = get_query_result(
        pipeline, "SELECT name FROM customers ORDER BY id"
    )
    if pipeline.pipeline_name in PIPELINES_WITH_LOAD:
        assert len(query_result) == 13
        assert query_result[0][0].as_py() == "simon"
        assert not error_message
        assert not traceback_string
    else:
        assert len(query_result) == 0
        assert error_message
        assert traceback_string

    # Test loads
    config = DashboardConfiguration()
    loads, error_message, traceback_string = get_loads(config, pipeline)
    if pipeline.pipeline_name in PIPELINES_WITH_LOAD:
        assert len(loads) >= 1
        assert not error_message
        assert not traceback_string
    else:
        assert error_message
        assert traceback_string
        assert loads == []


@pytest.mark.parametrize("pipeline", ALL_PIPELINES, indirect=True)
def test_sanitize_trace_for_display(pipeline: dlt.Pipeline):
    """Test sanitizing trace for display"""
    trace = pipeline.last_trace
    sanitized = sanitize_trace_for_display(trace)
    assert sanitized is not None
    assert isinstance(sanitized, dict)
    # check it can be rendered with marimo
    assert mo.json(sanitized).text is not None


@pytest.mark.parametrize(
    "pipeline, expected_steps, expected_status",
    [
        (SUCCESS_PIPELINE_DUCKDB, {"extract", "normalize", "load"}, "succeeded"),
        (SUCCESS_PIPELINE_FILESYSTEM, {"extract", "normalize", "load"}, "succeeded"),
        (EXTRACT_EXCEPTION_PIPELINE, {"extract"}, "failed"),
        (LOAD_EXCEPTION_PIPELINE, {"extract", "normalize", "load"}, "failed"),
    ],
    indirect=["pipeline"],
)
def test_get_steps_data_and_status(
    pipeline: dlt.Pipeline,
    expected_steps: Set[TVisualPipelineStep],
    expected_status: TPipelineRunStatus,
) -> None:
    """Test getting steps data and the pipeline execution status from trace"""
    trace = pipeline.last_trace

    steps_data, status = _get_steps_data_and_status(trace.steps)
    assert len(steps_data) == len(expected_steps)
    assert status == expected_status

    assert all(step.duration_ms > 0 for step in steps_data)
    if expected_status == "succeeded":
        assert all(step.failed is False for step in steps_data)
    else:
        assert any(step.failed is True for step in steps_data)

    assert set([step.step for step in steps_data]) == expected_steps


def test_get_migrations_count(temp_pipelines_dir) -> None:
    """Test getting migrations count from the pipeline's last load info"""

    pipeline = create_success_pipeline_duckdb(temp_pipelines_dir)

    migrations_count = _get_migrations_count(pipeline.last_trace.last_load_info)
    assert migrations_count == 1

    # Trigger multiple migrations
    pipeline.extract([{"id": 1, "name": "test"}], table_name="my_table")
    pipeline.extract([{"id": 2, "name": "test2", "new_column": "value"}], table_name="my_table")
    pipeline.extract(
        [{"id": 3, "name": "test3", "new_column": "value", "another_column": 100}],
        table_name="my_table",
    )
    pipeline.normalize()
    pipeline.load()
    migrations_count = _get_migrations_count(pipeline.last_trace.last_load_info)
    assert migrations_count == 3


@pytest.mark.parametrize(
    "pipeline, expected_steps, expected_status",
    [
        (SUCCESS_PIPELINE_DUCKDB, {"extract", "normalize", "load"}, "succeeded"),
        (SUCCESS_PIPELINE_FILESYSTEM, {"extract", "normalize", "load"}, "succeeded"),
        (EXTRACT_EXCEPTION_PIPELINE, {"extract"}, "failed"),
        (LOAD_EXCEPTION_PIPELINE, {"extract", "normalize", "load"}, "failed"),
    ],
    indirect=["pipeline"],
)
def test_build_pipeline_execution_visualization(
    pipeline: dlt.Pipeline,
    expected_steps: Set[TVisualPipelineStep],
    expected_status: TPipelineRunStatus,
) -> None:
    """Test overall pipeline execution visualization logic"""

    trace = pipeline.last_trace

    html = build_pipeline_execution_visualization(trace)
    html_str = str(html.text)

    # Check for CSS class structure
    assert 'class="pipeline-execution-container"' in html_str
    assert 'class="pipeline-execution-layout"' in html_str
    assert 'class="pipeline-execution-timeline"' in html_str
    assert 'class="pipeline-execution-badges"' in html_str

    assert f"Last execution ID: <strong>{trace.transaction_id[:8]}</strong>" in html_str
    total_time_match = re.search(
        r"<div>Total time: <strong>([\d.]+)(ms|s)?</strong></div>", html_str
    )
    assert total_time_match is not None

    # Check for status badge using CSS classes (not inline styles)
    status_badge_class = (
        "status-badge-green" if expected_status == "succeeded" else "status-badge-red"
    )
    assert (
        f'<div class="status-badge {status_badge_class}"><strong>{expected_status}</strong></div>'
        in html_str
    )

    # Check for migration badge using CSS classes (not inline styles)
    migrations_count = _get_migrations_count(trace.last_load_info) if trace.last_load_info else 0
    migration_badge = (
        f'<div class="status-badge status-badge-yellow"><strong>{migrations_count} dataset'
        " migration(s)</strong></div>"
    )
    if migrations_count != 0:
        assert migration_badge in html_str
    else:
        assert migration_badge not in html_str

    steps_data, _ = _get_steps_data_and_status(trace.steps)
    for step_data in steps_data:
        duration_pattern = re.search(rf"{step_data.step.capitalize()}\s+([\d.]+)(ms|s)?", html_str)
        assert duration_pattern is not None

    if "extract" in expected_steps:
        assert "var(--dlt-color-lime)" in html_str
    if "normalize" in expected_steps:
        assert "var(--dlt-color-aqua)" in html_str
    if "load" in expected_steps:
        assert "var(--dlt-color-pink)" in html_str


@pytest.mark.parametrize(
    "pipeline",
    [
        SUCCESS_PIPELINE_DUCKDB,
        SUCCESS_PIPELINE_FILESYSTEM,
        EXTRACT_EXCEPTION_PIPELINE,
        NORMALIZE_EXCEPTION_PIPELINE,
        LOAD_EXCEPTION_PIPELINE,
    ],
    indirect=["pipeline"],
)
def test_collect_load_packages_from_trace(
    pipeline: dlt.Pipeline,
) -> None:
    """Test getting load package status labels from trace"""

    trace = pipeline.last_trace
    table = load_package_status_labels(trace)

    list_of_load_package_info = cast(List[Dict[str, Any]], table.data)

    if pipeline.pipeline_name in ["success_pipeline_duckdb", "success_pipeline_filesystem"]:
        assert len(list_of_load_package_info) == 2
        assert all(
            "loaded" in str(load_package_info["status"].text)
            for load_package_info in list_of_load_package_info
        )

    elif pipeline.pipeline_name == "extract_exception_pipeline":
        assert len(list_of_load_package_info) == 1
        assert "discarded" in str(list_of_load_package_info[0]["status"].text)

    elif pipeline.pipeline_name == "load_exception_pipeline":
        assert len(list_of_load_package_info) == 1
        assert "aborted" in str(list_of_load_package_info[0]["status"].text)

    elif pipeline.pipeline_name == "normalize_exception_pipeline":
        assert len(list_of_load_package_info) == 1
        assert "pending" in str(list_of_load_package_info[0]["status"].text)
