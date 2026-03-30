import pytest
import dlt
import pyarrow

from dlt._workspace.helpers.dashboard.utils.queries import (
    clear_query_cache,
    get_query_result,
    get_default_query_for_table,
    get_example_query_for_dataset,
    update_query_history,
)
from tests.workspace.helpers.dashboard.example_pipelines import (
    PIPELINES_WITH_LOAD,
    SUCCESS_PIPELINE_DUCKDB,
)


@pytest.mark.parametrize("pipeline", PIPELINES_WITH_LOAD, indirect=True)
def test_get_query_result(pipeline: dlt.Pipeline):
    """Test getting query result from real pipeline"""
    clear_query_cache()

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


def test_update_query_history_empty():
    result = update_query_history({}, "SELECT 1", 5)
    assert result == {"SELECT 1": 5}


def test_update_query_history_reorders():
    history = {"SELECT 1": 10, "SELECT 2": 20}
    result = update_query_history(history, "SELECT 1", 15)
    assert list(result.keys()) == ["SELECT 1", "SELECT 2"]
    assert result["SELECT 1"] == 15


def test_update_query_history_new_entry_first():
    history = {"SELECT 1": 10, "SELECT 2": 20}
    result = update_query_history(history, "SELECT 3", 30)
    assert list(result.keys()) == ["SELECT 3", "SELECT 1", "SELECT 2"]
