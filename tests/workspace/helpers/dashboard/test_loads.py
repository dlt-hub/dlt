import pytest
import dlt
import marimo as mo
from typing import cast, List, Dict, Any

from dlt._workspace.helpers.dashboard.config import DashboardConfiguration
from dlt._workspace.helpers.dashboard.utils import (
    _get_migrations_count,
    get_loads,
    load_package_status_labels,
)
from tests.workspace.helpers.dashboard.example_pipelines import (
    ALL_PIPELINES,
    EXTRACT_EXCEPTION_PIPELINE,
    NORMALIZE_EXCEPTION_PIPELINE,
    LOAD_EXCEPTION_PIPELINE,
    PIPELINES_WITH_LOAD,
    SUCCESS_PIPELINE_DUCKDB,
    SUCCESS_PIPELINE_FILESYSTEM,
    create_success_pipeline_duckdb,
    create_fruitshop_duckdb_with_shared_dataset,
    create_humans_arrow_duckdb_with_shared_dataset,
)


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


def test_get_migrations_count(temp_pipelines_dir) -> None:
    """Test getting migrations count from the pipeline's last load info"""
    import duckdb

    db_conn = duckdb.connect()
    try:
        pipeline = create_success_pipeline_duckdb(temp_pipelines_dir, db_conn=db_conn)

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
    finally:
        db_conn.close()


def test_pipeline_loads_are_isolated_in_shared_dataset() -> None:
    config = DashboardConfiguration()

    duckdb_p = create_fruitshop_duckdb_with_shared_dataset()
    filesystem_p = create_humans_arrow_duckdb_with_shared_dataset()

    fruits_loads, _, _ = get_loads(config, duckdb_p, limit=100)
    humans_loads, _, _ = get_loads(config, filesystem_p, limit=100)

    assert mo.ui.table(fruits_loads).text is not None
    assert mo.ui.table(humans_loads).text is not None

    assert len(fruits_loads) >= 1
    assert len(humans_loads) >= 1

    fruit_schemas = set(duckdb_p.schema_names)
    human_schemas = set(filesystem_p.schema_names)

    # humans_loads must not contain fruits schemas
    assert all(row["schema_name"] not in fruit_schemas for row in humans_loads)

    # fruits_loads must not contain human schemas
    assert all(row["schema_name"] not in human_schemas for row in fruits_loads)
