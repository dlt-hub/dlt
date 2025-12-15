import marimo as mo
import pytest

from dlt._workspace.helpers.dashboard.config import DashboardConfiguration
from dlt._workspace.helpers.dashboard.utils import (
    pipeline_details,
    get_query_result,
    get_row_counts,
    get_loads,
)

from tests.workspace.helpers.dashboard.example_pipelines import (
    ALL_PIPELINES,
    PIPELINES_WITH_LOAD,
)


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
