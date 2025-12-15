import pytest
import dlt
import marimo as mo

from dlt._workspace.helpers.dashboard.config import DashboardConfiguration
from dlt._workspace.helpers.dashboard.utils import (
    create_table_list,
    get_row_counts_list,
    create_column_list,
)
from tests.workspace.helpers.dashboard.example_pipelines import (
    ALL_PIPELINES,
    PIPELINES_WITH_LOAD,
    SUCCESS_PIPELINE_DUCKDB,
)


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
