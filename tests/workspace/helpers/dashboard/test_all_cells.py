from typing import List

from dlt._workspace.helpers.dashboard import dlt_dashboard

from marimo._ast.cell import Cell
import marimo as mo
from marimo._runtime.control_flow import MarimoStopError

global_defaults = {
    "dlt_query_params": {},
    "mo_query_var_pipeline_name": None,
    "mo_cli_arg_pipelines_dir": None,
    "dlt_pipeline_name": "my_pipeline",
    "dlt_page_tabs": mo.ui.tabs({"tab": ""}),
    "dlt_data_table_list": [],
    "dlt_pipeline_link_list": [],
    "dlt_pipelines_dir": "some_dir",
    "dlt_pipeline_select": mo.ui.multiselect([]),
    "dlt_pipeline_count": 0,
    "dlt_schem_table_list": [],
    "dlt_query_result": [],
    "dlt_query_editor": "",
    "dlt_run_query_button": mo.ui.button(),
    "dlt_cache_query_results": mo.ui.switch(),
    "dlt_execute_query_on_change": mo.ui.switch(),
    "dlt_schema_show_other_hints": mo.ui.switch(),
    "dlt_schema_show_custom_hints": mo.ui.switch(),
    "dlt_schema_show_dlt_columns": mo.ui.switch(),
    "dlt_schema_show_type_hints": mo.ui.switch(),
    "dlt_pipeline": None,
    "dlt_query_history_table": None,
    "dlt_query": "",
    "dlt_loads_table": None,
    "dlt_all_pipelines": [],
    "mo_cli_arg_with_test_identifiers": True,
}


def test_run_all_cells():
    """
    Runs al cells with basic values, StopException is allowed
    """
    cells: List[Cell] = []
    for item in dir(dlt_dashboard):
        c = getattr(dlt_dashboard, item)
        if isinstance(c, Cell):
            cells.append(c)

    assert len(cells) > 0, "No cells found"

    for cell in cells:
        # the two cells below only work in a marimo context
        if cell.name in ["utils_cli_args_and_query_vars", "prepare_cli_args", "app_tabs"]:
            continue
        try:
            run_args = {k: v for k, v in global_defaults.items() if k in cell.refs}
            missing_args = [arg for arg in cell.refs if arg not in global_defaults]
            cell.run(**run_args)
        except MarimoStopError:
            pass
        except Exception as e:
            print(f"Failed running cell {cell.name}: {e}")
            print(f"Missing args: {missing_args}")
            raise e
