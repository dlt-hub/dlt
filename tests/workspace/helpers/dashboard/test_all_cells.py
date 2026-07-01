import os
from typing import Any, Dict, List, Optional, cast

import pytest

import dlt
from dlt._workspace.helpers.dashboard import dlt_dashboard, strings

from marimo._ast.cell import Cell
import marimo as mo
from marimo._runtime.control_flow import MarimoStopError
from tests.utils import get_test_storage_root
from tests.workspace.helpers.dashboard.conftest import NO_PIPELINES_TEXT, NO_TRACE_TEXT

global_defaults = {
    "dlt_query_params": {},
    "mo_query_var_pipeline_name": None,
    "mo_cli_arg_pipelines_dir": None,
    "dlt_pipeline_name": "my_pipeline",
    "dlt_page_tabs": mo.ui.tabs({"tab": ""}),
    "dlt_data_table_list": [],
    "dlt_pipelines_dir": os.path.join(os.path.abspath(get_test_storage_root()), "some_dir"),
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
    "dlt_local_pipeline_names": [],
    "mo_cli_arg_with_test_identifiers": True,
}

_DEFAULT_PIPELINES_DIR: str = cast(str, global_defaults["dlt_pipelines_dir"])


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


def _run_home(
    *,
    dlt_pipeline_name: Any,
    dlt_all_pipelines: List[Dict[str, str]],
    dlt_local_pipeline_names: Optional[List[str]] = None,
    dlt_pipelines_dir: str = _DEFAULT_PIPELINES_DIR,
) -> str:
    """Run the home cell with the given selection state and return its rendered text."""
    if dlt_local_pipeline_names is None:
        dlt_local_pipeline_names = [p["name"] for p in dlt_all_pipelines]

    output, _ = cast(
        Any,
        dlt_dashboard.home.run(
            dlt_profile_select=mo.ui.dropdown(["dev"]),
            dlt_pipeline_select=mo.ui.multiselect(
                [p["name"] for p in dlt_all_pipelines],
                label=strings.app_pipeline_select_label,
            ),
            dlt_local_pipeline_names=dlt_local_pipeline_names,
            dlt_pipelines_dir=dlt_pipelines_dir,
            dlt_refresh_button=mo.ui.run_button(label=strings.app_refresh_button),
            dlt_pipeline_name=dlt_pipeline_name,
            dlt_file_watcher=None,
        ),
    )
    return output.text if output is not None else ""


def test_home_no_pipeline_selected(success_pipeline_duckdb: dlt.Pipeline):
    """Pipelines exist but none selected: the cell picks the neutral hint, not the empty landing."""
    html = _run_home(
        dlt_pipeline_name=None,
        dlt_all_pipelines=[{"name": success_pipeline_duckdb.pipeline_name}],
        dlt_pipelines_dir=success_pipeline_duckdb.pipelines_dir,
    )
    assert strings.home_no_pipeline_selected in html
    assert NO_PIPELINES_TEXT not in html


def test_home_no_pipelines_at_all():
    """Empty workspace: the cell picks the no-pipelines landing."""
    html = _run_home(dlt_pipeline_name=None, dlt_all_pipelines=[])
    assert NO_PIPELINES_TEXT in html
    assert strings.home_no_pipeline_selected not in html


def test_home_phantom_pipeline_shows_empty_landing():
    """A ?pipeline= name with no directory on disk is not treated as an existing pipeline."""
    html = _run_home(
        dlt_pipeline_name=None,
        dlt_all_pipelines=[{"name": "ghost"}],
        dlt_local_pipeline_names=[],
    )
    assert NO_PIPELINES_TEXT in html
    assert strings.home_no_pipeline_selected not in html


def test_home_pipeline_selected(success_pipeline_duckdb: dlt.Pipeline):
    """A pipeline is selected: render its home view, neither landing hint present."""
    html = _run_home(
        dlt_pipeline_name=success_pipeline_duckdb.pipeline_name,
        dlt_all_pipelines=[{"name": success_pipeline_duckdb.pipeline_name}],
        dlt_pipelines_dir=success_pipeline_duckdb.pipelines_dir,
    )
    assert NO_PIPELINES_TEXT not in html
    assert strings.home_no_pipeline_selected not in html
    assert strings.app_refresh_button in html


def test_home_attach_error_keeps_selector_without_refresh():
    """A pipeline that cannot be attached shows the error and keeps the selector, no refresh."""
    html = _run_home(dlt_pipeline_name="ghost", dlt_all_pipelines=[{"name": "ghost"}])
    assert strings.home_error_attach_pipeline.format("ghost") in html
    assert strings.app_pipeline_select_label in html
    assert strings.app_refresh_button not in html


_SECTION_SWITCH_DEFAULTS = {
    "dlt_section_info_switch": mo.ui.switch(value=True),
    "dlt_section_schema_switch": mo.ui.switch(value=True),
    "dlt_section_state_switch": mo.ui.switch(value=True),
    "dlt_section_trace_switch": mo.ui.switch(value=True),
    "dlt_section_loads_switch": mo.ui.switch(value=True),
    "dlt_trace_steps_table": None,
    "dlt_config": None,
}


@pytest.mark.parametrize(
    "cell_name",
    [
        "section_info",
        "section_schema",
        "section_state",
        "section_trace",
        "section_loads",
    ],
)
def test_sections_render_no_content_without_pipeline(cell_name: str):
    """Deselecting a pipeline leaves no stale section content (marker stays empty)."""
    cell = getattr(dlt_dashboard, cell_name)
    defaults = {**global_defaults, **_SECTION_SWITCH_DEFAULTS, "dlt_pipeline": None}
    run_args = {k: v for k, v in defaults.items() if k in cell.refs}
    output, _ = cast(Any, cell.run(**run_args))
    text = output.text if output is not None else ""
    assert "has-content" not in text


def test_home_selected_pipeline_never_ran_stays_on_selected_path(
    never_ran_pipline: dlt.Pipeline,
):
    """A selected pipeline with no trace shows its no-trace hint, not a landing hint."""
    html = _run_home(
        dlt_pipeline_name=never_ran_pipline.pipeline_name,
        dlt_all_pipelines=[{"name": never_ran_pipline.pipeline_name}],
        dlt_pipelines_dir=never_ran_pipline.pipelines_dir,
    )
    assert NO_TRACE_TEXT in html
    assert NO_PIPELINES_TEXT not in html
    assert strings.home_no_pipeline_selected not in html
