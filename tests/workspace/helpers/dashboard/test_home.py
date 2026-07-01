import marimo as mo

from dlt._workspace.helpers.dashboard import strings
from dlt._workspace.helpers.dashboard.utils import home

from tests.workspace.helpers.dashboard.conftest import NO_PIPELINES_TEXT


def _profile_select() -> mo.ui.dropdown:
    return mo.ui.dropdown(["dev"])


def _pipeline_select() -> mo.ui.multiselect:
    return mo.ui.multiselect(["my_pipeline"], label=strings.app_pipeline_select_label)


def test_render_no_pipeline_selected_home_keeps_dropdown():
    """The deselected landing keeps the pipeline dropdown and shows the neutral hint."""
    html = mo.vstack(
        home.render_no_pipeline_selected_home(_profile_select(), _pipeline_select())
    ).text
    assert strings.app_pipeline_select_label in html
    assert strings.home_no_pipeline_selected in html
    assert NO_PIPELINES_TEXT not in html


def test_render_no_pipelines_home_omits_dropdown():
    """Regression guard for the scaffold refactor: empty workspace omits the dropdown."""
    html = mo.vstack(home.render_no_pipelines_home(_profile_select())).text
    assert strings.app_pipeline_select_label not in html
    assert NO_PIPELINES_TEXT in html
    assert strings.home_no_pipeline_selected not in html
