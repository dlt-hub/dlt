import marimo

from dlt.common.pipeline import get_dlt_pipelines_dir
from dlt.helpers.marimo import _load_package_viewer, load_package_viewer


def test_cell_pipeline_browser():
    kwargs = {"pipeline_path": get_dlt_pipelines_dir()}

    _, defs = _load_package_viewer.pipeline_browser.run(**kwargs)

    assert "pipeline_details" in defs
    assert isinstance(defs["pipeline_details"], marimo.ui.file_browser)


# TODO parameterize test for each supported file type
# this would require a good approach to mocking the state of
# pipelines_dir
def test_cell_file_viewer():
    kwargs = {"pipeline_details": marimo.ui.file_browser(get_dlt_pipelines_dir())}

    _, defs = _load_package_viewer.file_viewer.run(**kwargs)

    assert "obj" in defs
    assert defs["obj"] is None
