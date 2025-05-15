import marimo as mo

from dlt.helpers.studio.app import page_welcome


def test_welcome_cell():
    output, defs = page_welcome.run(  # type: ignore
        dlt_pipelines_dir="some_dir",
        dlt_pipeline_select=mo.ui.multiselect([1, 2, 3]),
        dlt_pipeline_count=5,
        dlt_pipeline_link_list="[LINK1, LINK2, LINK3]",
    )

    assert (
        "<code>dlt studio</code> has found <code>5</code> pipelines in local directory"
        in output.text
    )
