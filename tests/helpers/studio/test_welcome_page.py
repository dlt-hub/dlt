import marimo as mo

from dlt.helpers.studio.app import home


def test_welcome_cell():
    output, defs = home.run(  # type: ignore[unused-ignore]
        dlt_pipeline_select=mo.ui.multiselect([1, 2, 3]),
        dlt_all_pipelines=[
            {"name": "pipeline1", "link": "link1", "timestamp": 0},
            {"name": "pipeline2", "link": "link2", "timestamp": 1},
            {"name": "pipeline3", "link": "link3", "timestamp": 2},
        ],
        dlt_pipelines_dir="some_dir",
    )

    assert (
        "<code>dlt studio</code> has found <code>3</code> pipelines in local directory"
        in output.text
    )
