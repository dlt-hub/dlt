import marimo as mo

from dlt.helpers.dashboard.dlt_dashboard import home


def test_welcome_cell():
    output, defs = home.run(  # type: ignore[unused-ignore,misc]
        dlt_pipeline_select=mo.ui.multiselect([1, 2, 3]),
        dlt_all_pipelines=[
            {"name": "pipeline1", "link": "link1", "timestamp": 0},
            {"name": "pipeline2", "link": "link2", "timestamp": 1},
            {"name": "pipeline3", "link": "link3", "timestamp": 2},
        ],
        dlt_pipelines_dir="some_dir",
    )

    assert "We have found <code>3</code> pipelines in local directory" in output.text
