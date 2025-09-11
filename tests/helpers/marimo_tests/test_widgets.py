import marimo

import dlt.helpers.marimo


def test_available_widgets():
    expected_widgets = (
        "load_package_viewer",
        "schema_viewer",
        "local_pipelines_summary_viewer",
    )

    for expected_widget_name in expected_widgets:
        assert expected_widget_name in dlt.helpers.marimo.__all__
        widget = getattr(dlt.helpers.marimo, expected_widget_name, None)
        assert isinstance(widget, marimo.App)
