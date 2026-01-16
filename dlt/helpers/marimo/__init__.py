import marimo
import mowidgets

from dlt.helpers.marimo._load_package_viewer import app as load_package_viewer
from dlt.helpers.marimo._schema_viewer import app as schema_viewer
from dlt.helpers.marimo._pipeline_selector import app as pipeline_selector


def render(app: marimo.App, *args, **kwargs):
    if not isinstance(app, marimo.App):
        raise ValueError("app must be an instance of marimo.App")

    if app is load_package_viewer:
        return load_package_widget(*args, **kwargs)
    elif app is schema_viewer:
        return schema_viewer_widget(app, *args, **kwargs)
    elif app is pipeline_selector:
        return pipeline_selector_widget(app, *args, **kwargs)
    else:
        raise ValueError("app must be either load_package_viewer or schema_viewer")


def pipeline_selector_widget(app, *args, **kwargs):
    return mowidgets.widgetize(
        app,
        data_access=True,
        public_variables=["pipeline_path", "pipeline_name", "pipeline_locations"],
    )


def load_package_widget(app, pipeline_path: str, *args, **kwargs):
    return mowidgets.widgetize(app, data_access=True, inputs={"pipeline_path": pipeline_path})


def schema_viewer_widget(app, pipeline_name: str, *args, **kwargs):
    return mowidgets.widgetize(app, data_access=True, inputs={"pipeline_name": pipeline_name})


__all__ = (
    "render",
    "load_package_viewer",
    "schema_viewer",
    "pipeline_selector",
)
