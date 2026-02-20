from __future__ import annotations

from typing import TYPE_CHECKING

from dlt.common.exceptions import MissingDependencyException

try:
    import marimo
    import mowidgets
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dlt.helpers.marimo",
        [f"{version.DLT_PKG_NAME}[workspace]"],
    )

from dlt.helpers.marimo._load_package_viewer import app as load_package_viewer
from dlt.helpers.marimo._schema_viewer import app as schema_viewer
from dlt.helpers.marimo._pipeline_selector import app as pipeline_selector

if TYPE_CHECKING:
    from typing import Any


def render(app: marimo.App, *args: Any, **kwargs: Any) -> mowidgets.MoWidget:
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


def pipeline_selector_widget(app: marimo.App, *args: Any, **kwargs: Any) -> mowidgets.MoWidget:
    return mowidgets.widgetize(
        app,
        data_access=True,
        public_variables=["pipeline_path", "pipeline_name", "pipeline_locations"],
    )


def load_package_widget(
    app: marimo.App,
    pipeline_path: str,
    *args: Any,
    **kwargs: Any,
) -> mowidgets.MoWidget:
    return mowidgets.widgetize(app, data_access=True, inputs={"pipeline_path": pipeline_path})


def schema_viewer_widget(
    app: marimo.App,
    pipeline_name: str,
    *args: Any,
    **kwargs: Any,
) -> mowidgets.MoWidget:
    return mowidgets.widgetize(app, data_access=True, inputs={"pipeline_name": pipeline_name})


__all__ = (
    "render",
    "load_package_viewer",
    "schema_viewer",
    "pipeline_selector",
)
