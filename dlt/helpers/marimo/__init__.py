from __future__ import annotations

from typing import TYPE_CHECKING

from dlt.common.exceptions import MissingDependencyException

# when creating a widget, import the `app` variable and rename it to the widget name
from dlt.helpers.marimo._widgets._load_package_viewer import app as load_package_viewer
from dlt.helpers.marimo._widgets._schema_viewer import app as schema_viewer
from dlt.helpers.marimo._widgets._local_pipelines_summary_viewer import (
    app as local_pipelines_summary_viewer,
)

if TYPE_CHECKING:
    import marimo
    from mowidgets._widget import _MoWidgetBase


__all__ = (
    "load_package_viewer",
    "schema_viewer",
    "local_pipelines_summary_viewer",
)


def render(widget: marimo.App) -> _MoWidgetBase:
    try:
        import mowidgets

        return mowidgets.widgetize(widget, data_access=True)
    except ImportError:
        raise MissingDependencyException("dlt.helpers.marimo", ["marimo", "mowidgets"])
