import marimo as mo

# when adding widget, import the `app` variable and rename it to the widget name
from dlt.helpers.marimo._widgets._load_package_viewer import app as load_package_viewer
from dlt.helpers.marimo._widgets._schema_viewer import app as schema_viewer
from dlt.helpers.marimo._widgets._local_pipelines_summary_view import app as local_pipelines_summary_view


async def render(widget: mo.App) -> mo.Html:
    result = await widget.embed()
    return result.output
