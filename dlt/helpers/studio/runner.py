import subprocess

from dlt.common.exceptions import MissingDependencyException

# keep this, will raise if user tries to run studio without dependencies
try:
    import marimo
    import ibis
    import pandas
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dlt Studio",
        ["marimo", "pandas", "ibis-framework"],
        "dlt Studio requires additional dependencies, such as marimo, pandas and ibis-framework.",
    )


def run_studio() -> None:
    from dlt.helpers.studio import app

    subprocess.run(["marimo", "run", app.__file__])
