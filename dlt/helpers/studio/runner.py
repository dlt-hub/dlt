import subprocess

from dlt.common.exceptions import MissingDependencyException

# keep this, will raise if user tries to run studio without dependencies
try:
    import marimo
except ModuleNotFoundError:
    raise MissingDependencyException(
        "dlt Studio",
        ["marimo"],
        "dlt Studio requires additional dependencies such as marimo and pyarrow.",
    )


def run_studio() -> None:
    from dlt.helpers.studio import app

    subprocess.run(["marimo", "run", app.__file__])
