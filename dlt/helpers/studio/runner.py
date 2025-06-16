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


def run_studio(pipeline_name: str = None) -> None:
    from dlt.helpers.studio import app

    studio_cmd = ["marimo", "run", app.__file__]

    if pipeline_name:
        studio_cmd.append("--")
        studio_cmd.append("--pipeline")
        studio_cmd.append(pipeline_name)

    subprocess.run(studio_cmd)
