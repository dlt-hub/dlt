import os
import sys
import subprocess
from importlib.resources import files
from typing import Any
from pathlib import Path
from dlt.common.exceptions import MissingDependencyException


def _detect_dashboard_command() -> str:
    command = sys.argv[1]
    if command == "pipeline":
        return f"dlt pipeline {sys.argv[2]} show"
    elif command == "dashboard":
        return "dlt dashboard"
    else:
        raise ValueError(f"Invalid command: {command}")


# keep this, will raise if user tries to run dashboard without dependencies
try:
    import marimo
    import pyarrow
    import ibis
except ModuleNotFoundError:
    raise MissingDependencyException(
        _detect_dashboard_command(),
        ['dlt["workspace"]'],
        "to install the dlt workspace extra.",
    )

EJECTED_APP_FILE_NAME = "dlt_dashboard.py"
STYLE_FILE_NAME = "dlt_dashboard_styles.css"


def run_dashboard(
    pipeline_name: str = None,
    edit: bool = False,
    pipelines_dir: str = None,
    port: int = None,
    host: str = None,
) -> None:
    from dlt._workspace.helpers.dashboard import dlt_dashboard

    ejected_app_path = os.path.join(os.getcwd(), EJECTED_APP_FILE_NAME)
    ejected_css_path = os.path.join(os.getcwd(), STYLE_FILE_NAME)
    ejected_app_exists = os.path.exists(ejected_app_path)

    # when editing, eject the app with styles to the cwd if not present already
    if edit and not ejected_app_exists:
        with open(dlt_dashboard.__file__, "r", encoding="utf-8") as f:
            app_code = f.read()
        with open(ejected_app_path, "w", encoding="utf-8") as f:
            f.write(app_code)
        css_file_path = Path(files("dlt._workspace.helpers.dashboard") / STYLE_FILE_NAME)  # type: ignore
        with open(css_file_path, "r", encoding="utf-8") as f:
            css_content = f.read()
        with open(os.path.join(os.getcwd(), ejected_css_path), "w", encoding="utf-8") as f:
            f.write(css_content)
        ejected_app_exists = True

    # set current pipeline
    cli_args: Any = {}
    if pipeline_name:
        cli_args["pipeline"] = pipeline_name

    # app file
    app_file_path = dlt_dashboard.__file__ if not ejected_app_exists else ejected_app_path

    dashboard_cmd = ["marimo", "run" if not edit else "edit", app_file_path]

    if port:
        dashboard_cmd.append("--port")
        dashboard_cmd.append(str(port))

    if host:
        dashboard_cmd.append("--host")
        dashboard_cmd.append(host)

    if pipeline_name:
        dashboard_cmd.append("--")
        dashboard_cmd.append("--pipeline")
        dashboard_cmd.append(pipeline_name)
    if pipelines_dir:
        dashboard_cmd.append("--")
        dashboard_cmd.append("--pipelines-dir")
        dashboard_cmd.append(pipelines_dir)

    try:
        subprocess.run(dashboard_cmd)
    except KeyboardInterrupt:
        pass
