import contextlib
import os
import sys
import subprocess
from importlib.resources import files
import time
from typing import Any, Iterator, List
from pathlib import Path
import urllib

from dlt.common.exceptions import MissingDependencyException


# keep this, will raise if user tries to run dashboard without dependencies
try:
    import marimo
    import pyarrow
    import ibis  # noqa: I251
except ModuleNotFoundError:
    raise MissingDependencyException(
        "Workspace Dashboard",
        ["dlt[workspace]"],
        "to install the dlt workspace extra.",
    )

from dlt._workspace.helpers.dashboard.const import EJECTED_APP_FILE_NAME, STYLE_FILE_NAME


def run_dashboard(
    pipeline_name: str = None,
    edit: bool = False,
    pipelines_dir: str = None,
    port: int = None,
    host: str = None,
    with_test_identifiers: bool = False,
    headless: bool = False,
) -> None:
    """Run dashboard blocked"""
    try:
        subprocess.run(
            run_dashboard_command(
                pipeline_name, edit, pipelines_dir, port, host, with_test_identifiers, headless
            )
        )
    except KeyboardInterrupt:
        pass


def _wait_http_up(url: str, timeout_s: float = 15.0, wait_on_ok: float = 0.1) -> None:
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            with urllib.request.urlopen(url, timeout=1.0):
                time.sleep(wait_on_ok)
                return
        except Exception:
            time.sleep(0.1)
    raise TimeoutError(f"Server did not become ready: {url}")


@contextlib.contextmanager
def start_dashboard(
    pipelines_dir: str = None,
    port: int = 2718,
    test_identifiers: bool = True,
    headless: bool = True,
    wait_on_ok: float = 1.0,
) -> Iterator[subprocess.Popen[bytes]]:
    """Launches dashboard in context manager that will kill it after use"""
    command = run_dashboard_command(
        pipeline_name=None,
        edit=False,
        pipelines_dir=pipelines_dir,
        port=port,
        with_test_identifiers=test_identifiers,
        headless=headless,
    )
    # start the dashboard process using subprocess.Popen
    proc = subprocess.Popen(command)
    try:
        _wait_http_up(f"http://localhost:{port}", timeout_s=60.0, wait_on_ok=wait_on_ok)
        yield proc
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def run_dashboard_command(
    pipeline_name: str = None,
    edit: bool = False,
    pipelines_dir: str = None,
    port: int = None,
    host: str = None,
    with_test_identifiers: bool = False,
    headless: bool = False,
) -> List[str]:
    """Creates cli command to run workspace dashboard"""
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
        with open(ejected_css_path, "w", encoding="utf-8") as f:
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

    if headless:
        dashboard_cmd.append("--headless")

    # collect marimo app CLI args (after the "--" separator)
    app_args: List[str] = []
    if pipeline_name:
        app_args += ["--pipeline", pipeline_name]
    if pipelines_dir:
        app_args += ["--pipelines-dir", pipelines_dir]
    if with_test_identifiers:
        app_args += ["--with_test_identifiers", "true"]
    if app_args:
        dashboard_cmd += ["--"] + app_args

    return dashboard_cmd
