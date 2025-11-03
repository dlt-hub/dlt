import ast
import os
import shutil
from typing import Any, Callable, List

import dlt
from dlt.common.typing import TFun
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs.pluggable_run_context import (
    RunContextBase,
    ProfilesRunContext,
)
from dlt.common.configuration.specs.runtime_configuration import RuntimeConfiguration
from dlt.common.reflection.utils import set_ast_parents
from dlt.common.runtime import run_context
from dlt.common.runtime.telemetry import with_telemetry
from dlt.common.storages.file_storage import FileStorage

from dlt._workspace.cli.exceptions import CliCommandException, CliCommandInnerException
from dlt._workspace.cli import echo as fmt

from dlt.reflection.script_visitor import PipelineScriptVisitor

REQUIREMENTS_TXT = "requirements.txt"
PYPROJECT_TOML = "pyproject.toml"
GITHUB_WORKFLOWS_DIR = os.path.join(".github", "workflows")
AIRFLOW_DAGS_FOLDER = os.path.join("dags")
AIRFLOW_BUILD_FOLDER = os.path.join("build")
MODULE_INIT = "__init__.py"


def display_run_context_info() -> None:
    run_context = dlt.current.run_context()
    if isinstance(run_context, ProfilesRunContext):
        if run_context.default_profile != run_context.profile:
            # print warning
            fmt.echo(
                "Profile `%s` is active."
                % (fmt.style(run_context.profile, fg="yellow", reset=True),),
                err=True,
            )


def add_mcp_arg_parser(
    subparsers: Any, description: str, help_str: str, default_sse_port: int
) -> None:
    command_parser = subparsers.add_parser(
        "mcp",
        description=description,
        help=help_str,
    )
    command_parser.add_argument("--stdio", action="store_true", help="Use stdio transport mode")
    command_parser.add_argument(
        "--port",
        type=int,
        default=default_sse_port,
        help=f"SSE port to use (default: {default_sse_port})",
    )


def _may_safe_delete_local(run_context: RunContextBase, deleted_dir_type: str) -> bool:
    deleted_dir = getattr(run_context, deleted_dir_type)
    deleted_abs = os.path.abspath(deleted_dir)
    run_dir_abs = os.path.abspath(run_context.run_dir)
    settings_dir_abs = os.path.abspath(run_context.settings_dir)

    # never allow deleting run_dir or settings_dir themselves
    for ctx_abs, label in (
        (run_dir_abs, "run dir (workspace root)"),
        (settings_dir_abs, "settings dir"),
    ):
        if deleted_abs == ctx_abs:
            fmt.error(
                f"{deleted_dir_type} `{deleted_dir}` is the same as {label} and cannot be deleted"
            )
            return False

    # ensure deleted directory is inside run_dir
    try:
        common = os.path.commonpath([deleted_abs, run_dir_abs])
    except ValueError:
        # occurs when paths are on different drives on windows
        common = ""
    if common != run_dir_abs:
        fmt.error(
            f"{deleted_dir_type} `{deleted_dir}` is not within run dir (workspace root) and cannot"
            " be deleted"
        )
        return False

    return True


def _wipe_dir(
    run_context: RunContextBase, dir_attr: str, echo_template: str, recreate_dirs: bool = True
) -> None:
    """Echo, safely wipe and optionally recreate a directory from run context.

    Args:
        run_context: Current run context.
        dir_attr: Attribute name on the run context that holds the directory path, eg. "local_dir".
        echo_template: Template used to echo the action to the user. Must contain a single %s placeholder for the styled path.
        recreate_dirs: when True, recreate the directory after deletion.
    """
    dir_path = getattr(run_context, dir_attr, None)
    if not dir_path:
        raise CliCommandException()

    # ensure we never attempt to operate on run_dir or settings_dir
    if not _may_safe_delete_local(run_context, dir_attr):
        raise CliCommandException()

    # show relative path to the user when shorter
    display_dir = os.path.relpath(dir_path, ".")
    if len(display_dir) > len(dir_path):
        display_dir = dir_path

    fmt.echo(echo_template % fmt.style(display_dir, fg="yellow"))

    if os.path.exists(dir_path):
        shutil.rmtree(dir_path, onerror=FileStorage.rmtree_del_ro)

    if recreate_dirs:
        os.makedirs(dir_path, exist_ok=True)


def check_delete_local_data(run_context: RunContextBase, skip_data_dir: bool) -> List[str]:
    """Display paths to be deleted and ask for confirmation.

    Args:
        run_context: current run context.
        skip_data_dir: when True, do not include the data_dir in the deletion set.

    Returns:
        A list of run_context attribute names that should be deleted. Empty list if user cancels.

    Raises:
        CliCommandException: if context is invalid or deletion is not safe.
    """
    # ensure profiles context
    if not isinstance(run_context, ProfilesRunContext):
        fmt.error("Cannot delete local data for a context without profiles")
        raise CliCommandException()

    attrs: list[str] = ["local_dir"]
    if not skip_data_dir:
        attrs.append("data_dir")

    # ensure we never attempt to operate on run_dir or settings_dir
    for attr in attrs:
        if not _may_safe_delete_local(run_context, attr):
            raise CliCommandException()

    # display relative paths to run_dir
    fmt.echo("The following dirs will be deleted:")
    for attr in attrs:
        dir_path = getattr(run_context, attr)
        display_dir = os.path.relpath(dir_path, run_context.run_dir)
        if attr == "local_dir":
            template = "- %s (locally loaded data)"
        elif attr == "data_dir":
            template = "- %s (pipeline working folders)"
        else:
            raise ValueError(attr)

        fmt.echo(template % fmt.style(display_dir, fg="yellow", reset=True))

    # ask for confirmation
    if not fmt.confirm("Do you want to proceed?", default=False):
        return []

    return attrs


def delete_local_data(
    run_context: RunContextBase, dir_attrs: List[str], recreate_dirs: bool = True
) -> None:
    """Delete local data directories after explicit confirmation.

    Args:
        run_context: current run context.
        dir_attrs: A list of run_context attribute names that should be deleted.
        recreate_dirs: when True, recreate directories after deletion.
    """

    # delete selected directories
    for attr in dir_attrs:
        _wipe_dir(run_context, attr, "Deleting %s", recreate_dirs)


def parse_init_script(
    command: str, script_source: str, init_script_name: str
) -> PipelineScriptVisitor:
    # parse the script first
    tree = ast.parse(source=script_source)
    set_ast_parents(tree)
    visitor = PipelineScriptVisitor(script_source)
    visitor.visit_passes(tree)
    if len(visitor.mod_aliases) == 0:
        raise CliCommandInnerException(
            command,
            f"The pipeline script {init_script_name} does not import dlt and does not seem to run"
            " any pipelines",
        )

    return visitor


def ensure_git_command(command: str) -> None:
    try:
        import git
    except ImportError as imp_ex:
        if "Bad git executable" not in str(imp_ex):
            raise
        raise CliCommandInnerException(
            command,
            "'git' command is not available. Install and setup git with the following the guide %s"
            % "https://docs.github.com/en/get-started/quickstart/set-up-git",
            imp_ex,
        ) from imp_ex


def track_command(
    command: str, track_before: bool, *args: str, **kwargs: str
) -> Callable[[TFun], TFun]:
    """Return a telemetry decorator for CLI commands.

    Wraps a function with anonymous telemetry tracking using ``with_telemetry``. Depending on
    ``track_before``, emits an event either before execution or after execution with success
    information.

    Success semantics:
    - if the wrapped function returns an int, 0 is treated as success; other values as failure.
    - for non-int returns, success is True unless an exception is raised.

    Args:
        command: event/command name to report.
        track_before: if True, emit a single event before calling the function. if False,
            emit a single event after the call, including success state.
        *args: names of parameters from the decorated function whose values should be included
            in the event properties.
        **kwargs: additional key-value pairs to include in the event properties.

    Returns:
        a decorator that applies telemetry tracking to the decorated function.
    """
    return with_telemetry("command", command, track_before, *args, **kwargs)


def get_telemetry_status() -> bool:
    c = resolve_configuration(RuntimeConfiguration())
    return c.dlthub_telemetry


def make_dlt_settings_path(path: str = None) -> str:
    """Returns path to file in dlt settings folder. Returns settings folder if path not specified."""
    ctx = run_context.active()
    if not path:
        return ctx.settings_dir
    return ctx.get_setting(path)
