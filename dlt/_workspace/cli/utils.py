import argparse
import ast
import os
import platform
import shutil
import subprocess
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple

import dlt
from dlt.common.pipeline import get_dlt_pipelines_dir
from dlt.common.schema import Schema
from dlt.common.storages.configuration import TSchemaFileFormat
from dlt.common.time import ensure_pendulum_datetime_non_utc
from dlt.common.typing import TAnyDateTime, TFun
from dlt.common.configuration.container import Container
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs.pluggable_run_context import (
    PluggableRunContext,
    ProfilesRunContext,
)
from dlt.common.configuration.specs.runtime_configuration import RuntimeConfiguration
from dlt.common.reflection.utils import set_ast_parents
from dlt.common.runtime import run_context
from dlt.common.runtime.telemetry import with_telemetry
from dlt.common.storages.file_storage import FileStorage
from dlt.common.versioned_state import json_decode_state

from dlt.pipeline.pipeline import Pipeline
from dlt.pipeline.trace import get_trace_file_path
from dlt.reflection.script_visitor import PipelineScriptVisitor

from dlt._workspace.cli.exceptions import CliCommandInnerException
from dlt._workspace.cli import echo as fmt
from dlt._workspace.helpers.dashboard.typing import TPipelineListItem
from dlt._workspace.typing import (
    ProviderInfo,
    ProviderLocationInfo,
    TLocationScope,
    TSchemaExport,
)
from dlt._workspace.profile import is_local_profile


REQUIREMENTS_TXT = "requirements.txt"
PYPROJECT_TOML = "pyproject.toml"
GITHUB_WORKFLOWS_DIR = os.path.join(".github", "workflows")

DEFAULT_MCP_FEATURES: FrozenSet[str] = frozenset(
    {"workspace", "pipeline", "toolkit", "secrets", "context"}
)
"""Default MCP feature set. Defined here (not in server.py) so argparsers can
reference it without importing fastmcp."""
AIRFLOW_DAGS_FOLDER = os.path.join("dags")
AIRFLOW_BUILD_FOLDER = os.path.join("build")
MODULE_INIT = "__init__.py"
DATETIME_FORMAT = "YYYY-MM-DD HH:mm:ss"


def is_hub_available() -> bool:
    # check if hub is connected
    from dlt import hub

    if not hub.__found__:
        fmt.warning("Install %s for workspace dashboard and mcp support" % fmt.bold("dlt[hub]"))
        return False
    return True


def get_pipeline_trace_mtime(pipelines_dir: str, pipeline_name: str) -> float:
    """Get mtime of the trace saved by pipeline, which approximates run time"""
    trace_file = get_trace_file_path(pipelines_dir, pipeline_name)
    if os.path.isfile(trace_file):
        return os.path.getmtime(trace_file)
    return 0


def _get_pipeline_initial_cwd(pipelines_dir: str, pipeline_name: str) -> Optional[str]:
    """Read initial_cwd from a pipeline's local state without attaching."""
    state_path = os.path.join(pipelines_dir, pipeline_name, Pipeline.STATE_FILE)
    try:
        with open(state_path, encoding="utf-8") as f:
            state = json_decode_state(f.read())
        local: Dict[str, Any] = state.get("_local", {})
        return local.get("initial_cwd")  # type: ignore[no-any-return]
    except (OSError, ValueError, KeyError):
        return None


def list_local_pipelines(
    pipelines_dir: str = None,
    sort_by_trace: bool = True,
    additional_pipelines: List[str] = None,
    run_dir: Optional[str] = None,
) -> Tuple[str, List[TPipelineListItem]]:
    """Get the local pipelines directory and the list of pipeline names in it.

    Args:
        pipelines_dir: The local pipelines directory. Defaults to get_dlt_pipelines_dir().
        sort_by_trace: Whether to sort the pipelines by the latest timestamp of trace.
        additional_pipelines: Extra pipeline names to include in the list.
        run_dir: When set, only return pipelines whose initial_cwd matches this path.
    """
    pipelines_dir = pipelines_dir or get_dlt_pipelines_dir()
    storage = FileStorage(pipelines_dir)

    try:
        pipelines = storage.list_folder_dirs(".", to_root=False)
    except Exception:
        pipelines = []

    if additional_pipelines:
        for pipeline in additional_pipelines:
            if pipeline and pipeline not in pipelines:
                pipelines.append(pipeline)

    if run_dir:
        abs_project_dir = os.path.abspath(run_dir)
        pipelines = [
            p for p in pipelines if _get_pipeline_initial_cwd(pipelines_dir, p) == abs_project_dir
        ]

    # check last trace timestamp and create dict
    pipelines_with_timestamps: List[TPipelineListItem] = []
    for pipeline in pipelines:
        pipelines_with_timestamps.append(
            {"name": pipeline, "timestamp": get_pipeline_trace_mtime(pipelines_dir, pipeline)}
        )

    if sort_by_trace:
        pipelines_with_timestamps.sort(key=lambda x: x["timestamp"], reverse=True)

    return pipelines_dir, pipelines_with_timestamps


def date_from_timestamp_with_ago(
    timestamp: TAnyDateTime, datetime_format: str = DATETIME_FORMAT
) -> str:
    """Return a date with ago section"""
    if not timestamp or timestamp == 0:
        return "never"
    timestamp = ensure_pendulum_datetime_non_utc(timestamp)
    time_formatted = timestamp.format(datetime_format)
    ago = timestamp.diff_for_humans()
    return f"{ago} ({time_formatted})"


def open_local_folder(folder: str) -> None:
    """Open a folder in the OS file explorer."""
    system = platform.system()
    if system == "Windows":
        os.startfile(folder)  # type: ignore[attr-defined,unused-ignore]
    elif system == "Darwin":
        subprocess.run(["open", folder], check=True)
    elif shutil.which("wslview"):
        subprocess.run(["wslview", folder], check=True)
    else:
        subprocess.run(["xdg-open", folder], check=True)


def open_url(url: str) -> None:
    """Open `url` in the default browser. WSL2-aware via wslview."""
    # wslview probed first: WSL reports platform.system()=="Linux", but xdg-open
    # would fail there with no in-WSL browser, so route to the Windows one.
    system = platform.system()
    try:
        if shutil.which("wslview"):
            subprocess.run(["wslview", url], check=True)
        elif system == "Windows":
            os.startfile(url)  # type: ignore[attr-defined,unused-ignore]
        elif system == "Darwin":
            subprocess.run(["open", url], check=True)
        elif shutil.which("xdg-open"):
            subprocess.run(["xdg-open", url], check=True)
        else:
            import webbrowser

            webbrowser.open(url)
    except Exception:
        # Headless / CI: callers already echoed the URL, so failure is non-fatal.
        pass


def display_run_context_info() -> None:
    run_context = dlt.current.run_context()
    if isinstance(run_context, ProfilesRunContext):
        # warn when active profile is not local-only — such profiles map to
        # data synced with dltHub, so a local destructive command can affect it
        if not is_local_profile(run_context.profile):
            fmt.echo(
                "Profile `%s` is active and is not a local-only profile — "
                "this command may read or write data synced to dltHub."
                % fmt.style(run_context.profile, fg="yellow", reset=True),
                err=True,
            )


def make_mcp_run_flags(default_port: int = 8000) -> argparse.ArgumentParser:
    """Build a parent parser with the shared MCP run flags (--stdio, --sse, --port, --features).

    Returns an ArgumentParser with `add_help=False` suitable for use as a
    `parents=[...]` entry.  Does **not** require `fastmcp` to be installed.
    """
    flags = argparse.ArgumentParser(add_help=False)
    flags.add_argument("--stdio", action="store_true", help="Use stdio transport mode")
    flags.add_argument(
        "--sse",
        action="store_true",
        help="Use legacy SSE transport instead of streamable-http",
    )
    flags.add_argument(
        "--port",
        type=int,
        default=default_port,
        help="Port for the MCP server (default: %d)" % default_port,
    )
    defaults = sorted(DEFAULT_MCP_FEATURES)
    flags.add_argument(
        "--features",
        nargs="*",
        default=None,
        help=(
            "MCP features to enable/disable. Default: %s. "
            "Use +name to add, -name to remove "
            "(e.g. --features=-secrets,+context)"
            % ", ".join(defaults)
        ),
    )
    return flags


def add_mcp_arg_parser(subparsers: Any, description: str, help_str: str, default_port: int) -> None:
    import importlib.util

    if importlib.util.find_spec("fastmcp") is None:
        return

    flags = make_mcp_run_flags(default_port)
    subparsers.add_parser(
        "mcp",
        description=description,
        help=help_str,
        parents=[flags],
    )


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
    command: str, track_before: bool, *args: str, **kwargs: Any
) -> Callable[[TFun], TFun]:
    """Return a telemetry decorator for CLI commands.

    Wraps a function with anonymous telemetry tracking using `with_telemetry`. Depending on
    `track_before`, emits an event either before execution or after execution with success
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
    # pass the function so the host is resolved at invocation time, not import time
    kwargs.setdefault("host", fmt.get_cli_host_name)
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


def get_provider_locations() -> List[ProviderInfo]:
    """Return structured info about all config providers and their locations.

    Works with both RunContext (OSS, no profiles) and WorkspaceRunContext
    (profile-aware). Always reflects the currently active profile.
    """
    ctx_plug = Container()[PluggableRunContext]
    ctx = ctx_plug.context
    providers = ctx_plug.providers.providers

    settings_dir = os.path.abspath(ctx.settings_dir)
    global_dir = os.path.abspath(ctx.global_dir)

    result: List[ProviderInfo] = []
    for provider in providers:
        profile: Optional[str] = getattr(provider, "_profile", None)
        present_set = set(os.path.abspath(p) for p in provider.present_locations)

        loc_infos: List[ProviderLocationInfo] = []
        for path in provider.locations:
            abs_path = os.path.abspath(path)
            is_present = abs_path in present_set

            # determine scope: project if under settings_dir, global otherwise
            if abs_path.startswith(settings_dir + os.sep) or abs_path == settings_dir:
                scope: TLocationScope = "project"
            elif abs_path.startswith(global_dir + os.sep) or abs_path == global_dir:
                scope = "global"
            else:
                scope = "global"

            # determine if this specific location is profile-scoped
            profile_name: Optional[str] = None
            if profile and os.path.basename(path).startswith(f"{profile}."):
                profile_name = profile

            loc_infos.append(ProviderLocationInfo(path, is_present, scope, profile_name))

        result.append(ProviderInfo(provider, loc_infos))
    return result


def fetch_schema_export(
    schema: Schema,
    format_: TSchemaFileFormat = "yaml",
    remove_defaults: bool = True,
    hide_columns: bool = False,
) -> TSchemaExport:
    """Export a schema object in the requested format."""
    if format_ == "json":
        content = schema.to_pretty_json(remove_defaults=remove_defaults)
    elif format_ == "yaml":
        content = schema.to_pretty_yaml(remove_defaults=remove_defaults)
    elif format_ == "dbml":
        content = schema.to_dbml()
    elif format_ == "dot":
        content = schema.to_dot()
    elif format_ == "mermaid":
        content = schema.to_mermaid(hide_columns=hide_columns)
    else:
        content = schema.to_pretty_yaml(remove_defaults=remove_defaults)
    return TSchemaExport(
        schema_name=schema.name,
        format_=format_,
        content=content,
    )
