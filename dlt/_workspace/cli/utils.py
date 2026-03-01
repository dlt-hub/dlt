import ast
import os
import shutil
from typing import Any, Callable, Dict, List, Optional, Tuple

import dlt
from dlt.common.pipeline import get_dlt_pipelines_dir
from dlt.common.time import ensure_pendulum_datetime_non_utc
from dlt.common.typing import TAnyDateTime, TFun
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs.pluggable_run_context import (
    PluggableRunContext,
    RunContextBase,
    ProfilesRunContext,
)
from dlt.common.configuration.specs.runtime_configuration import RuntimeConfiguration
from dlt.common.reflection.utils import set_ast_parents
from dlt.common.runtime import run_context
from dlt.common.runtime.exec_info import get_plus_version
from dlt.common.runtime.telemetry import with_telemetry
from dlt.common.storages.file_storage import FileStorage
from dlt.common.versioned_state import json_decode_state

from dlt.pipeline.pipeline import Pipeline
from dlt.pipeline.trace import get_trace_file_path
from dlt.reflection.script_visitor import PipelineScriptVisitor

from dlt._workspace.cli.exceptions import CliCommandException, CliCommandInnerException
from dlt._workspace.cli import echo as fmt
from dlt._workspace.helpers.dashboard.typing import TPipelineListItem
from dlt._workspace.typing import (
    ProviderInfo,
    ProviderLocationInfo,
    TLocationInfo,
    TLocationScope,
    TProfileInfo,
    TProviderInfo,
    TToolkitIndexEntry,
    TWorkspaceInfo,
)


REQUIREMENTS_TXT = "requirements.txt"
PYPROJECT_TOML = "pyproject.toml"
GITHUB_WORKFLOWS_DIR = os.path.join(".github", "workflows")
AIRFLOW_DAGS_FOLDER = os.path.join("dags")
AIRFLOW_BUILD_FOLDER = os.path.join("build")
MODULE_INIT = "__init__.py"
DATETIME_FORMAT = "YYYY-MM-DD HH:mm:ss"


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


def add_mcp_arg_parser(subparsers: Any, description: str, help_str: str, default_port: int) -> None:
    command_parser = subparsers.add_parser(
        "mcp",
        description=description,
        help=help_str,
    )
    command_parser.add_argument("--stdio", action="store_true", help="Use stdio transport mode")
    command_parser.add_argument(
        "--sse",
        action="store_true",
        help="Use legacy SSE transport instead of streamable-http",
    )
    command_parser.add_argument(
        "--port",
        type=int,
        default=default_port,
        help=f"Port for the MCP server (default: {default_port})",
    )
    command_parser.add_argument(
        "--features",
        nargs="*",
        default=None,
        help="Additional MCP feature sets to enable (default: pipeline, workspace)",
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


def fetch_workspace_info() -> TWorkspaceInfo:
    """Return workspace information as a structured dict.

    Works with both OSS RunContext (no profiles) and WorkspaceRunContext.
    Always includes all provider locations (verbose mode).
    """
    from dlt._workspace.cli.ai.utils import load_toolkits_index

    ctx = Container()[PluggableRunContext].context

    # profile info — only when profiles are available
    profile_info: Optional[TProfileInfo] = None
    if isinstance(ctx, ProfilesRunContext):
        from dlt._workspace.profile import read_profile_pin

        profile_info = TProfileInfo(
            name=ctx.profile,
            is_pinned=ctx.profile == read_profile_pin(ctx),
            data_dir=ctx.data_dir,
            local_dir=ctx.local_dir,
            configured_profiles=ctx.configured_profiles(),
        )

    # workspace name — only meaningful for WorkspaceRunContext
    name: Optional[str] = None
    if isinstance(ctx, ProfilesRunContext):
        name = ctx.name

    # provider locations — always verbose (all locations)
    providers: List[TProviderInfo] = []
    for info in get_provider_locations():
        providers.append(
            TProviderInfo(
                name=info.provider.name,
                is_empty=info.provider.is_empty,
                locations=[
                    TLocationInfo(
                        path=loc.path,
                        present=loc.present,
                        scope=loc.scope,
                        profile_name=loc.profile_name,
                    )
                    for loc in info.locations
                ],
            )
        )

    # dlt and dlthub versions
    plus_version = get_plus_version()
    dlthub_version: Optional[str] = plus_version["version"] if plus_version else None

    # initialized: config.toml exists in settings dir
    initialized = os.path.isfile(make_dlt_settings_path("config.toml"))

    # installed toolkits from local index
    installed_toolkits: Dict[str, TToolkitIndexEntry] = load_toolkits_index()

    return TWorkspaceInfo(
        name=name,
        run_dir=ctx.run_dir,
        settings_dir=ctx.settings_dir,
        global_dir=ctx.global_dir,
        profile=profile_info,
        providers=providers,
        dlt_version=dlt.__version__,
        dlthub_version=dlthub_version,
        initialized=initialized,
        installed_toolkits=installed_toolkits,
    )
