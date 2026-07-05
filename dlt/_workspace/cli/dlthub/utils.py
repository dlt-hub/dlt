"""dlthub-host-only loaders and local-data helpers."""

import os
import shutil
from typing import Any, Dict, List, Optional

import dlt
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.pluggable_run_context import (
    PluggableRunContext,
    ProfilesRunContext,
    RunContextBase,
)
from dlt.common.runtime.exec_info import get_plus_version
from dlt.common.storages.file_storage import FileStorage

from dlt._workspace.cli import echo as fmt
from dlt._workspace.cli.exceptions import CliCommandException
from dlt._workspace.cli.utils import (
    PYPROJECT_TOML,
    REQUIREMENTS_TXT,
    get_provider_locations,
    make_dlt_settings_path,
)
from dlt._workspace.cli.dlthub.ai.typing import TToolkitIndexEntry
from dlt._workspace.cli.dlthub.typing import (
    TCurrentProfileFullInfo,
    TCurrentProfileInfo,
    TDeploymentJobInfo,
    TDeploymentManifestInfo,
    TInitDependencyChoice,
    TInitDependencySystem,
    TInitFileEntry,
    TInitFileStatus,
    TInitPlan,
    TProfileInfo,
    TWorkspaceInfo,
)
from dlt._workspace.deployment import (
    DEFAULT_DEPLOYMENT_MODULE,
    humanize_trigger,
    manifest_from_module,
)
from dlt._workspace.deployment._job_ref import format_job_label
from dlt._workspace.deployment._trigger_helpers import parse_trigger
from dlt._workspace.deployment.exceptions import InvalidTrigger
from dlt._workspace.deployment.manifest import expand_triggers
from dlt._workspace.deployment.requirements import (
    get_workspace_install_specs,
    render_pep508,
    render_requirements_lines,
    render_uv_source,
)
from dlt._workspace.profile import BUILT_IN_PROFILES, is_local_profile, read_profile_pin
from dlt._workspace.typing import TLocationInfo, TProviderInfo


# kept in sync with the workspace deps that dlthub init seeds into the scaffolded project
# (was the `workspace` extra in pyproject.toml; mirrored in `[dependency-groups] workspace-deps`)
WORKSPACE_DEPS: List[str] = [
    "duckdb>=0.9",
    "ibis-framework>=12.0.0",
    "pyarrow>=16.0.0",
    "marimo>=0.14.5",
    "fastmcp>=3.0.0",
    "mowidgets>=0.2.1 ; python_version >= '3.11'",
    "pathspec>=0.11.2",
    "pydbml>=1.2.0",
    "croniter>=6.0.0",
    "s3fs>=2022.4.0",
]


def is_uv_available() -> bool:
    """Return `True` if the `uv` binary is on PATH."""
    return shutil.which("uv") is not None


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


def check_delete_local_data(run_context: RunContextBase, skip_local_data_dir: bool) -> List[str]:
    """Display paths to be deleted and ask for confirmation.

    Args:
        run_context: current run context.
        skip_local_data_dir: when True, preserve `local_dir` (locally loaded data, e.g. DuckDB)
            and only delete the pipelines working dir (`data_dir`).

    Returns:
        A list of run_context attribute names that should be deleted. Empty list if user cancels.

    Raises:
        CliCommandException: if context is invalid or deletion is not safe.
    """
    # ensure profiles context
    if not isinstance(run_context, ProfilesRunContext):
        fmt.error("Cannot delete local data for a context without profiles")
        raise CliCommandException()

    attrs: list[str] = []
    if not skip_local_data_dir:
        attrs.append("local_dir")
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


def fetch_profiles_list() -> List[TProfileInfo]:
    """Return all available profiles with their status flags.

    Works with ProfilesRunContext (workspace). Returns an empty list for OSS RunContext.
    """
    ctx = Container()[PluggableRunContext].context
    if not isinstance(ctx, ProfilesRunContext):
        return []

    pinned = read_profile_pin(ctx)
    current = ctx.profile
    configured = set(ctx.configured_profiles())

    return [
        TProfileInfo(
            name=name,
            description=BUILT_IN_PROFILES.get(name, "custom profile"),
            is_current=name == current,
            is_pinned=name == pinned,
            is_configured=name in configured,
            is_local=is_local_profile(name),
        )
        for name in ctx.available_profiles()
    ]


def fetch_workspace_info() -> TWorkspaceInfo:
    """Return workspace information as a structured dict.

    Works with both OSS RunContext (no profiles) and WorkspaceRunContext.
    Always includes all provider locations (verbose mode).
    """
    from dlt._workspace.cli.dlthub.ai.utils import load_toolkits_index

    ctx = Container()[PluggableRunContext].context

    # profile info — only when profiles are available
    profile_info: Optional[TCurrentProfileInfo] = None
    configured_profiles: List[str] = []
    if isinstance(ctx, ProfilesRunContext):
        configured_profiles = ctx.configured_profiles()
        profile_info = TCurrentProfileInfo(
            name=ctx.profile,
            description="",
            is_current=True,
            is_pinned=ctx.profile == read_profile_pin(ctx),
            is_configured=ctx.profile in configured_profiles,
            is_local=is_local_profile(ctx.profile),
            data_dir=ctx.data_dir,
            local_dir=ctx.local_dir,
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
        configured_profiles=configured_profiles,
        providers=providers,
        dlt_version=dlt.__version__,
        dlthub_version=dlthub_version,
        initialized=initialized,
        installed_toolkits=installed_toolkits,
    )


def fetch_profile_info() -> Optional[TCurrentProfileFullInfo]:
    """Returns the active profile's info plus filtered provider locations."""
    ctx = Container()[PluggableRunContext].context
    if not isinstance(ctx, ProfilesRunContext):
        return None

    configured_profiles = ctx.configured_profiles()
    pinned = read_profile_pin(ctx)

    # provider locations filtered to this profile + global. excludes other profiles'
    # toml files that fetch_workspace_info() returns indiscriminately.
    providers: List[TProviderInfo] = []
    for info in get_provider_locations():
        filtered = [
            TLocationInfo(
                path=loc.path,
                present=loc.present,
                scope=loc.scope,
                profile_name=loc.profile_name,
            )
            for loc in info.locations
            if loc.scope == "global" or loc.profile_name == ctx.profile
        ]
        if not filtered:
            continue
        providers.append(
            TProviderInfo(
                name=info.provider.name,
                is_empty=info.provider.is_empty,
                locations=filtered,
            )
        )

    return TCurrentProfileFullInfo(
        name=ctx.profile,
        description="",
        is_current=True,
        is_pinned=ctx.profile == pinned,
        is_configured=ctx.profile in configured_profiles,
        is_local=is_local_profile(ctx.profile),
        data_dir=ctx.data_dir,
        local_dir=ctx.local_dir,
        providers=providers,
        configured_profiles=configured_profiles,
    )


def fetch_deployment_info() -> TDeploymentManifestInfo:
    """Summarize the workspace deployment manifest."""

    try:
        manifest, _warnings = manifest_from_module(DEFAULT_DEPLOYMENT_MODULE)
    except ImportError as exc:
        default_file = os.path.join(os.getcwd(), f"{DEFAULT_DEPLOYMENT_MODULE}.py")
        # default deployment module not found
        if not os.path.isfile(default_file):
            return TDeploymentManifestInfo(status="not_found")
        # any other import error
        return TDeploymentManifestInfo(
            status="generation_failed", error=f"{type(exc).__name__}: {exc}"
        )
    except Exception as exc:
        return TDeploymentManifestInfo(
            status="generation_failed", error=f"{type(exc).__name__}: {exc}"
        )

    jobs_info: List[TDeploymentJobInfo] = []
    counts: Dict[str, int] = {}
    for job_def in manifest.get("jobs", []):
        expose = job_def.get("expose") or {}
        deliver = job_def.get("deliver") or {}
        entry_point = job_def["entry_point"]
        # category priority: explicit expose.category > delivers to a pipeline > job_type
        category: str
        if expose.get("category"):
            category = expose["category"]
        elif deliver.get("pipeline_name"):
            category = "pipeline"
        else:
            category = entry_point["job_type"]
        counts[category] = counts.get(category, 0) + 1

        expanded = expand_triggers(job_def)
        default = job_def.get("default_trigger")
        default_human: Optional[str] = None
        other_human: List[str] = []
        for trig in expanded:
            # skip manual: synthetic triggers in the summary
            try:
                if parse_trigger(trig).type == "manual":
                    continue
            except InvalidTrigger:
                pass
            humanized = humanize_trigger(trig)
            if trig == default and default_human is None:
                default_human = humanized
            else:
                other_human.append(humanized)

        entry: TDeploymentJobInfo = {
            "job_ref": job_def["job_ref"],
            "display_label": format_job_label(
                job_def["job_ref"],
                job_def.get("expose"),
                job_def.get("deliver"),
            ),
            "category": category,
            "triggers": other_human,
        }
        if default_human is not None:
            entry["default_trigger"] = default_human
        jobs_info.append(entry)

    return TDeploymentManifestInfo(
        status="ok",
        total_jobs=len(jobs_info),
        counts_by_category=counts,
        jobs=jobs_info,
    )


def _make_init_entry(path: str, *, accept_existing: bool, force: bool) -> TInitFileEntry:
    status: TInitFileStatus
    if os.path.exists(path):
        if accept_existing:
            status = "skip"
        elif force:
            status = "create"
        else:
            status = "conflict"
    else:
        status = "create"
    return TInitFileEntry(path=path, status=status, accept_existing=accept_existing)


def fetch_init_plan(
    run_dir: str,
    *,
    name: Optional[str] = None,
    force: bool = False,
    dependencies: TInitDependencyChoice = "auto",
) -> TInitPlan:
    """Build a plan describing what `dlthub init` would write."""
    # `dependencies` overrides uv detection: "pyproject" forces pyproject.toml,
    # "requirements" forces requirements.txt, "auto" uses uv-on-PATH as the signal
    abs_run_dir = os.path.abspath(run_dir)
    project_name = name or os.path.basename(abs_run_dir)
    uv = is_uv_available()
    if dependencies == "pyproject":
        dependency_system: TInitDependencySystem = PYPROJECT_TOML  # type: ignore[assignment]
    elif dependencies == "requirements":
        dependency_system = REQUIREMENTS_TXT  # type: ignore[assignment]
    else:
        dependency_system = PYPROJECT_TOML if uv else REQUIREMENTS_TXT  # type: ignore[assignment]

    # .workspace is overall marker if workspace exists or not
    keep_existing = not force
    workspace_marker = os.path.join(abs_run_dir, ".dlt", ".workspace")

    files: List[TInitFileEntry] = [
        _make_init_entry(
            os.path.join(abs_run_dir, dependency_system),
            accept_existing=keep_existing,
            force=force,
        ),
        _make_init_entry(
            os.path.join(abs_run_dir, ".gitignore"),
            accept_existing=keep_existing,
            force=force,
        ),
        _make_init_entry(
            os.path.join(abs_run_dir, ".dlt", "config.toml"),
            accept_existing=True,
            force=force,
        ),
        _make_init_entry(
            os.path.join(abs_run_dir, ".dlt", "secrets.toml"),
            accept_existing=True,
            force=force,
        ),
        _make_init_entry(workspace_marker, accept_existing=keep_existing, force=force),
    ]

    # reproduce dlt[hub]/dlthub/dlthub-client install modes (editable/path/git) in the
    # scaffold via uv.sources / `-e <path>` so a fresh `uv sync` matches the dev env
    workspace_specs = get_workspace_install_specs()
    dependency_specs: List[str] = [
        render_pep508(s, for_deployment=False) for s in workspace_specs
    ] + list(WORKSPACE_DEPS)
    uv_sources: Dict[str, Dict[str, Any]] = {}
    for s in workspace_specs:
        src = render_uv_source(s)
        if src is not None:
            uv_sources[s["name"]] = src
    requirements_lines: List[str] = []
    for s in workspace_specs:
        requirements_lines.extend(render_requirements_lines(s))
    requirements_lines.extend(WORKSPACE_DEPS)

    return TInitPlan(
        run_dir=abs_run_dir,
        project_name=project_name,
        dependency_system=dependency_system,
        uv_available=uv,
        dependency_specs=dependency_specs,
        uv_sources=uv_sources,
        requirements_lines=requirements_lines,
        workspace_deps=list(WORKSPACE_DEPS),
        files=files,
        workspace_exists=os.path.isfile(workspace_marker),
    )
