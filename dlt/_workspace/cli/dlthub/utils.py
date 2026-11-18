"""dlthub-host-only loaders and local-data helpers."""

import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

import tomlkit
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name

import dlt
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.pluggable_run_context import (
    PluggableRunContext,
    ProfilesRunContext,
    RunContextBase,
)
from dlt.common.runtime.exec_info import get_dlthub_version
from dlt.common.storages.file_storage import FileStorage
from dlt.version import get_requires_python

from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.cli import echo as fmt
from dlt._workspace.cli._pipeline_files import get_single_file_templates_storage
from dlt._workspace.cli.exceptions import CliCommandException
from dlt._workspace.cli.utils import (
    PYPROJECT_TOML,
    REQUIREMENTS_TXT,
    get_provider_locations,
    make_dlt_settings_path,
)
from dlt._workspace.cli.dlthub.ai.typing import TToolkitIndexEntry

if TYPE_CHECKING:
    from dlt._workspace.cli.dlthub.ai.agents import _AIAgent
from dlt._workspace.cli.dlthub.typing import (
    TCurrentProfileFullInfo,
    TCurrentProfileInfo,
    TDeploymentJobInfo,
    TDeploymentManifestInfo,
    TInitDependencyChoice,
    TInitDependencySystem,
    TInitFileEntry,
    TInitFileStatus,
    TInitMergeError,
    TInitPlan,
    TEnvironmentInfo,
    TProfileInfo,
    TWorkspaceInfo,
)
from dlt._workspace.deployment import (
    get_deployment_module_file,
    humanize_trigger,
    manifest_from_module,
)
from dlt._workspace.environment import (
    get_deployment_module_for_env,
    get_profiles_for_env,
    select_environment,
)
from dlt._workspace.deployment._job_ref import format_job_label, job_category
from dlt._workspace.deployment._trigger_helpers import parse_trigger
from dlt._workspace.deployment.exceptions import InvalidTrigger
from dlt._workspace.deployment.manifest import expand_triggers, is_module_missing
from dlt._workspace.deployment.requirements import (
    dlt_extra_specs,
    floored_spec,
    get_workspace_install_specs,
    is_uv_available,
    plan_dependency_file_changes,
    python_version,
    render_pep508,
    render_requirements_lines,
    render_uv_source,
)
from dlt._workspace.deployment.typing import TDependencyChange, TInstallSpec
from dlt._workspace.settings_files import read_profile_pin
from dlt._workspace.typing import DEFAULT_DEPLOYMENT_MODULE, TLocationInfo, TProviderInfo


def workspace_deps() -> List[str]:
    """Deps `dlthub init` seeds into the scaffolded project, mirrored in `[dependency-groups] workspace-deps`."""
    # packages the launchers also add carry the launcher floors and dlt's extra bounds
    return [
        "duckdb>=0.9,<2",
        floored_spec("ibis-framework"),
        dlt_extra_specs("parquet")["pyarrow"],
        floored_spec("marimo"),
        floored_spec("fastmcp"),
        "mowidgets>=0.2.1 ; python_version >= '3.11'",
        "pathspec>=0.11.2",
        "pydbml>=1.2.0",
        dlt_extra_specs("s3")["s3fs"],
    ]


GITIGNORE_FILE = ".gitignore"


def get_missing_gitignore_entries(existing: str, template: str) -> List[str]:
    """Patterns of the required block of the `.gitignore` `template` that `existing` lacks.

    The required block is the top of the template, up to the first empty line.
    """
    present = {line.strip() for line in existing.splitlines()}
    missing: List[str] = []
    for line in template.splitlines():
        pattern = line.strip()
        if not pattern:
            break
        if not pattern.startswith("#") and pattern not in present:
            missing.append(pattern)
    return missing


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


def _build_profile_info(ctx: WorkspaceRunContext, name: str) -> TProfileInfo:
    profile = ctx.get_profile_definition(name)
    env = profile.get("env")
    environment = ctx.get_environment_definition(env) if env else None
    info = TProfileInfo(
        **profile,
        name=name,
        also_in_env=(
            [n for n in get_profiles_for_env(environment) if n != name] if environment else []
        ),
        is_current=name == ctx.profile,
        is_pinned=name == read_profile_pin(ctx),
        is_configured=name in ctx.configured_profiles(),
    )
    # `is_configured` states the fact, `configured` also holds for a profile that needs no files
    info.pop("configured", None)
    return info


def fetch_profiles_list() -> List[TProfileInfo]:
    """Return all available profiles with their status flags.

    Works with WorkspaceRunContext. Returns an empty list for other contexts.
    """
    ctx = Container()[PluggableRunContext].context
    if not isinstance(ctx, WorkspaceRunContext):
        return []
    return [_build_profile_info(ctx, name) for name in ctx.available_profiles()]


def fetch_environments_list() -> List[TEnvironmentInfo]:
    """Returns the declared environments, each with its profiles.

    Works with WorkspaceRunContext. Returns an empty list for other contexts.
    """
    ctx = Container()[PluggableRunContext].context
    if not isinstance(ctx, WorkspaceRunContext):
        return []
    environments: List[TEnvironmentInfo] = []
    for name, environment in ctx.environment_definitions.items():
        config = ctx.get_environment_config(name)
        info = TEnvironmentInfo(
            **environment,
            workspace_name=config.settings.name,
            profile_infos=[_build_profile_info(ctx, p) for p in get_profiles_for_env(environment)],
        )
        info["deployment_module"] = get_deployment_module_file(
            config.deployment_module or DEFAULT_DEPLOYMENT_MODULE
        )
        if config.runtime.workspace_id:
            info["workspace_id"] = config.runtime.workspace_id
        environments.append(info)
    return environments


def _build_current_profile_info(ctx: WorkspaceRunContext) -> TCurrentProfileInfo:
    info = TCurrentProfileInfo(
        **_build_profile_info(ctx, ctx.profile), data_dir=ctx.data_dir, local_dir=ctx.local_dir
    )
    if ctx.environment:
        # as the default write profile resolves it, the same that `fetch_environments_list` shows
        config = ctx.get_environment_config(ctx.environment)
        info["workspace_name"] = config.settings.name
        info["deployment_module"] = get_deployment_module_file(
            config.deployment_module or DEFAULT_DEPLOYMENT_MODULE
        )
        if config.runtime.workspace_id:
            info["workspace_id"] = config.runtime.workspace_id
    return info


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
    name: Optional[str] = None
    if isinstance(ctx, WorkspaceRunContext):
        name = ctx.name
        configured_profiles = ctx.configured_profiles()
        profile_info = _build_current_profile_info(ctx)

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
    version_info = get_dlthub_version()
    dlthub_version: Optional[str] = version_info["version"] if version_info else None

    # initialized: config.toml exists in settings dir
    initialized = os.path.isfile(make_dlt_settings_path("config.toml"))

    installed_toolkits: Dict[str, TToolkitIndexEntry] = load_toolkits_index(hide_init=True)

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
    if not isinstance(ctx, WorkspaceRunContext):
        return None

    configured_profiles = ctx.configured_profiles()

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
        **_build_current_profile_info(ctx),
        providers=providers,
        configured_profiles=configured_profiles,
    )


def fetch_deployment_info() -> TDeploymentManifestInfo:
    """Summarize the workspace deployment manifest."""

    # summarizes a deployment to the environment of a remote profile, a local run otherwise
    ctx = Container()[PluggableRunContext].context
    environment = None
    if isinstance(ctx, WorkspaceRunContext) and ctx.environment:
        environment = select_environment(ctx.environment)
    module_file = get_deployment_module_file(get_deployment_module_for_env(environment))
    try:
        manifest, _warnings = manifest_from_module(module_file, environment)
    except Exception as exc:
        if isinstance(exc, ImportError) and is_module_missing(exc, module_file):
            return TDeploymentManifestInfo(status="not_found", deployment_module=module_file)
        return TDeploymentManifestInfo(
            status="generation_failed",
            deployment_module=module_file,
            error=f"{type(exc).__name__}: {exc}",
        )

    jobs_info: List[TDeploymentJobInfo] = []
    counts: Dict[str, int] = {}
    for job_def in manifest.get("jobs", []):
        expose = job_def.get("expose") or {}
        deliver = job_def.get("deliver") or {}
        entry_point = job_def["entry_point"]
        category = job_category(expose, deliver, entry_point["job_type"])
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
        if job_profile := (job_def.get("require") or {}).get("profile"):
            entry["profile"] = job_profile
        if default_human is not None:
            entry["default_trigger"] = default_human
        jobs_info.append(entry)

    info = TDeploymentManifestInfo(
        status="ok",
        deployment_module=module_file,
        total_jobs=len(jobs_info),
        counts_by_category=counts,
        jobs=jobs_info,
    )
    if manifest.get("environment"):
        info["environment"] = manifest["environment"]
    return info


def _make_init_entry(path: str, *, keep: bool) -> TInitFileEntry:
    status: TInitFileStatus
    if not os.path.exists(path):
        status = "create"
    elif keep:
        status = "skip"
    else:
        status = "overwrite"
    return TInitFileEntry(path=path, status=status, accept_existing=keep)


def _has_project_table(pyproject_path: str) -> bool:
    with open(pyproject_path, encoding="utf-8") as f:
        return "project" in tomlkit.parse(f.read())


def fetch_init_plan(
    run_dir: str,
    *,
    name: Optional[str] = None,
    overwrite: bool = False,
    skip: bool = False,
    dependencies: TInitDependencyChoice = "auto",
    agent: Optional["_AIAgent"] = None,
) -> TInitPlan:
    """Build a plan describing what `dlthub init` would write.

    `agent`, when given, decides which packages the workspace depends on. Without `overwrite` or
    `skip`, an existing `.gitignore` gets the missing required entries and an existing dependency
    file the missing or outdated dependencies.
    """
    # `dependencies` overrides uv detection: "pyproject" forces pyproject.toml,
    # "requirements" forces requirements.txt, "auto" uses uv-on-PATH as the signal
    abs_run_dir = os.path.abspath(run_dir)
    project_name = name or os.path.basename(abs_run_dir)
    merge = not (overwrite or skip)
    uv = is_uv_available()
    if dependencies == "pyproject":
        dependency_system: TInitDependencySystem = PYPROJECT_TOML  # type: ignore[assignment]
    elif dependencies == "requirements":
        dependency_system = REQUIREMENTS_TXT  # type: ignore[assignment]
    elif not skip and os.path.isfile(os.path.join(abs_run_dir, PYPROJECT_TOML)):
        dependency_system = PYPROJECT_TOML  # type: ignore[assignment]
    elif not skip and os.path.isfile(os.path.join(abs_run_dir, REQUIREMENTS_TXT)):
        dependency_system = REQUIREMENTS_TXT  # type: ignore[assignment]
    else:
        dependency_system = PYPROJECT_TOML if uv else REQUIREMENTS_TXT  # type: ignore[assignment]

    # .workspace is overall marker if workspace exists or not
    workspace_marker = os.path.join(abs_run_dir, ".dlt", ".workspace")
    dependency_path = os.path.join(abs_run_dir, dependency_system)
    gitignore_path = os.path.join(abs_run_dir, GITIGNORE_FILE)

    # settings files are always kept, the dlthub scaffold is replaced only with `overwrite`
    files: List[TInitFileEntry] = [
        _make_init_entry(dependency_path, keep=not overwrite),
        _make_init_entry(gitignore_path, keep=not overwrite),
        _make_init_entry(os.path.join(abs_run_dir, ".dlt", "config.toml"), keep=True),
        _make_init_entry(os.path.join(abs_run_dir, ".dlt", "secrets.toml"), keep=True),
        _make_init_entry(workspace_marker, keep=not overwrite),
    ]

    # reproduce dlt[hub]/dlthub/dlthub-client install modes (editable/path/git) in the
    # scaffold via uv.sources / `-e <path>` so a fresh `uv sync` matches the dev env
    workspace_specs = get_workspace_install_specs()
    deps = workspace_deps()
    if agent is not None:
        workspace_specs, deps = agent.workspace_python_dependencies(workspace_specs, deps)
    dependency_specs: List[str] = [
        render_pep508(s, for_deployment=False) for s in workspace_specs
    ] + deps
    uv_sources: Dict[str, Dict[str, Any]] = {}
    for s in workspace_specs:
        src = render_uv_source(s)
        if src is not None:
            uv_sources[s["name"]] = src
    requirements_lines: List[str] = []
    for s in workspace_specs:
        requirements_lines.extend(render_requirements_lines(s))
    requirements_lines.extend(deps)

    gitignore_additions: List[str] = []
    dependency_changes: List[TDependencyChange] = []
    merge_error: Optional[TInitMergeError] = None
    if merge and os.path.isfile(gitignore_path):
        with open(gitignore_path, encoding="utf-8") as f:
            existing_gitignore = f.read()
        gitignore_additions = get_missing_gitignore_entries(
            existing_gitignore, get_single_file_templates_storage().load(GITIGNORE_FILE)
        )
        if gitignore_additions:
            files[1]["status"] = "merge"
    if merge and os.path.isfile(dependency_path):
        # `uv add` needs a `[project]` table, a pyproject with tool settings only has none
        if dependency_system == PYPROJECT_TOML and not _has_project_table(dependency_path):
            merge_error = "no_project_table"
        else:
            dependency_changes = plan_dependency_file_changes(
                Path(dependency_path), dependency_specs, workspace_specs
            )
        if any(change["op"] != "newer" for change in dependency_changes):
            files[0]["status"] = "merge"
            if dependency_system == PYPROJECT_TOML and not uv:
                merge_error = "uv_missing"

    # like `uv init`: the running Python and newer ones, which the packages pinned from this
    # environment all support, up to the newest Python dlt supports
    dlt_upper_bounds = [
        str(s) for s in SpecifierSet(get_requires_python() or "") if s.operator in ("<", "<=")
    ]
    plan = TInitPlan(
        run_dir=abs_run_dir,
        project_name=project_name,
        dependency_system=dependency_system,
        uv_available=uv,
        dependency_specs=dependency_specs,
        requires_python=", ".join([f">={python_version()}", *dlt_upper_bounds]),
        uv_sources=uv_sources,
        requirements_lines=requirements_lines,
        workspace_deps=deps,
        files=files,
        workspace_exists=os.path.isfile(workspace_marker),
        gitignore_additions=gitignore_additions,
        dependency_changes=dependency_changes,
    )
    if merge_error:
        plan["merge_error"] = merge_error
    return plan
