from typing import Any, Dict, List, Literal, Optional

from dlt.common.typing import NotRequired, TypedDict

from dlt._workspace.cli.dlthub.ai.typing import TToolkitIndexEntry
from dlt._workspace.deployment.typing import TDependencyChange
from dlt._workspace.typing import (
    TEnvironmentDefinition,
    TProfileDefinition,
    TProfileOptionsTakes,
    TProviderInfo,
)


class TProfileOptions(TypedDict, total=False):
    """The profile options of a `dlthub` command, declared in `profile_options` of its
    `SupportsCliCommand`. A declaration replaces the one of the command above it, except one with
    `subcommands` only."""

    takes: TProfileOptionsTakes
    """`profile` when not given."""
    required: bool
    """The command needs `--profile`, `--environment` or `--workspace`."""
    refusal: str
    """Why a command that takes none of the options refuses `--profile`."""
    option_help: Dict[str, str]
    """Help of an option, by option string, that replaces its default help."""
    subcommands: Dict[str, "TProfileOptions"]
    """The subcommands that declare other options, by name."""


class TProfileInfo(TProfileDefinition):
    """A profile with its status flags."""

    name: str
    also_in_env: List[str]
    """The other profiles of the environment."""
    is_current: bool
    is_pinned: bool


class TEnvironmentInfo(TEnvironmentDefinition):
    """An environment with its remote workspace and the status of its profiles."""

    workspace_name: str
    """Name of the remote workspace of the environment. Until the local workspace is connected in
    the environment, dlt names it after the local workspace and the environment."""
    workspace_id: NotRequired[str]
    """Id of the remote workspace. Present when the local workspace is connected in the
    environment."""
    profile_infos: List[TProfileInfo]
    """The profiles of the environment, the default write profile first."""


class TCurrentProfileInfo(TProfileInfo):
    """The active profile with its session directories. For a profile of an environment, also the
    remote workspace and the deployment module of the environment."""

    data_dir: str
    local_dir: str
    workspace_name: NotRequired[str]
    workspace_id: NotRequired[str]
    deployment_module: NotRequired[str]
    """File of the deployment module of the environment."""


class TCurrentProfileFullInfo(TCurrentProfileInfo):
    """The active profile plus filtered provider locations for the verbose `profile info` view."""

    providers: List[TProviderInfo]
    configured_profiles: List[str]


class TWorkspaceInfo(TypedDict):
    """Full workspace state returned by `fetch_workspace_info`."""

    name: Optional[str]
    run_dir: str
    settings_dir: str
    global_dir: str
    profile: Optional[TCurrentProfileInfo]
    configured_profiles: List[str]
    providers: List[TProviderInfo]
    dlt_version: str
    dlthub_version: Optional[str]
    initialized: bool
    installed_toolkits: Dict[str, TToolkitIndexEntry]


TDeploymentManifestStatus = Literal["ok", "not_found", "generation_failed"]


class TDeploymentJobInfo(TypedDict):
    """A single job entry in the deployment manifest summary."""

    job_ref: str
    display_label: str
    category: str
    profile: NotRequired[str]
    """The profile that the job declares with `require`."""
    default_trigger: NotRequired[str]
    triggers: List[str]


class TDeploymentManifestInfo(TypedDict):
    """Summary of the workspace deployment manifest."""

    status: TDeploymentManifestStatus
    deployment_module: NotRequired[str]
    """File of the deployment module that dlt generated the manifest from, or did not find."""
    error: NotRequired[str]
    environment: NotRequired[str]
    """The environment that dlt generated the manifest for."""
    total_jobs: NotRequired[int]
    counts_by_category: NotRequired[Dict[str, int]]
    jobs: NotRequired[List[TDeploymentJobInfo]]


TInitDependencySystem = Literal["pyproject.toml", "requirements.txt"]
TInitDependencyChoice = Literal["auto", "pyproject", "requirements"]
TInitFileStatus = Literal["create", "skip", "merge", "overwrite"]
TInitMergeError = Literal[
    "uv_missing", "no_project_table", "hashed_requirements", "included_requirements"
]


class TInitFileEntry(TypedDict):
    """A file the `dlthub init` plan would touch."""

    path: str
    status: TInitFileStatus
    accept_existing: bool


class TInitPlan(TypedDict):
    """Plan for `dlthub init` — files to write and dependency seeds."""

    run_dir: str
    project_name: str
    dependency_system: TInitDependencySystem
    uv_available: bool
    dependency_specs: List[str]
    """`[project.dependencies]` array contents."""
    requires_python: str
    """`[project] requires-python`: the running Python and newer ones that dlt supports."""
    uv_sources: Dict[str, Dict[str, Any]]
    """`[tool.uv.sources]` map; empty for pure-PyPI installs."""
    requirements_lines: List[str]
    """Full `requirements.txt` body, including `-e <path>` lines for editable installs."""
    workspace_deps: List[str]
    files: List[TInitFileEntry]
    workspace_exists: bool
    gitignore_additions: List[str]
    """Required entries that an existing `.gitignore` lacks, written on merge."""
    dependency_changes: List[TDependencyChange]
    """Changes to the existing dependency file, written on merge."""
    merge_error: NotRequired[TInitMergeError]
    """Why the existing dependency file cannot take the dependency changes."""
