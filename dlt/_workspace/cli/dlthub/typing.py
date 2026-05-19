from typing import Any, Dict, List, Literal, Optional

from dlt.common.typing import NotRequired, TypedDict

from dlt._workspace.cli.dlthub.ai.typing import TToolkitIndexEntry
from dlt._workspace.typing import TProviderInfo


class TProfileInfo(TypedDict):
    """A single profile with its status flags."""

    name: str
    description: str
    is_current: bool
    is_pinned: bool
    is_configured: bool
    is_local: bool


class TCurrentProfileInfo(TProfileInfo):
    """The active profile, extending base with session directories."""

    data_dir: str
    local_dir: str


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
    default_trigger: NotRequired[str]
    triggers: List[str]


class TDeploymentManifestInfo(TypedDict):
    """Summary of the workspace deployment manifest."""

    status: TDeploymentManifestStatus
    error: NotRequired[str]
    total_jobs: NotRequired[int]
    counts_by_category: NotRequired[Dict[str, int]]
    jobs: NotRequired[List[TDeploymentJobInfo]]


TInitDependencySystem = Literal["pyproject.toml", "requirements.txt"]
TInitDependencyChoice = Literal["auto", "pyproject", "requirements"]
TInitFileStatus = Literal["create", "skip", "conflict"]


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
    uv_sources: Dict[str, Dict[str, Any]]
    """`[tool.uv.sources]` map; empty for pure-PyPI installs."""
    requirements_lines: List[str]
    """Full `requirements.txt` body, including `-e <path>` lines for editable installs."""
    workspace_deps: List[str]
    files: List[TInitFileEntry]
    workspace_exists: bool
