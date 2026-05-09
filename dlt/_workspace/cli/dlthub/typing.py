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


class TRunJobInfo(TypedDict):
    """Resolved `workspace run` request — all data needed to launch the job."""

    job_ref: str
    display_label: str
    trigger: str
    trigger_humanized: str
    launcher: str
    run_id: str
    entry_point: Dict[str, Any]
    manifest_warnings: List[str]
    refresh_warning: NotRequired[str]
    profile_warning: NotRequired[str]
