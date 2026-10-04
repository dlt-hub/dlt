"""Run-orchestration types — shared between local and remote `run` / `serve`."""

from typing import Any, Dict, List, Literal

from dlt.common.typing import NotRequired, TypedDict


TRunLocation = Literal["local", "remote"]


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


class TRunBannerInfo(TypedDict):
    """Data shown in the unified `Starting <job> [local|remote] ...` banner."""

    display_label: str
    job_ref: str
    trigger: str
    trigger_humanized: str
    profile: str
    location: TRunLocation
    run_id: NotRequired[str]
    workspace_name: NotRequired[str]
    port: NotRequired[int]
