from datetime import datetime  # noqa: I251
from typing import Any, Dict, List, Literal, NamedTuple, NewType, Optional, Union

from dlt.common.typing import NotRequired, TypedDict


MANIFEST_ENGINE_VERSION = 1

REQUIREMENTS_ENGINE_VERSION = 1

MAIN_GROUP = "main"
"""Conventional group name for top-level workspace dependencies."""

DEFAULT_DEPLOYMENT_MODULE = "__deployment__"
"""Default deployment module name for manifest generation."""

TJobRef = NewType("TJobRef", str)
"""Resolved job reference in `jobs.<section>.<name>` or `jobs.<name>` form."""

DASHBOARD_JOB_REF = TJobRef("jobs.workspace.dashboard")
"""job_ref of the synthesized dashboard; doubles as its requirements group name."""

TTrigger = NewType("TTrigger", str)
"""Normalized trigger string in `type:expr` form, e.g. `"schedule:0 8 * * *"`."""

TTriggerType = Literal[
    "schedule",
    "every",
    "once",
    "job.success",
    "job.fail",
    "http",
    "deployment",
    "webhook",
    "tag",
    "manual",
    "pipeline_name",
]

TFreshnessConstraint = NewType("TFreshnessConstraint", str)
"""Opaque freshness constraint string in `type:job_ref` form."""

TFreshnessType = Literal["job.is_matching_interval_fresh", "job.is_fresh"]
"""Constraint types for interval freshness checks — not triggers."""

TRefreshPolicy = Literal["always", "auto", "block"]
"""Refresh-signal policy of a job
- `auto` - if received, propagates refresh signal to downstream jobs
- `always` - sends refresh signal to downstream jobs when started
- `block` - blocks refresh signal, prevents downstream jobs from receiving it.
"""


class HttpTriggerInfo(NamedTuple):
    port: Optional[int]
    path: str


class TParsedTrigger(NamedTuple):
    type: TTriggerType  # noqa: A003
    expr: Union[None, str, float, datetime, HttpTriggerInfo]
    raw: TTrigger


TJobType = Literal["batch", "interactive", "stream"]
"""Execution model: batch (run to completion), interactive (long-running HTTP), stream (consumer loop)."""

TInterfaceType = Literal["gui", "rest_api", "mcp"]
"""What an interactive job exposes: web UI, programmatic API, or MCP tool server."""


TJobExposeCategory = Literal["pipeline", "mcp", "dashboard", "notebook"]
"""UI category for grouping jobs in the runtime interface."""


class TJobExposeSpec(TypedDict, total=False):
    """User-facing UI presentation metadata, accepted by decorators."""

    display_name: str
    """Human-friendly label shown in the UI. May contain spaces and punctuation."""
    tags: Union[str, List[str]]
    """Job tags that can also be used to trigger group of jobs."""
    starred: bool
    """Show in top-level runtime UI."""
    manual: bool
    """When `True` (default), runner creates a `manual:` trigger for this job."""


class TExposeSpec(TJobExposeSpec):
    """Extends TJobExposeSpec will elements not directly set by users."""

    interface: NotRequired[TInterfaceType]
    """What an interactive job exposes: `"gui"`, `"rest_api"`, or `"mcp"`."""
    category: NotRequired[TJobExposeCategory]
    """UI grouping category (e.g. `"pipeline"`, `"notebook"`)."""


class TRequireSpec(TypedDict, total=False):
    """Runner (machine, environment) requirements for a job."""

    dependency_groups: List[str]
    """PEP 735 groups to install on top of the workspace's `default_groups`."""
    profile: str
    """Workspace profile name to activate for this job."""
    provider: str
    """Infra provider identifier (e.g. `"modal"`). Runtime default when unset."""
    machine: str
    """Machine spec identifier (e.g. `"gpu-a100"`, `"2xlarge"`)."""
    region: str
    """Runner region for placement (e.g. `"us-east-1"`, `"eu-west"`)."""
    timezone: str
    """IANA timezone for cron ticks and intervals (e.g. `"America/New_York"`). Default: UTC."""


class TEntryPoint(TypedDict):
    """How to invoke the job code."""

    module: str
    """Python module path relative to workspace root."""
    function: Optional[str]
    """Function name within the module, or `None` for module-level jobs."""
    job_type: TJobType
    launcher: str
    """Fully qualified launcher module path."""


class TRunArgs(TypedDict, total=False):
    """Runtime-supplied arguments for launching a job.

    Provided by the runtime when invoking a launcher. Not part of the
    deployment manifest — filled in at launch time.
    """

    port: int
    """HTTP port assigned by the runtime for interactive jobs."""
    base_path: str
    """Reverse proxy subpath prefix (e.g. `"/workspace/123/notebook"`)."""


class TIntervalSpec(TypedDict):
    """Overall time range for interval-based job scheduling.

    Stored in the manifest. Together with the job's cron schedule, defines
    the set of discrete intervals to process.
    """

    start: Union[str, datetime]
    """ISO 8601 string or `datetime` for the start of the range. Required."""
    end: NotRequired[Union[str, datetime]]
    """ISO 8601 string or `datetime` for the end of the range. Defaults to now."""


class TJobRunContext(TypedDict):
    """Job run context injected into job functions that declare a `run_context` argument."""

    run_id: str
    """Unique identifier for this job run."""
    trigger: TTrigger
    """The trigger string that fired this run."""
    run_args: NotRequired[TRunArgs]
    """Runtime-provided launch arguments (port, base_path), if available."""
    interval_start: NotRequired[datetime]
    """Start of the interval being processed."""
    interval_end: NotRequired[datetime]
    """End of the interval being processed."""
    refresh: bool
    """Refresh signal with request to refresh (reload) the data"""


class TRuntimeEntryPoint(TEntryPoint):
    """Entry point enriched with runtime-assigned launch arguments."""

    run_args: NotRequired[TRunArgs]
    interval_start: NotRequired[str]
    """ISO 8601 UTC start of the interval being processed."""
    interval_end: NotRequired[str]
    """ISO 8601 UTC end of the interval being processed."""
    interval_timezone: NotRequired[str]
    """IANA timezone name (from `require.timezone`); applied to the interval by the launcher."""
    allow_external_schedulers: NotRequired[bool]
    """Propagated from `TJobDefinition.allow_external_schedulers`. Tells the
    launcher whether to inject `TimeIntervalContext(allow_external_schedulers=True)`
    so dlt incrementals join the runner-provided interval without per-resource opt-in."""
    profile: NotRequired[str]
    """Active workspace profile, resolved from require.profile."""
    config: NotRequired[Dict[str, Any]]
    """Config key-value pairs injected as env vars before job execution."""
    refresh: NotRequired[bool]
    """Refresh signal with request to refresh (reload) the data"""


class TTimeoutSpec(TypedDict):
    """Expanded timeout specification stored in the manifest."""

    timeout: NotRequired[Union[int, float]]
    """Maximum duration of the job after which it will be terminated."""
    grace_period: NotRequired[Union[int, float]]
    """Seconds for graceful shutdown after sending termination signal."""


class TExecuteSpec(TypedDict):
    """Runtime execution constraints for a job."""

    timeout: NotRequired[Optional[TTimeoutSpec]]
    concurrency: NotRequired[Optional[int]]
    """Max concurrent runs. Default `1` for both batch and interactive jobs.
    Pass any positive integer to allow that many concurrent instances, or
    explicitly `None` to remove the limit."""


class TDeliverSpec(TypedDict, total=False):
    """Associates a job with a data delivery target."""

    source_ref: str
    """Source rel_ref, e.g. `"sources.job_runs.chat_analytics"`."""
    pipeline_name: str
    """Pipeline name this job operates on."""
    deadline: str
    """Human-readable delivery deadline, e.g. `"8am on Mondays"`."""


class TJobDefinition(TypedDict):
    """A single job in the deployment manifest."""

    job_ref: TJobRef
    """Unique job identity: `"jobs.<section>.<name>"` or `"jobs.<name>"` for module-level jobs."""
    description: NotRequired[str]
    entry_point: TEntryPoint
    expose: NotRequired[TExposeSpec]
    """UI presentation and scheduling metadata."""
    triggers: List[TTrigger]
    """When to run: 0, 1, or many trigger strings."""
    execute: TExecuteSpec
    config_keys: NotRequired[List[str]]
    """Config keys discovered from function signature (`dlt.config.value` defaults)."""
    deliver: NotRequired[TDeliverSpec]
    interval: NotRequired[TIntervalSpec]
    """Overall time range for interval-based scheduling."""
    freshness: NotRequired[List[TFreshnessConstraint]]
    """Upstream freshness constraints for interval eligibility."""
    allow_external_schedulers: NotRequired[bool]
    """When `True`, intervals and state are managed by the scheduler."""
    require: NotRequired[TRequireSpec]
    """Runtime resource requirements."""
    default_trigger: NotRequired[TTrigger]
    """Primary trigger, computed during manifest generation. Prefers schedule/every triggers."""
    refresh: NotRequired[TRefreshPolicy]
    """Controls refresh policy of the job"""


class TDeploymentFileItem(TypedDict, total=False):
    """A file in the deployment package."""

    relative_path: str
    size_in_bytes: int
    sha3_256: str
    """Set for regular file entries only."""
    linkname: str
    """Set for symlink entries only."""


class TFilesManifest(TypedDict):
    """Base manifest for file sync. Used by PackageBuilder."""

    engine_version: int
    files: List[TDeploymentFileItem]


class TJobsDeploymentManifest(TypedDict):
    """Full deployment manifest with job definitions."""

    engine_version: int
    version: NotRequired[int]
    """Auto-incremented on content change."""
    version_hash: NotRequired[str]
    """SHA3-256 content hash, excluding version metadata."""
    previous_hashes: NotRequired[List[str]]
    """Last 10 version hashes for history."""
    created_at: str
    """ISO 8601 timestamp of manifest creation."""
    deployment_module: str
    """Module name, default `"__deployment__"`."""
    description: NotRequired[str]
    """First line of the deployment module's `__doc__`."""
    tags: NotRequired[List[str]]
    """From `__tags__` of the deployment module."""
    jobs: List[TJobDefinition]


class TWorkspaceRequirementsManifest(TypedDict):
    """Workspace Python dependencies exported per dependency group."""

    engine_version: int
    python_version: str
    """Client-side `major.minor` Python version captured at export time."""
    default_groups: List[str]
    """Groups installed on top of any job-declared `dependency_groups`."""
    groups: Dict[str, List[str]]
    """Group name to sorted PEP 508 specs. Always contains `"main"`."""
    launcher_requirements: Dict[str, List[str]]
    """Launcher module path to mandatory specs installed for jobs of that launcher."""
