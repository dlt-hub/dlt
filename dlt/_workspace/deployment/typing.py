from typing import List, Literal, NamedTuple, NewType, Optional, Union

from dlt.common.pendulum import pendulum
from dlt.common.typing import NotRequired, TypedDict


MANIFEST_ENGINE_VERSION = 2

TJobRef = NewType("TJobRef", str)
"""Resolved job reference in `jobs.<section>.<name>` or `jobs.<name>` form."""

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
]


class HttpTriggerInfo(NamedTuple):
    port: Optional[int]
    path: str


class TParsedTrigger(NamedTuple):
    type: TTriggerType  # noqa: A003
    expr: Union[None, str, float, pendulum.DateTime, HttpTriggerInfo]
    raw: TTrigger


TJobType = Literal["batch", "interactive", "stream"]
"""Execution model: batch (run to completion), interactive (long-running HTTP), stream (consumer loop)."""

TInterfaceType = Literal["gui", "rest_api", "mcp"]
"""What an interactive job exposes: web UI, programmatic API, or MCP tool server."""


class TExposeSpec(TypedDict, total=False):
    """How the runtime presents a running interactive job."""

    interface: TInterfaceType


class TEntryPoint(TypedDict):
    """How to invoke the job code."""

    module: str
    """Python module path relative to workspace root."""
    function: Optional[str]
    """Function name within the module, or `None` for module-level jobs."""
    job_type: TJobType
    launcher: NotRequired[Optional[str]]
    """Fully qualified launcher module path, or `None` for system default."""


class TRunArgs(TypedDict, total=False):
    """Runtime-supplied arguments for launching a job.

    Provided by the runtime when invoking a launcher. Not part of the
    deployment manifest — filled in at launch time.
    """

    port: int
    """HTTP port assigned by the runtime for interactive jobs."""
    base_path: str
    """Reverse proxy subpath prefix (e.g. `"/workspace/123/notebook"`)."""


class TJobRunContext(TypedDict):
    """Runtime context injected into job functions that declare a `run_context` parameter.

    Built by the launcher at run time — never stored in the manifest.
    """

    run_id: str
    """Unique identifier for this job run."""
    trigger: TTrigger
    """The trigger string that fired this run."""
    run_args: NotRequired[TRunArgs]
    """Runtime-provided launch arguments (port, base_path), if available."""


class TRuntimeEntryPoint(TEntryPoint):
    """Entry point enriched with runtime-assigned launch arguments.

    Extends TEntryPoint with `run_args` that the runtime provides when
    invoking the launcher. This is what launchers receive — not stored
    in the deployment manifest.
    """

    run_args: NotRequired[TRunArgs]


class TTimeoutSpec(TypedDict):
    """Expanded timeout specification stored in the manifest."""

    timeout: NotRequired[float]
    """Max wall-clock duration in seconds."""
    grace_period: NotRequired[float]
    """Seconds for graceful shutdown before hard kill. Default: 30 (batch), 5 (interactive)."""


class TExecutionSpec(TypedDict):
    """Runtime execution constraints for a job."""

    timeout: NotRequired[Optional[TTimeoutSpec]]
    concurrency: NotRequired[Optional[int]]
    """Max concurrent runs. None = no limit (batch default), 1 = single instance (interactive default)."""


class TDeliveryRef(TypedDict):
    """Associates a job with a data source it is supposed to deliver."""

    source_ref: str
    """Source rel_ref, e.g. `"sources.job_runs.chat_analytics"`."""
    deadline: NotRequired[str]
    """Human-readable delivery deadline, e.g. `"8am on Mondays"`."""


class TDeliverySpec(TDeliveryRef):
    """Links a data source to its delivery deadline and responsible jobs."""

    job_refs: List[TJobRef]
    """Job rel_refs that deliver this source."""


class TJobDefinition(TypedDict):
    """A single job in the deployment manifest."""

    job_ref: TJobRef
    """Unique job identity: `"jobs.<section>.<name>"` or `"jobs.<name>"` for module-level jobs."""
    display_name: NotRequired[str]
    """Inferred by launcher when possible (e.g. notebook title from marimo)."""
    description: NotRequired[str]
    entry_point: TEntryPoint
    expose: NotRequired[TExposeSpec]
    """Exposure configuration for interactive jobs."""
    triggers: List[TTrigger]
    """When to run: 0, 1, or many trigger strings."""
    execution: TExecutionSpec
    config_keys: NotRequired[List[str]]
    """Config keys discovered from function signature (`dlt.config.value` defaults)."""
    deliver: NotRequired[TDeliveryRef]
    manual_disabled: NotRequired[bool]
    """When `True`, manifest generation skips adding the automatic `manual:` trigger."""
    starred: bool
    """Show in top-level runtime UI."""
    tags: NotRequired[List[str]]


class TDeploymentFileItem(TypedDict, total=False):
    """A file in the deployment package."""

    relative_path: str
    size_in_bytes: int
    sha3_256: str


class TFilesManifest(TypedDict):
    """Base manifest for file sync. Used by PackageBuilder."""

    engine_version: int
    files: List[TDeploymentFileItem]


class TDeploymentManifest(TFilesManifest):
    """Full deployment manifest with job definitions.

    Extends TFilesManifest with deployment module metadata, job definitions,
    and delivery specs. Produced by importing `__deployment__.py` and running
    launcher detectors. The runtime reconciles jobs from this manifest.
    """

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
    """From `__doc__` of the deployment module."""
    tags: NotRequired[List[str]]
    """From `__tags__` of the deployment module."""
    jobs: List[TJobDefinition]
    delivery_specs: NotRequired[List[TDeliverySpec]]
