"""Deployment manifest types: the contract between workspace and runtime.

The manifest is produced by `dlt workspace deploy` (which imports __deployment__.py,
discovers jobs, and serializes them) and consumed by the runtime API (which upserts
jobs, reconciles triggers, and soft-deletes removed jobs).

Reference scheme:
    rel_ref := jobs|sources.<section>.<name>
    abs_ref := <workspace>.jobs|sources.<section>.<name>
"""

from typing import Any, Dict, List, Literal, NewType, Optional

from typing_extensions import NotRequired, TypedDict


MANIFEST_ENGINE_VERSION = 2

DEFAULT_HTTP_PORT = 5000

TTrigger = NewType("TTrigger", str)
"""Normalized trigger string in `type:expr` form, e.g. `"schedule:0 8 * * *"`."""

TJobType = Literal["batch", "interactive", "stream"]
"""Execution model: batch (run to completion), interactive (long-running HTTP), stream (consumer loop)."""

TInterfaceType = Literal["gui", "rest_api", "mcp"]
"""What an interactive job exposes: web UI, programmatic API, or MCP tool server."""


class TExposeSpec(TypedDict, total=False):
    """How the runtime reaches and presents a running interactive job."""

    interface: TInterfaceType
    port: int
    """HTTP port the job listens on."""
    run_params: Dict[str, Any]
    """Framework-specific parameters for the runtime (e.g. WebSocket port)."""


class TEntryPoint(TypedDict):
    """How to invoke the job code."""

    module: str
    """Python module path relative to workspace root."""
    function: Optional[str]
    """Function name within the module, or `None` for module-level jobs."""
    job_type: TJobType
    launcher: NotRequired[Optional[str]]
    """Fully qualified launcher module path, or `None` for system default."""
    expose: NotRequired[TExposeSpec]
    """Exposure configuration for interactive jobs."""


class TTimeoutSpec(TypedDict):
    """Expanded timeout specification stored in the manifest."""

    timeout: float
    """Max wall-clock duration in seconds."""
    grace_period: NotRequired[float]
    """Seconds for graceful shutdown before hard kill. Default: 30."""


class TExecutionSpec(TypedDict):
    """Runtime execution constraints for a job."""

    timeout: NotRequired[Optional[TTimeoutSpec]]
    concurrency: NotRequired[int]
    """Max concurrent runs of this job. Default: 1."""


class TDeliveryRef(TypedDict):
    """Associates a job with a data source it is supposed to deliver."""

    source_ref: str
    """Source rel_ref, e.g. `"sources.job_runs.chat_analytics"`."""
    deadline: NotRequired[str]
    """Human-readable delivery deadline, e.g. `"8am on Mondays"`."""


class TDeliverySpec(TypedDict):
    """Links a data source to its delivery deadline and responsible jobs."""

    source_ref: str
    """Source rel_ref, e.g. `"sources.job_runs.chat_analytics"`."""
    deadline: str
    """Human-readable delivery deadline."""
    job_refs: List[str]
    """Job rel_refs that deliver this source."""


class TJobDefinition(TypedDict):
    """A single job in the deployment manifest."""

    job_ref: str
    """Unique job identity: `"jobs.<section>.<name>"`."""
    display_name: NotRequired[str]
    """Inferred by launcher when possible (e.g. notebook title from marimo)."""
    description: NotRequired[str]
    entry_point: TEntryPoint
    triggers: List[TTrigger]
    """When to run: 0, 1, or many trigger strings."""
    execution: TExecutionSpec
    config_keys: NotRequired[List[str]]
    """Config keys discovered from function signature (`dlt.config.value` defaults)."""
    deliver: NotRequired[TDeliveryRef]
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
