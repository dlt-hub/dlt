"""Deployment manifest spec: data types for the contract between workspace and runtime.

The manifest is produced by `dlt workspace deploy` (which imports __deployment__.py,
discovers jobs, and serializes them) and consumed by the runtime API (which upserts
jobs, reconciles triggers, and soft-deletes removed jobs).

Design decisions:
    - Triggers are canonical strings, not typed objects. Both sides share a grammar
      that round-trips unambiguously: string -> parse -> string.
    - No control-plane retries. Retries live in user code via `runner()`.
    - Soft delete on removal (job becomes inactive, history preserved).
      Hard delete only via explicit user action.
    - One deployment module per workspace. Jobs are unique by ref.

Job types vs launchers:
    Job type determines the execution model (batch, interactive, stream).
    Launcher is an optional convenience wrapper for a specific framework
    (marimo, mcp, streamlit). Launchers are implemented in
    dlt._workspace.deployment.launchers.<name> and handle framework-specific concerns
    (port, flags, ASGI wiring, transport config). The system launcher (None)
    imports the module and calls JobFactory or runs __main__.

Reference scheme:
    Sources and jobs share a unified naming scheme.

    rel_ref := jobs|sources.<section>.<name>
    abs_ref := <workspace>.jobs|sources.<section>.<name>

    where:
        workspace: from pyproject name, or folder name by default
        section:   module name (not fully qualified) by default
        name:      function name by default

    The manifest uses rel_ref. rel_ref doubles as config layout path.

    Shorthands: the jobs./sources. prefix can be omitted in CLI and trigger
    strings when unambiguous. The manifest always stores the full rel_ref.
"""

from typing import Any, Dict, List, Literal, NewType, Optional

from typing_extensions import NotRequired, TypedDict


MANIFEST_ENGINE_VERSION = 2

# -- trigger grammar ---------------------------------------------------------
#
# Triggers are strings with a normalized form:  type:expr
#
#   schedule:cron_expr          "schedule:0 8 * * *"
#   every:human_period          "every:5h"
#   once:iso_timestamp          "once:2026-03-15T08:00:00Z"
#   job.success:job_ref         "job.success:jobs.job_runs.backfil_chats"
#   job.fail:job_ref            "job.fail:jobs.job_runs.backfil_chats"
#   http:port                   "http:5000"  (interactive: start and expose on port)
#   deployment:                 "deployment:"  (fires after new code is deployed)
#   webhook:path                "webhook:ingest/chat"
#   tag:tag_name                "tag:backfill"
#
# Shorthands (accepted as input, normalized before storing in manifest):
#   "0 8 * * *"                 -> "schedule:0 8 * * *"  (auto-detect 5 cron fields)
#   "2026-03-15T08:00:00Z"     -> "once:2026-03-15T08:00:00Z"  (auto-detect ISO timestamp)
#   "http"                      -> "http:5000"  (default port)
#   "http:9090"                 -> "http:9090"  (explicit port)
#   "deployment"                -> "deployment:"
#   "webhook"                   -> "webhook:"
#
# The manifest always stores the normalized form.
#
# Server-side validation:
#   - job.success/job.fail triggers referencing non-existing
#     job_refs must be flagged as validation errors.
#
# All triggers can be enabled/disabled by the runtime without modifying
# the manifest. The manifest carries the declared set; runtime tracks state.

TTrigger = NewType("TTrigger", str)

DEFAULT_HTTP_PORT = 5000

# -- job types ---------------------------------------------------------------

TJobType = Literal["batch", "interactive", "stream"]
"""Job type determines the execution model.

    batch:       call once, run to completion, exit
    interactive: start process, keep it running, expose HTTP
    stream:      runtime manages consumer loop, calls function with micro-batches
"""

# -- interface types ---------------------------------------------------------

TInterfaceType = Literal["gui", "rest_api", "mcp"]
"""What the interactive job exposes to users.

    gui:      web UI (marimo notebook, streamlit dashboard, flask app, etc.)
              use tags on the job to hint display style: "notebook", "dashboard", "webapp"
    rest_api: programmatic HTTP API
    mcp:      MCP tool server (streamable HTTP transport)
"""


# -- expose spec (interactive jobs only) ------------------------------------


class TExposeSpec(TypedDict, total=False):
    """How the runtime reaches and presents a running interactive job.

    For auto-detected launchers (marimo, mcp, streamlit), the launcher
    detector fills in defaults. For custom interactive jobs, the user
    provides interface via the decorator or config.

    Port is typically derived from the http trigger ("http:9090" -> port 9090).
    When set explicitly here, it overrides the trigger-derived port.
    """

    interface: TInterfaceType
    port: int


# -- entry point -------------------------------------------------------------


class TEntryPoint(TypedDict):
    """How to invoke the job code.

    module:   Python module path relative to workspace root.
    function: function name within the module, or None for module-level jobs.
    job_type: execution model (batch, interactive, stream).
    launcher: fully qualified Python module path for the launcher, or None
              for the system default launcher.

    System launcher (None):
        batch + function     -> import module, call function
        batch + null         -> run module as __main__ (python -m module)
        interactive + func   -> import module, call function (long-running)
        interactive + null   -> import module, run as __main__ (long-running)

    Pre-defined launchers:
        dlt._workspace.deployment.launchers.marimo
        dlt._workspace.deployment.launchers.mcp
        dlt._workspace.deployment.launchers.streamlit

    All launchers are invocable via: python -m <launcher> <job-context-args>

    Launchers handle framework-specific concerns: port assignment, CLI flags,
    ASGI wiring, transport config. Framework-specific settings are resolved
    via standard dlt config/secrets.
    """

    module: str
    function: Optional[str]
    job_type: TJobType
    launcher: NotRequired[Optional[str]]
    expose: NotRequired[TExposeSpec]


# -- execution constraints ---------------------------------------------------


class TTimeoutSpec(TypedDict):
    """Expanded timeout specification stored in the manifest.

    The decorator accepts shorthand forms (float seconds, human string).
    The manifest always stores the expanded TypedDict.
    """

    timeout: float
    """Max wall-clock duration in seconds."""
    grace_period: NotRequired[float]
    """Seconds to allow for graceful shutdown before hard kill. Default: 30."""


class TExecutionSpec(TypedDict):
    """Runtime execution constraints for a job.

    timeout:     max wall-clock duration. In the manifest, always stored as
                 TTimeoutSpec with timeout in seconds and optional grace_period.
                 The decorator accepts shorthand forms:
                   - float: seconds (e.g. 3600.0)
                   - str: human-readable interval (e.g. "4h", "24h", "30m")
                   - TTimeoutSpec: explicit dict with timeout and grace_period
                 None means no limit.
    concurrency: max concurrent runs of this job. Defaults to 1.
    """

    timeout: NotRequired[Optional[TTimeoutSpec]]
    concurrency: NotRequired[int]


# -- delivery ----------------------------------------------------------------


class TDeliveryRef(TypedDict):
    """Associates a job with a data source it is supposed to deliver.

    Enables freshness monitoring: the runtime knows the expected delivery
    deadline and can alert when data is late.
    """

    source_ref: str
    """Source rel_ref, e.g. "sources.job_runs.chat_analytics"."""
    deadline: str


class TDeliverySpec(TypedDict):
    """Top-level delivery specification.

    Links a data source/resource to its delivery deadline and the jobs
    responsible for delivering it.
    """

    source_ref: str
    """Source rel_ref, e.g. "sources.job_runs.chat_analytics"."""
    deadline: str
    job_refs: List[str]
    """Job rel_refs that deliver this source."""


# -- job definition ----------------------------------------------------------


class TJobDefinition(TypedDict):
    """A single job in the deployment manifest.

    Identity:
        job_ref is the unique key within a workspace: "jobs.<section>.<name>"
        section defaults to the module name, name defaults to the function name.

    Discovery:
        - Only names listed in __deployment__.__all__ are considered.
        - Detectors run on each __all__ element and determine job_type,
          launcher, and expose.
        - The first detector that matches produces the TJobDefinition.
        - Detectors may inspect further (e.g. docstrings, notebook title).

    Lifecycle:
        - Present in manifest -> upsert (create or update)
        - Absent from manifest -> soft delete (inactive, triggers disabled,
          run history preserved)
        - Hard delete only via explicit CLI: `dlt runtime delete <ref> --confirm`
    """

    job_ref: str
    """Job rel_ref: "jobs.<section>.<name>" for function jobs,
    "jobs.<section>" for module-level jobs."""
    display_name: NotRequired[str]
    """Inferred by launcher when possible (e.g. notebook title from marimo)."""
    description: NotRequired[str]

    entry_point: TEntryPoint

    triggers: List[TTrigger]

    execution: TExecutionSpec

    config_keys: NotRequired[List[str]]

    deliver: NotRequired[TDeliveryRef]

    starred: bool
    tags: NotRequired[List[str]]


# -- file item ---------------------------------------------------------------


class TDeploymentFileItem(TypedDict):
    """A file in the deployment package."""

    relative_path: str
    size_in_bytes: int
    sha3_256: str


# -- top-level manifest ------------------------------------------------------


class TDeploymentManifest(TypedDict):
    """Collected from the local workspace by `dlt workspace deploy`.

    Contains only information discovered from __deployment__.py and the
    workspace filesystem. Runtime-specific fields (workspace_id, deployment_id)
    are NOT part of this manifest — the runtime assigns them on receipt.

    Sent to the runtime API which reconciles jobs:
        - jobs in manifest -> upsert
        - jobs not in manifest -> soft delete (inactive, history preserved)

    Deployment module metadata (if present, overwrites workspace-level values):
        __doc__   -> description
        __tags__  -> tags

    A workspace may have multiple deployment modules. Keep workspace-level
    description/tags in the "main" deployment module.
    """

    engine_version: int
    created_at: str

    deployment_module: str
    description: NotRequired[str]
    tags: NotRequired[List[str]]

    files: List[TDeploymentFileItem]
    jobs: List[TJobDefinition]
    delivery_specs: List[TDeliverySpec]


# -- example manifest --------------------------------------------------------

EXAMPLE_MANIFEST: TDeploymentManifest = {
    "engine_version": MANIFEST_ENGINE_VERSION,
    "created_at": "2026-02-19T12:00:00Z",
    "deployment_module": "__deployment__",
    "description": "Chat ingestion workspace - loads and transforms chat data",
    "tags": ["production", "team:data-eng"],
    "files": [
        {
            "relative_path": "job_runs.py",
            "size_in_bytes": 3420,
            "sha3_256": "abcdef0123456789",
        },
        {
            "relative_path": "notebook.py",
            "size_in_bytes": 512,
            "sha3_256": "9876543210fedcba",
        },
        {
            "relative_path": "__deployment__.py",
            "size_in_bytes": 280,
            "sha3_256": "1122334455667788",
        },
    ],
    "jobs": [
        # -- batch job: no triggers (manual only) --
        {
            "job_ref": "jobs.job_runs.backfil_chats",
            "description": "Backfill chat messages from source database",
            "entry_point": {
                "module": "job_runs",
                "function": "backfil_chats",
                "job_type": "batch",
            },
            "triggers": [],
            "execution": {"timeout": {"timeout": 86400.0}, "concurrency": 1},
            "starred": False,
            "tags": ["ingestion"],
        },
        # -- batch job: cron trigger --
        {
            "job_ref": "jobs.job_runs.get_daily_chats",
            "entry_point": {
                "module": "job_runs",
                "function": "get_daily_chats",
                "job_type": "batch",
            },
            "triggers": ["schedule:0 8 * * *"],
            "execution": {"timeout": {"timeout": 14400.0}, "concurrency": 1},
            "starred": False,
        },
        # -- batch job: triggered by other jobs, delivers an SLA --
        {
            "job_ref": "jobs.job_runs.analyze_chats",
            "entry_point": {
                "module": "job_runs",
                "function": "analyze_chats",
                "job_type": "batch",
            },
            "triggers": [
                "job.success:jobs.job_runs.backfil_chats",
                "job.success:jobs.job_runs.get_daily_chats",
            ],
            "execution": {"concurrency": 1},
            "deliver": {
                "source_ref": "sources.job_runs.chat_analytics",
                "deadline": "8am on Mondays",
            },
            "starred": False,
            "tags": ["transformation"],
        },
        # -- batch job: tag trigger --
        {
            "job_ref": "jobs.job_runs.load_chats",
            "entry_point": {
                "module": "job_runs",
                "function": "load_chats",
                "job_type": "batch",
            },
            "triggers": [
                "tag:backfill",
            ],
            "execution": {"timeout": {"timeout": 86400.0, "grace_period": 60.0}, "concurrency": 1},
            "starred": False,
        },
        # -- batch job: config injection --
        {
            "job_ref": "jobs.job_runs.job_picker",
            "description": "This magic job will run code for any other job",
            "entry_point": {
                "module": "job_runs",
                "function": "job_picker",
                "job_type": "batch",
            },
            "triggers": [],
            "execution": {"concurrency": 1},
            "config_keys": ["job_name"],
            "starred": True,
        },
        # -- interactive: marimo notebook (auto-detected, launcher fills expose) --
        {
            "job_ref": "jobs.notebook",
            "entry_point": {
                "module": "notebook",
                "function": None,
                "job_type": "interactive",
                "launcher": "dlt._workspace.deployment.launchers.marimo",
                "expose": {"interface": "gui", "port": 2718},
            },
            "triggers": ["http:2718"],
            "execution": {"concurrency": 1},
            "starred": False,
            "tags": ["notebook"],
        },
        # -- interactive: mcp server (auto-detected) --
        {
            "job_ref": "jobs.my_mcp",
            "entry_point": {
                "module": "my_mcp",
                "function": None,
                "job_type": "interactive",
                "launcher": "dlt._workspace.deployment.launchers.mcp",
                "expose": {"interface": "mcp"},
            },
            "triggers": ["http:5000"],
            "execution": {"concurrency": 1},
            "starred": False,
        },
        # -- interactive: streamlit app (auto-detected) --
        {
            "job_ref": "jobs.my_streamlit_app",
            "entry_point": {
                "module": "my_streamlit_app",
                "function": None,
                "job_type": "interactive",
                "launcher": "dlt._workspace.deployment.launchers.streamlit",
                "expose": {"interface": "gui", "port": 8501},
            },
            "triggers": ["http:8501"],
            "execution": {"concurrency": 1},
            "starred": False,
            "tags": ["dashboard"],
        },
        # -- interactive: custom uwsgi app (no launcher, user-specified expose) --
        {
            "job_ref": "jobs.my_uwsgi.webapp",
            "description": "will see this in runtime",
            "entry_point": {
                "module": "my_uwsgi",
                "function": "start",
                "job_type": "interactive",
                "expose": {"interface": "rest_api", "port": 9090},
            },
            "triggers": ["http:9090"],
            "execution": {"concurrency": 1},
            "starred": False,
        },
    ],
    "delivery_specs": [
        {
            "source_ref": "sources.job_runs.chat_analytics",
            "deadline": "8am on Mondays",
            "job_refs": ["jobs.job_runs.analyze_chats"],
        },
    ],
}
