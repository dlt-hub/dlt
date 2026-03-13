# Deployment Manifest Spec

The deployment manifest is the contract between the local workspace and the dlt runtime.
It is produced by `dlt workspace deploy` and consumed by the runtime API.

## Overview

```
__deployment__.py
       |
       v  (import + inspect)
  dlt workspace deploy
       |
       v  (serialize)
  TDeploymentManifest (JSON)
       |
       v  (send to runtime API)
  Runtime reconciles:
    - jobs in manifest    -> upsert (create or update)
    - jobs not in manifest -> soft delete (inactive, triggers disabled, history preserved)
    - hard delete only via: dlt runtime delete <job_name> --confirm
```

## Deployment module: `__deployment__.py`

The deployment module declares what exists in the workspace via Python imports.
The import style determines the job type:

```python
"""Chat ingestion workspace - loads and transforms chat data"""

__tags__ = ["production", "team:data-eng"]

# explicit function imports -> one job per function, uses @runtime.job() metadata
from job_runs import backfil_chats, analyze_chats, get_daily_chats
from my_uwsgi import start

# module imports -> one job per module, function=null, launcher auto-detected
import notebook          # -> marimo (detected: marimo.App)
import my_mcp            # -> mcp (detected: FastMCP instance)
import my_streamlit_app  # -> streamlit (detected: st.* usage)
```

### Module-level metadata

| Dunder      | Manifest field  | Purpose                                |
|-------------|-----------------|----------------------------------------|
| `__doc__`   | `description`   | Workspace description visible in runtime |
| `__tags__`  | `tags`          | Deployment-level tags                  |

## Reference scheme

Sources and jobs share a unified naming scheme:

```
rel_ref := jobs|sources.<section>.<name>    (function-level)
rel_ref := jobs|sources.<section>           (module-level)
abs_ref := <workspace>.rel_ref
```

Where:
- **workspace**: from pyproject name, or folder name by default
- **section**: module name (not fully qualified) by default
- **name**: function name by default; omitted for module-level jobs

The manifest uses `rel_ref`. It doubles as the config layout path.

Examples:
- `jobs.job_runs.backfil_chats` — function job in `job_runs` module
- `jobs.notebook` — module-level marimo job
- `sources.job_runs.chat_analytics` — source in `job_runs` module

Shorthands: the `jobs.`/`sources.` prefix can be omitted in CLI and trigger strings
when unambiguous. The manifest always stores the full rel_ref.

## Job types

Jobs have three execution models:

| Job type      | Execution model                                              |
|---------------|--------------------------------------------------------------|
| `batch`       | Call once, run to completion, exit                           |
| `interactive` | Start process, keep it running, expose HTTP                  |
| `stream`      | Runtime manages consumer loop, calls function with micro-batches |

Job type determines how the runtime allocates resources, handles failures,
and manages lifecycle. It is orthogonal to the launcher.

## Launchers

Launchers are convenience wrappers for common frameworks. They increase QoL
for the most commonly run interactive jobs like marimo notebooks or MCP servers.

A launcher handles framework-specific concerns: port assignment, CLI flags,
ASGI wiring, transport configuration. Framework-specific settings (e.g. MCP
transport mode) are resolved via standard dlt config/secrets.

### Pre-defined launchers

Launchers are stored in the manifest as fully qualified Python module paths.
This makes the invocation unambiguous: `python -m <launcher> <job-context-args>`.

| Launcher module                       | Detects                  | Default expose                        |
|---------------------------------------|--------------------------|---------------------------------------|
| `dlt._workspace.deployment.launchers.marimo`     | `marimo.App` in module   | `{interface: "gui", port: 2718}`      |
| `dlt._workspace.deployment.launchers.mcp`        | `FastMCP` instance       | `{interface: "mcp"}`                  |
| `dlt._workspace.deployment.launchers.streamlit`  | `streamlit` usage        | `{interface: "gui", port: 8501}`      |

### System launcher (None)

When `launcher` is `None`, the system launcher is used
(`dlt._workspace.deployment.launchers.job`):

| job_type      | function | Behavior                                      |
|---------------|----------|-----------------------------------------------|
| `batch`       | set      | Import module, call `JobFactory`              |
| `batch`       | null     | Run module as `__main__` (`python -m module`) |
| `interactive` | set      | Import module, call function (long-running)   |
| `interactive` | null     | Import module, run as `__main__` (long-running) |

The system launcher is distributed in dlt (e.g. `dlt._workspace.deployment.launchers.job`).
It imports the module, resolves configuration, and invokes the `JobFactory` or
executes the module directly.

## Job discovery

Detectors run on each element in `__deployment__.__all__` to determine
job type, launcher, and expose configuration.

| Detector   | Matches on                           | job_type      | launcher      | expose                           |
|------------|--------------------------------------|---------------|---------------|----------------------------------|
| `@runtime.job` (batch) | `JobFactory` without `http` trigger | `batch` | `None` | — |
| `@runtime.job` (interactive) | `JobFactory` with `http` trigger | `interactive` | `None` | from decorator args |
| marimo     | module with `marimo.App`             | `interactive` | `dlt._workspace.deployment.launchers.marimo`    | `{interface: "gui", port: 2718}` |
| mcp        | module with `FastMCP` instance       | `interactive` | `dlt._workspace.deployment.launchers.mcp`       | `{interface: "mcp"}`             |
| streamlit  | module with `streamlit` usage        | `interactive` | `dlt._workspace.deployment.launchers.streamlit` | `{interface: "gui", port: 8501}` |
| fallback   | module with `__main__` block         | `batch`       | `None`        | —                                |

Discovery rules:
1. Read `__deployment__.__all__` — only explicitly listed names are considered
2. For each element, run detectors in order
3. First match produces the `TJobDefinition`
4. Detectors may inspect further (e.g. docstrings, notebook title)

## Entry points

An entry point defines what code to invoke and how.

| Field      | Type            | Required | Description                                      |
|------------|-----------------|----------|--------------------------------------------------|
| `module`   | `str`           | yes      | Python module path relative to workspace root    |
| `function` | `str \| null`   | yes      | Function name, or null for module-level jobs      |
| `job_type` | `TJobType`      | yes      | Execution model (batch, interactive, stream)      |
| `launcher` | `str \| null`   | no       | Fully qualified launcher module (e.g. `dlt._workspace.deployment.launchers.marimo`), null for system default |
| `expose`   | `TExposeSpec`   | no       | How to reach/present the job (interactive only)   |

## Expose spec (interactive jobs)

Interactive jobs expose HTTP endpoints. The expose spec tells the runtime
what interface the job presents and optionally which port.

| Field       | Type              | Default | Description                                    |
|-------------|-------------------|---------|------------------------------------------------|
| `interface` | `TInterfaceType`  | —       | What the job exposes: `"gui"`, `"rest_api"`, `"mcp"` |
| `port`      | `int`             | from trigger | Override port (usually derived from `http:<port>` trigger) |

### Interface types

| Interface  | Runtime behavior                                          |
|------------|-----------------------------------------------------------|
| `gui`      | Proxy HTTP, present URL to user. Use tags for display hints: `"notebook"`, `"dashboard"`, `"webapp"` |
| `rest_api` | Proxy HTTP, health checks, no UI framing                  |
| `mcp`      | Proxy HTTP (streamable HTTP transport), register in tool catalog |

All interfaces use HTTP. The distinction tells the runtime how to present and
integrate the job, not which protocol to use.

## Triggers

Triggers are **canonical strings** with a normalized form: `type:expr`.
Both workspace and runtime share the same grammar.
A trigger can be parsed from and serialized back to a string unambiguously.

Jobs may have 0, 1, or many triggers.

### Normalized form

The manifest always stores triggers in normalized `type:expr` form.

| Type            | Normalized form                                  | Expr                       |
|-----------------|--------------------------------------------------|----------------------------|
| `schedule`      | `schedule:0 8 * * *`                             | cron expression            |
| `every`         | `every:5h`                                       | human-readable period      |
| `once`          | `once:2026-03-15T08:00:00Z`                      | ISO 8601 timestamp         |
| `job.success`   | `job.success:jobs.job_runs.backfil_chats`        | job_ref                    |
| `job.fail`      | `job.fail:jobs.job_runs.backfil_chats`           | job_ref                    |
| `http`          | `http:5000`                                      | port number                |
| `deployment`    | `deployment:`                                    | (empty)                    |
| `webhook`       | `webhook:ingest/chat`                            | path                       |
| `tag`           | `tag:backfill`                                   | tag name                   |

### Shorthands

Certain inputs are auto-detected and normalized before storing in the manifest:

| Input                        | Normalized to                        | Detection rule                 |
|------------------------------|--------------------------------------|--------------------------------|
| `0 8 * * *`                  | `schedule:0 8 * * *`                | 5 space-separated cron fields  |
| `2026-03-15T08:00:00Z`      | `once:2026-03-15T08:00:00Z`        | ISO 8601 timestamp             |
| `http`                       | `http:5000`                          | literal keyword, default port  |
| `http:9090`                  | `http:9090`                          | already normalized             |
| `deployment`                 | `deployment:`                        | literal keyword                |
| `webhook`                    | `webhook:`                           | literal keyword                |

### Server-side validation

Job triggers (`job.success`, `job.fail`) that reference non-existing
job_refs must be flagged as validation errors by the runtime.

### Trigger examples in Python decorators

```python
# cron (shorthand — auto-detected and normalized to "schedule:0 8 * * *")
@runtime.job(trigger="0 8 * * *")

# period
@runtime.job(trigger="every:6h")

# tag (broadcast: `dlt runtime launch tag:backfill` triggers all tagged jobs)
@runtime.job(trigger="tag:backfill")

# job event (Python sugar: backfil_chats.success resolves to "job.success:jobs.job_runs.backfil_chats")
@runtime.job(trigger=backfil_chats.success)

# multiple triggers
@runtime.job(trigger=["0 8 * * *", backfil_chats.success])

# interactive with default port (normalized to "http:5000")
@runtime.job(trigger="http")

# interactive with explicit port
@runtime.job(trigger="http:9090", expose={"interface": "rest_api"})

# run after every deployment (code sync)
@runtime.job(trigger="deployment")

# one-shot at a specific time
@runtime.job(trigger="once:2026-03-15T08:00:00Z")
```

### Trigger state

Triggers can be enabled/disabled by the runtime without modifying the manifest.
The manifest carries the declared set; runtime tracks enabled/disabled state separately.

## Execution constraints

| Field         | Type                  | Default | Description                                   |
|---------------|-----------------------|---------|-----------------------------------------------|
| `timeout`     | `TTimeoutSpec \| null` | null   | Max wall-clock duration with optional grace period |
| `concurrency` | `int`                 | 1       | Max concurrent runs of this job               |

### Timeout

The manifest stores timeout as a `TTimeoutSpec` dict:

| Field          | Type    | Default | Description                                    |
|----------------|---------|---------|------------------------------------------------|
| `timeout`      | `float` | —       | Max wall-clock duration in seconds             |
| `grace_period` | `float` | 30.0    | Seconds for graceful shutdown before hard kill |

The `@runtime.job` decorator accepts shorthand forms that are expanded
before storing in the manifest:

| Decorator input                     | Manifest `TTimeoutSpec`                           |
|-------------------------------------|---------------------------------------------------|
| `timeout=3600`                      | `{"timeout": 3600.0}`                             |
| `timeout=3600.0`                    | `{"timeout": 3600.0}`                             |
| `timeout="4h"`                      | `{"timeout": 14400.0}`                            |
| `timeout="24h"`                     | `{"timeout": 86400.0}`                            |
| `timeout={"timeout": 86400, "grace_period": 60}` | `{"timeout": 86400.0, "grace_period": 60.0}` |

These apply to all job types. For batch jobs, timeout prevents runaway processes.
For interactive jobs, timeout enables periodic recycling (e.g. `"24h"` to restart daily).
The grace period gives the job time to finish in-flight work before being hard-killed.

### No control-plane retries

Retries live in user code via `runner(pipeline, retry_policy="backoff")`.
This is intentional: data pipeline retry semantics are domain-specific and
belong in the code, not on the platform.

## Delivery specs

Delivery specs link jobs to data sources with freshness deadlines.
This enables monitoring "something that didn't happen" - the runtime can alert
when expected data hasn't been delivered by the deadline.

| Field        | Type         | Description                                 |
|--------------|-------------|----------------------------------------------|
| `source_ref` | `str`        | Source rel_ref                               |
| `deadline`   | `str`        | Human-readable deadline ("8am on Mondays")   |
| `job_refs`   | `list[str]`  | Job rel_refs that deliver this source        |

### Example

```python
@delivery.sla(deadline="8am on Mondays")
@dlt.source
def chat_analytics(chat_dataset: dlt.Dataset):
    ...

@runtime.job(
    trigger=[backfil_chats.success, get_daily_chats.success],
    deliver=chat_analytics
)
def analyze_chats(run_context: TJobRunContext):
    ...
```

Produces in the manifest:
```json
{
  "deliver": {"source_ref": "sources.job_runs.chat_analytics", "deadline": "8am on Mondays"},
  "triggers": ["job.success:jobs.job_runs.backfil_chats", "job.success:jobs.job_runs.get_daily_chats"]
}
```

## Job lifecycle

### Deploy (upsert)

`dlt workspace deploy` imports the deployment module, collects all jobs, and sends
the manifest to runtime. Runtime upserts jobs by their `job_ref`.

### Removal (soft delete)

Jobs present in the previous manifest but absent from the new one are **soft deleted**:
- Status set to `inactive`
- All triggers disabled
- Run history, pipeline state, and logs preserved

### Hard delete

Only via explicit user action:
```
dlt runtime delete <job_ref> --confirm
```

Ref shorthands are accepted in CLI and trigger strings: the `jobs.` or `sources.`
prefix is added automatically when unambiguous (e.g. `job_runs.backfil_chats` resolves
to `jobs.job_runs.backfil_chats`).

This is a data platform. Stuff does not disappear.

## Top-level manifest fields

The manifest contains only information collected from the local workspace.
Runtime-specific fields (`workspace_id`, `deployment_id`) are assigned by the
runtime on receipt and are not part of this contract.

| Field               | Type                    | Description                           |
|---------------------|-------------------------|---------------------------------------|
| `engine_version`    | `int`                   | Manifest schema version               |
| `created_at`        | `str`                   | ISO 8601 timestamp                    |
| `deployment_module` | `str`                   | Module name (default: `__deployment__`) |
| `description`       | `str \| null`           | From `__doc__`; if present, overwrites workspace description |
| `tags`              | `list[str]`             | From `__tags__`; if present, overwrites workspace tags |
| `files`             | `list[TDeploymentFileItem]` | Files in the deployment package   |
| `jobs`              | `list[TJobDefinition]`  | All jobs in this deployment           |
| `delivery_specs`    | `list[TDeliverySpec]`   | Delivery SLA definitions              |

A workspace may have multiple deployment modules. Keep workspace-level
description and tags in the "main" deployment module.
