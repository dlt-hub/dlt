---
title: Deployments
description: Deploy dlt pipelines, jobs, and interactive applications to the dltHub platform with ad-hoc runs or a versioned deployment manifest
keywords: [dlthub platform, deploy, deployment, jobs, decorators, manifest, reconciliation]
---

# Deployments

The dltHub platform offers two ways to get your code running in the cloud:

- **Ad-hoc launch** — point `dlthub run` or `dlthub serve` at a Python file. Best for quickly trying a script.
- **Manifest-based deploy** — declare jobs in `__deployment__.py` and run `dlthub deploy`. Required for scheduling, follow-up triggers, freshness checks, and multi-job workspaces.

Both methods require a configured workspace — see [Workspace setup](workspace-setup.md) if you haven't done that yet.

## Quick deploy: ad-hoc launch

The fastest way to run an existing script on the dltHub platform is to point `run` or `serve` at a Python file:

```sh
# run the batch script locally first to catch missing dependencies or broken config
dlthub local run fruitshop_pipeline.py

# Deploy and run a batch script (uses `prod` profile)
dlthub run fruitshop_pipeline.py

# Stream logs in your terminal until the run completes
dlthub run fruitshop_pipeline.py -f

# Deploy and serve an interactive app (notebook, dashboard, MCP — uses `access` profile)
dlthub local serve fruitshop_notebook.py    # local
dlthub serve fruitshop_notebook.py          # remote
```

Under the hood, the CLI generates a single-job deployment manifest from that file and syncs it to the dltHub platform. This **ad-hoc deploy** is great for getting started but does not support:

- Scheduled triggers (cron, intervals)
- Follow-up jobs (run B after A succeeds)
- Freshness constraints
- Multi-job workspaces deployed as a unit

For all of these you need **job decorators** and a **deployment module**, described next.

## Jobs and deployments

A dltHub platform workspace can contain many jobs scheduled on different cadences, chained together by triggers and freshness constraints. The three building blocks are:

- **Job decorators** that attach scheduling and metadata to Python functions
- **`__deployment__.py`** that declares which jobs exist in the workspace
- **`dlthub deploy`** that syncs the entire job graph to the dltHub platform in one step

### Job decorators

The `dlt.hub.run` module provides three decorators:

| Decorator | Used for |
|-----------|----------|
| `@run.pipeline` | A batch job bound to a named `dlt.pipeline` (gets pipeline-aware retries and dataset linking) |
| `@run.job` | A general-purpose batch job (any Python function — data quality checks, reports, custom scripts) |
| `@run.interactive` | A long-running HTTP service (notebook, MCP server, Streamlit app, REST API) |

Example: an ingestion pipeline that runs every 5 minutes and is tagged for bulk operations.

```py
import dlt
from dlt.hub import run
from dlt.hub.run import trigger

@run.pipeline(
    "github_pipeline",
    trigger=trigger.every("5m"),
    expose={"tags": ["ingest"], "display_name": "GitHub commits ingest"},
)
def load_commits():
    """Load commits and contributors from the GitHub REST API."""
    pipeline = dlt.pipeline(
        pipeline_name="github_pipeline",
        destination="warehouse",
        dataset_name="github_data",
    )
    pipeline.run(github_rest_api_source())
```

A general-purpose job, scheduled hourly:

```py
@run.job(
    trigger=trigger.schedule("0 * * * *"),
    expose={"display_name": "GitHub data quality"},
)
def run_dq_checks():
    """Validate ingested data; the job fails if any check fails."""
    if not all_passed:
        raise RuntimeError("Data quality checks failed")
```

For the full catalog of `trigger=` options (cron, intervals, follow-ups, freshness, refresh cascade), see [Triggers and scheduling](triggers.md). For per-job options like `execute=`, `require=`, and `expose=`, see [Job configuration](job-configuration.md).

### The deployment module

`__deployment__.py` is a Python module that declares everything deployable in the workspace. The dltHub platform discovers jobs by inspecting it.

```py
"""GitHub ingest workspace -- loads and monitors GitHub API data"""

from github_pipeline import load_commits
from github_dq_pipeline import run_dq_checks

import github_transformations_notebook
import github_dq_notebook
import github_report_notebook

__all__ = [
    "load_commits",
    "run_dq_checks",
    "github_transformations_notebook",
    "github_dq_notebook",
    "github_report_notebook",
]
```

Rules:

- **Function imports** (`from github_pipeline import load_commits`) produce one job per function. The function must be decorated with `@run.pipeline`, `@run.job`, or `@run.interactive`.
- **Module imports** (`import github_report_notebook`) produce one job per module. The framework is auto-detected — marimo notebooks become interactive notebook jobs, FastMCP modules become MCP servers, Streamlit modules become dashboards.
- **`__all__`** lists exactly the names to deploy. Without it, the manifest generator scans `__dict__` and warns.
- **`__doc__`** (the module docstring) becomes the workspace description in the dltHub platform dashboard.

You can also define decorated jobs **inline** in `__deployment__.py` — useful for small MCP servers or one-off batch jobs.

### Deploying with `dlthub deploy`

This is the central command for manifest-based deployment. It reads `__deployment__.py`, generates a manifest, and syncs it to the dltHub platform:

```sh
dlthub deploy
```

The deploy command:

1. Imports `__deployment__.py` and collects every job
2. Generates a deployment manifest (a JSON document describing every job's triggers, entry point, and metadata)
3. Syncs your code and configuration to the dltHub platform
4. Sends the manifest for **reconciliation**

#### Reconciliation

The dltHub platform compares the new manifest against the currently deployed jobs:

| Status | Meaning |
|--------|---------|
| **added** | New job — will be created |
| **updated** | Job definition changed — will be updated |
| **unchanged** | No changes — left as-is |
| **archived** | Job was in the previous manifest but not in this one — triggers disabled, history preserved |

Removing a job from `__deployment__.py` does not delete it — it archives it, preserving run history and logs.

#### Preview before deploying

```sh
# see what would change without applying
dlthub deploy --dry-run

# dump the full expanded manifest as YAML
dlthub deploy --show-manifest
```

### Running and monitoring deployed jobs

Once deployed, scheduled jobs run automatically. You can also run them by hand — and run the local counterpart first whenever you want to debug locally:

```sh
# run a specific job by name (ad-hoc, syncs code first)
dlthub local run load_commits         # locally
dlthub run load_commits -f            # in the cloud

# trigger jobs without re-syncing code (uses currently deployed code)
dlthub job trigger "tag:ingest"
dlthub job trigger "schedule:*"
dlthub job trigger "tag:ingest" --dry-run    # preview only

# trigger by pipeline name
dlthub local pipeline run github_pipeline    # locally
dlthub pipeline run github_pipeline          # in the cloud

# serve an interactive job
dlthub local serve github_report_notebook    # locally
dlthub serve github_report_notebook          # in the cloud
```

:::note
`dlthub pipeline run` (and its local sibling) can only trigger jobs decorated with `@run.pipeline` — they are matched by `deliver.pipeline_name`. Jobs declared with `@run.job` or `@run.interactive` are not addressable this way; use `dlthub run <job_name>` or `dlthub job trigger <selector>` instead.
:::

For diagnosing failed runs, viewing logs, and dashboards, see [Monitoring and debugging](monitoring.md).

## Deployments and configurations are versioned separately

- **Deployment** — your code files (`.py` scripts, notebooks)
- **Configuration** — your `.dlt/*.toml` files (settings and secrets)

You can update code without changing secrets and vice versa. Use these commands to sync them independently:

```sh
# Sync code and configuration without reconciling the manifest
dlthub workspace deployment sync       # sync only code
dlthub workspace configuration sync    # sync only configuration

# List and inspect previous versions
dlthub workspace deployment list
dlthub workspace deployment info [version_number]
dlthub workspace configuration list
dlthub workspace configuration info [version]
```

See the [CLI reference](../command-line-interface.md) for the full set of deployment and job commands.
