---
title: Deployment
description: Use the dlthub-platform toolkit to author __deployment__.py, deploy your workspace, and schedule pipelines on the dltHub Platform.
keywords: [ai harness, dlthub-platform, deployment, __deployment__.py, dlthub deploy, scheduling, triggers]
---

# Deployment

The `dlthub-platform` toolkit is the AI Harness's answer to "I've got a pipeline running locally, now how do I run it on dltHub?" It guides your coding agent through preparing a deployment manifest, running `dlthub deploy`, wiring up schedules, and debugging failures.

Deployment on the dltHub Platform is manifest-based: a Python file, called `__deployment__.py`, at the root of your workspace declares which pipelines and notebooks are deployable. `dlthub deploy` reads that file and syncs the manifest to the platform. See [Deployments](../pipeline-operations/deployments.md) for more information. This page focuses on how the toolkit drives them from your agent.

## Use cases

Install the toolkit when you have a pipeline that runs locally with `dlthub local run` and you want to:

- Run it on the platform on demand (one-off) or on a cron schedule.
- Serve a notebook or dashboard as an interactive job.
- Add freshness checks or chain jobs off other job outcomes.
- Debug a job that failed after deploy.

For a one-off remote execution without scheduling you don't strictly need this toolkit. `dlthub run <script>` uploads and runs the script as a batch job. Reach for `dlthub-platform` when you want persistent, scheduled deployment.

## Install

Either let the router pick it when you tell your agent "let's deploy," or install explicitly:

```sh
uv run dlthub ai toolkit install dlthub-platform
```

## The skills

| Skill | When it runs | What it does |
| --- | --- | --- |
| `setup-runtime` | Once, before your first deploy | Verifies the workspace is ready: `pyproject.toml` present, `dlt[hub]` installed, `.dlt/.workspace` exists, and you're logged in and connected to a workspace on the platform. |
| `prepare-deployment` | Every time you add or change a deployable job | Splits dev and prod credentials into profile-scoped files, sets up a production destination, and helps you edit `__deployment__.py` so pipelines and notebooks are exported and triggered correctly. |
| `deploy-workspace` | After `prepare-deployment` finishes cleanly | Runs `dlthub deploy`, streams progress, and confirms which jobs registered on the platform. |
| `debug-deployment` | After a deploy or a scheduled run fails | Reads platform logs, inspects the manifest, checks credentials, and proposes a fix. |

The four skills chain naturally, so you usually don't invoke them by name. The workflow rule shipped with the toolkit tells the agent which one to run next.

## Schedule a pipeline every 10 mins

Assume you have a `fruitshop_pipeline.py` in the workspace that already runs locally. You tell your agent:

> Deploy `fruitshop_pipeline` to the platform on a 10-minute schedule.

Here's the sequence the toolkit drives, roughly what you'll see in the agent's turns:

**1. `setup-runtime` (only on the first deploy)**: the agent checks `pyproject.toml`, verifies `.dlt/.workspace` exists and `dlt[hub]` is installed, then walks you through `dlthub login` and `dlthub workspace connect`. If anything is missing, it asks you to fix it before continuing.

**2. `prepare-deployment`**: the agent splits dev and prod credentials into `.dlt/dev.secrets.toml` and `.dlt/prod.secrets.toml`, sets up a production destination (for example, Motherduck if you're on DuckDB locally), then opens `__deployment__.py`, imports `load_fruitshop` from your pipeline module, wraps it with `run.pipeline` and a schedule trigger, and exports it in `__all__`:

```py title="fruitshop_pipeline.py"
import dlt
from fruitshop_source import fruitshop

def load_fruitshop():
    pipeline = dlt.pipeline(
        pipeline_name="fruitshop_pipeline",
        destination="fruitshop_destination",
        dataset_name="fruitshop_data",
    )
    pipeline.run(fruitshop())
```

```py title="__deployment__.py"
"""Fruitshop workspace — ingests fruitshop data every 10 minutes."""
from dlt.hub import run
from dlt.hub.run import trigger
from fruitshop_pipeline import load_fruitshop

load_fruitshop = run.pipeline(
    "fruitshop_pipeline",
    trigger=trigger.schedule("*/10 * * * *"),
)(load_fruitshop)

__all__ = ["load_fruitshop"]
```

**3. `deploy-workspace`**: the agent first runs `uv run dlthub deploy --dry-run` to preview the manifest changes, then, after you approve:

```sh
uv run dlthub deploy
```

It streams the output, confirms the job registered with the schedule you asked for, and links to the platform UI where you can watch the next run.

**4. Later, if a scheduled run fails**: you tell the agent "the 10-minute job is failing." It invokes `debug-deployment`, fetches the failed job's logs, checks the manifest and credentials, and either fixes the issue in code or explains why the platform is rejecting the deploy.

## Related reading

- [Deployments](../pipeline-operations/deployments.md) covers the manifest model, how `dlthub deploy` generates and syncs it, and archived-vs-active job semantics.
- [Triggers](../pipeline-operations/triggers.md) documents `trigger.schedule`, follow-up chains, and freshness checks in depth.
- [Secrets management](../pipeline-operations/secrets-management.md) covers how the platform stores and injects the credentials your deployed jobs need.
- [Job configuration](../pipeline-operations/job-configuration.md) covers per-job settings like static egress IPs, runtimes, and profile-aware overrides.
