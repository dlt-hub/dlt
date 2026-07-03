---
title: Deploy a pipeline with dltHub
description: Run, deploy, and schedule dlt pipelines with dltHub's managed orchestrator
keywords: [orchestrator, scheduling, cron, dlthub, platform, deploy]
---

# Deploy a pipeline with dltHub

dltHub ships a managed orchestrator built around the `@dlt.hub.run` decorators. The schedule and the data dependencies live in your Python code, not in a separate DAG (directed acyclic graph) file or YAML.

This page walks through four stages: scaffolding a workspace, running a pipeline ad-hoc, deploying it via `__deployment__.py`, and scheduling it with a trigger. For the full feature surface (follow-up chains, freshness gates, refresh cascade, tags, timezones) see [Triggers and scheduling](../../hub/pipeline-operations/triggers.md).

## 1. Install and scaffold a workspace

If you don't have `uv` yet, follow the [uv installation guide](https://docs.astral.sh/uv/getting-started/installation/). Then scaffold a workspace:

```sh
uvx dlthub-init@latest
```

This creates the `.dlt/.workspace` marker that activates workspace mode, along with `pyproject.toml` and `.dlt/` config and secrets files. For other install options and setup paths, see [Installation](../../hub/getting-started/installation.md).

## 2. Run a pipeline ad-hoc

Any regular dlt pipeline can be run on the platform without a deployment file. Given a `pipeline.py` with a top-level `pipeline.run(...)` call:

```sh
uv run dlthub run pipeline.py
```

This deploys the file ad-hoc, executes it on the platform, and streams logs back to your terminal. Use this for one-off runs and smoke tests. See [Quick deploy](../../hub/pipeline-operations/deployments.md#quick-deploy-ad-hoc-launch).

## 3. Add `__deployment__.py`

To run the same pipeline as a managed job, decorate the entrypoint and declare it in a workspace `__deployment__.py`:

```py
import dlt
from dlt.hub import run


@run.pipeline("ingest_breweries")
def ingest_breweries():
    pipeline = dlt.pipeline(
        pipeline_name="ingest_breweries",
        destination="warehouse",
        dataset_name="brewery_data",
    )
    pipeline.run(brewery_source())
```

```py
# __deployment__.py
from ingest_breweries import ingest_breweries

__all__ = ["ingest_breweries"]
```

Deploy with:

```sh
uv run dlthub deploy
```

To trigger a run, invoke the job by name:

```sh
uv run dlthub run ingest_breweries
```

See [Deployments](../../hub/pipeline-operations/deployments.md) for the manifest layout, reconciliation rules, and the full lifecycle.

## 4. Schedule it

Pass a `trigger=` to the decorator to make the job cron-driven:

```py
from dlt.hub.run import trigger


@run.pipeline(
    "ingest_breweries",
    trigger=trigger.schedule("0 * * * *"),    # every hour, on the hour
)
def ingest_breweries():
    ...
```

Run `dlthub deploy` again to push the trigger to the platform. The scheduler then runs the job on its cron. Trigger factories also include `trigger.every("5m")` (fixed interval) and `trigger.once(...)` (single run). See [Basic triggers](../../hub/pipeline-operations/triggers.md#basic-triggers).

:::tip Adjust the schedule from the platform
You can also change the cron from the dltHub platform's **Manage Schedule** dialog, useful for ad-hoc pauses or tweaks without redeploying. To make a change permanent, update the decorator and run `dlthub deploy`.
:::

## Advanced features

- [Follow-up triggers](../../hub/pipeline-operations/triggers.md#follow-up-triggers): chain jobs by passing `.success` / `.fail` / `.completed` from one job as the trigger of another.
- [Freshness checks](../../hub/pipeline-operations/triggers.md#freshness-checks): let a job skip its scheduled run if upstream data isn't fresh.
- [Refresh cascade](../../hub/pipeline-operations/triggers.md#refresh-cascade): propagate a refresh signal downstream through the graph.
- [Tags and bulk triggering](../../hub/pipeline-operations/triggers.md#tags-and-bulk-triggering): group jobs and trigger them by selector.
- [Monitoring and debugging](../../hub/pipeline-operations/monitoring.md): runs, logs, and lineage from the command line or web UI.

## See also

- [Introduction to dltHub](../../hub/getting-started/introduction.md)
- [dltHub platform tutorial](../../hub/getting-started/platform-tutorial.md)
- [Triggers and scheduling](../../hub/pipeline-operations/triggers.md)
- [Deployments](../../hub/pipeline-operations/deployments.md)
