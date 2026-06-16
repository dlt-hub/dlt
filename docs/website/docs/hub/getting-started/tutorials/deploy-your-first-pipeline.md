---
title: Deploy your first pipeline with dltHub
description: Scaffold a fresh dltHub workspace and deploy a pipeline to the managed platform in a few minutes
keywords: [dlthub, deploy, first pipeline, getting started, workspace, dlthub-start]
---

# Deploy your first pipeline with dltHub

This tutorial takes you from nothing to a pipeline running on the managed dltHub platform. We scaffold a workspace with
`dlthub-start`, which sets up a runnable example pipeline **and runs your first deployment for you** as part of setup.
Then you'll inspect the run, point production at your own destination, and schedule it.

If you already have a working `dlt` pipeline you'd like to bring to the platform, follow
[Migrate an existing dlt pipeline](migrate-existing-pipeline.md) instead.

## What you will do

1. Run one command that scaffolds a workspace, deploys it to a managed **playground** workspace, and opens your coding agent.
2. Inspect the run in the dltHub UI.
3. Point your production profile at a cloud destination.
4. Schedule the pipeline.

## Prerequisites

- Python 3.10–3.13
- [uv](https://docs.astral.sh/uv/) (recommended). See [installation](../installation.md) for alternative install paths.



## 1. Scaffold and deploy in one command

A single command sets up everything — no arguments needed:

```sh
uvx dlthub-start@latest
```

`dlthub-start` runs the whole first-time workflow for you, in order:

1. **Scaffolds a workspace** into the current directory — falling back to a `./playground` folder when the directory isn't empty.
2. **Installs dependencies** with `uv sync` (`dlt[hub]` plus the workspace's `pyproject.toml`).
3. **Runs your first deployment** on the managed platform:
   - logs you in (`dlthub login`),
   - connects the workspace to a managed **playground** workspace (`dlthub workspace connect playground`),
   - runs the example pipeline and streams the logs (`dlthub run --follow load_sample_shop`),
   - opens your workspace overview (`dlthub show`) in the dltHub platform.
4. **Hands off to your coding agent** — only now does it prompt you to pick an agent (Claude / Cursor / Codex), wire up
   its workbench files, and launch it seeded with a starter prompt so you can build a pipeline for your own source, for
   example:
   > Build a dlt pipeline for the [API name] API and load [endpoint/data] into DuckDB.

By the time the command finishes, you already have a pipeline that ran on the managed platform — and your coding agent
is open, ready to build your own. The rest of this tutorial explains what just happened and how to make it your own.


## 2. See what you got

The scaffold is a complete, runnable [workspace](../installation.md#what-is-a-dlthub-workspace):

```text
playground/
├── pyproject.toml      # dlt[hub] + workspace dependencies
├── pipeline.py         # the Sample Shop pipeline (public API, no auth, no secrets)
├── __deployment__.py   # declares the deployable jobs in the workspace
└── .dlt/               # config, secrets, and the .workspace marker
```

`pipeline.py` loads a public sample online-shop API into a local DuckDB warehouse. It runs without API keys or signups,
so it can be deployed as-is and swapped for your own source later. The pipeline is already a deployable job:

```py
import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.hub import run


def sample_shop():
    return rest_api_source(
        {
            "client": {
                "base_url": "https://jaffle-shop.dlthub.com/api/v1/",
                "paginator": {"type": "header_link"},
            },
            "resources": [
                {"name": "customers", "primary_key": "id"},
                {"name": "orders", "primary_key": "id"},
                {"name": "products", "primary_key": "sku"},
            ],
        }
    )


@run.pipeline("sample_shop_pipeline")
def load_sample_shop():
    """Load sample shop data from the public REST API."""
    pipeline = dlt.pipeline(
        pipeline_name="sample_shop_pipeline",
        destination="warehouse",
        dataset_name="sample_shop",
    )
    pipeline.run(sample_shop().add_limit(1))
```

`__deployment__.py` lists the jobs that get deployed together — here, just `load_sample_shop`:

```py
"""Minimal dltHub workspace."""

from pipeline import load_sample_shop

__all__ = ["load_sample_shop"]
```

You can read more about this manifest in [Deployments](../../pipeline-operations/deployments.md).

## 3. Inspect and re-run

`dlthub show` (which the scaffolder opened for you) is the dltHub web UI — your jobs, runs, logs, and a SQL interface
over the destination. Reopen it any time:

```sh
cd playground
uv run dlthub show
```

Or go to [app.dlthub.com](https://app.dlthub.com). Trigger the pipeline again, and view its run history:

```sh
# run the job on the platform
uv run dlthub run load_sample_shop

# view runs for this pipeline
uv run dlthub job runs show pipeline.load_sample_shop
```

Before deploying anything, you can always rehearse locally first — it uses the `dev` profile and catches missing
dependencies or broken config without spending a remote slot:

```sh
uv run dlthub local run load_sample_shop
```

:::note The playground workspace
`dlthub-start` connects you to a shared **playground** workspace so you can see a real run immediately. For your own
project, create a dedicated workspace with `uv run dlthub workspace connect <name> --create` and bind to it.
:::

## 4. Point production at a cloud destination

The scaffold's destination, aliased `warehouse`, is local DuckDB. That's perfect for development but not for a deployed
pipeline — DuckDB on the platform is ephemeral. Configure the `prod` profile to write to a cloud destination you control
(for example [MotherDuck](../../../dlt-ecosystem/destinations/motherduck.md),
[BigQuery](../../../dlt-ecosystem/destinations/bigquery.md),
[Snowflake](../../../dlt-ecosystem/destinations/snowflake.md), or
[filesystem / S3](../../../dlt-ecosystem/destinations/filesystem.md)). Keep the `warehouse` alias so your code doesn't
change — only the configuration does.

**`.dlt/prod.config.toml`** (settings for batch jobs running on dltHub):

```toml
[destination.warehouse]
destination_type = "motherduck"
```

**`.dlt/prod.secrets.toml`** (read/write credentials — never committed):

```toml
[destination.warehouse.credentials]
database = "your_database"
password = "your-service-token"
```

Files matching `*.secrets.toml` are gitignored by default. dltHub stores your secrets securely when you sync your
configuration. For the full credentials model — including the read-only `access` profile used by interactive notebooks —
see [Workspace setup](../../pipeline-operations/workspace-setup.md#credentials-and-configs) and
[Profiles](../../pipeline-operations/profiles.md).

Rehearse the production config locally before deploying:

```sh
uv run dlthub local profile use prod
uv run dlthub local run load_sample_shop
```

## 5. Schedule the pipeline

Scheduling is declarative — add a trigger to the job's decorator and redeploy. To run the pipeline every 10 minutes,
edit `pipeline.py`:

```py
from dlt.hub import run
from dlt.hub.run import trigger


@run.pipeline("sample_shop_pipeline", trigger=trigger.schedule("*/10 * * * *"))
def load_sample_shop():
    ...
```

Then deploy the whole workspace from its `__deployment__.py`:

```sh
uv run dlthub deploy
```

`dlthub deploy` reads `__deployment__.py`, syncs your code and configuration, and reconciles the jobs on the platform.
Preview without applying using `--dry-run`. To stop a schedule, remove the trigger and redeploy. The full catalog of
triggers — cron, intervals, follow-up chains, freshness, and scheduler-driven incremental windows — is in
[Triggers and scheduling](../../pipeline-operations/triggers.md), and the deploy/reconcile model is in
[Deployments](../../pipeline-operations/deployments.md).

## Next steps

- [Migrate an existing dlt pipeline](migrate-existing-pipeline.md) to the platform.
- Swap the sample source for your own — edit `pipeline.py` or ask your coding agent to [build a pipeline](../../ingestion/init.md) for your source.
- Reshape your data with [Transformations](../../transformations/index.md).
- Add [data quality checks](../../data-quality/index.md) to catch issues early.
- Work through the [advanced example](advanced-examples.md) — a full starter pack with pipelines, transformations, and a dashboard.
