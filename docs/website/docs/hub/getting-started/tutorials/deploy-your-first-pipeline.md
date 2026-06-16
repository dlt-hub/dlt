---
title: Deploy your first pipeline with dltHub
description: Scaffold a fresh dltHub workspace and deploy a pipeline to the managed platform in a few minutes
keywords: [dlthub, deploy, first pipeline, getting started, workspace, dlthub-start]
---

# Deploy your first pipeline with dltHub

Go from nothing to a pipeline running on the managed dltHub platform. `dlthub-start` scaffolds a workspace **and runs
your first deployment for you**.

Migrating an existing `dlt` pipeline instead? See [Migrate an existing dlt pipeline](migrate-existing-pipeline.md).

## Prerequisites

- Python 3.10–3.13
- [uv](https://docs.astral.sh/uv/) (recommended). See [installation](../installation.md) for alternative install paths.


## 1. Scaffold and deploy in one command

```sh
uvx dlthub-start@latest
```

No arguments needed — `dlthub-start` runs the whole first-time workflow:

1. **Scaffolds a workspace** into the current directory (or a `./playground` folder if it isn't empty).
2. **Installs dependencies** with `uv sync`.
3. **Runs your first deployment** — logs you in, connects to a managed **playground** workspace, runs the example
   pipeline (`dlthub run --follow load_sample_shop`), and opens the dashboard (`dlthub show`).
4. **Opens your coding agent** (Claude / Cursor / Codex), seeded with a prompt to build a pipeline for your own source.

By the time it finishes, a pipeline has already run on the platform.

:::tip Explore first, then build your own
`dlthub-start` is the quickest way to explore dltHub end-to-end — it spins up a throwaway **playground** workspace and
wires an **AI workbench** into your coding agent. Ask the agent to build a pipeline for any source one-shot (for example,
*"load the GitHub API into DuckDB"*) and it will write **and deploy** it from scratch. Already have a `dlt` pipeline you
maintain? See [Migrate an existing dlt pipeline](migrate-existing-pipeline.md) instead.
:::

## 2. What you got

```text
playground/
├── pyproject.toml      # dlt[hub] + dependencies
├── pipeline.py         # the Sample Shop pipeline (public API, no auth)
├── __deployment__.py   # declares the deployable jobs
└── .dlt/               # config, secrets, and the .workspace marker
```

`pipeline.py` loads a public sample API into a local DuckDB warehouse. It's already a deployable job — decorated with
`@run.pipeline` and listed in `__deployment__.py`. See [Deployments](../../pipeline-operations/deployments.md) for how
the manifest works.

## 3. Re-run and inspect

```sh
cd playground

uv run dlthub run load_sample_shop   # run on the platform
uv run dlthub show                   # open the dashboard
```

Rehearse locally first — `uv run dlthub local run load_sample_shop` uses the `dev` profile and spends no remote slot.

## 4. Point production at a cloud destination

The scaffold writes to local DuckDB, which is ephemeral on the platform. Point the `prod` profile at a cloud destination
([MotherDuck](../../../dlt-ecosystem/destinations/motherduck.md),
[BigQuery](../../../dlt-ecosystem/destinations/bigquery.md),
[Snowflake](../../../dlt-ecosystem/destinations/snowflake.md),
[S3](../../../dlt-ecosystem/destinations/filesystem.md)). Keep the `warehouse` alias so your code doesn't change.

**`.dlt/prod.config.toml`**

```toml
[destination.warehouse]
destination_type = "motherduck"
```

**`.dlt/prod.secrets.toml`** (gitignored)

```toml
[destination.warehouse.credentials]
database = "your_database"
password = "your-service-token"
```

See [Workspace setup](../../pipeline-operations/workspace-setup.md#credentials-and-configs) and
[Profiles](../../pipeline-operations/profiles.md) for the full credentials model.

## 5. Schedule the pipeline

Add a trigger to the job and redeploy:

```py
from dlt.hub import run
from dlt.hub.run import trigger


@run.pipeline("sample_shop_pipeline", trigger=trigger.schedule("*/10 * * * *"))
def load_sample_shop():
    ...
```

```sh
uv run dlthub deploy
```

See [Triggers and scheduling](../../pipeline-operations/triggers.md) for cron, intervals, and follow-ups.

## Next steps

- [Migrate an existing dlt pipeline](migrate-existing-pipeline.md)
- [Transformations](../../transformations/index.md) · [Data quality](../../data-quality/index.md) · [Advanced example](advanced-examples.md)
