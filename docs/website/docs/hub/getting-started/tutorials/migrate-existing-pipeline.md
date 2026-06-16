---
title: Migrate an existing dlt pipeline to dltHub
description: Turn a working OSS dlt pipeline into a dltHub workspace and deploy it on the managed platform
keywords: [dlthub, migrate, existing pipeline, workspace mode, deploy, schedule, profiles]
---

# Migrate an existing dlt pipeline to dltHub

A dltHub **workspace** is just your existing project plus a marker file and a little config. This turns a working OSS
`dlt` pipeline into a workspace and deploys it on the managed platform — your pipeline code barely changes.

Starting from scratch instead? See [Deploy your first pipeline](deploy-your-first-pipeline.md).

:::note `dlthub-start` vs. this guide
[`dlthub-start`](deploy-your-first-pipeline.md) is for **exploring** the platform: it spins up a throwaway **playground**
workspace and an **AI workbench** that can build and deploy a pipeline for a brand-new source one-shot (for example, the
GitHub API into DuckDB). This guide is the other direction — bringing a `dlt` pipeline you already maintain onto the
platform, with minimal code changes and no scaffolding.
:::

## Starting point

A typical OSS `dlt` script, `sample_shop_pipeline.py`, loading a public API into BigQuery:

```py
import dlt
from dlt.sources.rest_api import rest_api_source


def sample_shop():
    return rest_api_source(
        {
            "client": {
                "base_url": "https://jaffle-shop.dlthub.com/api/v1/",
                "paginator": {"type": "header_link"},
            },
            "resources": [{"name": "customers"}, {"name": "products"}],
        }
    )


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="sample_shop_pipeline",
        destination="bigquery",
        dataset_name="sample_shop",
    )
    pipeline.run(sample_shop())
```

## 1. Install and enable workspace mode

Add the `hub` extra to your project:

```sh
uv pip install "dlt[hub]"
```

Then turn on workspace mode from your project root:

```sh
dlthub init
```

`dlthub init` creates the `.dlt/.workspace` marker plus `config.toml`, `secrets.toml`, and a `pyproject.toml` — your
pipeline script is left untouched. See [installation](../installation.md#enable-workspace-mode) for the manual marker.

## Or let your coding agent migrate it

Once `dlt[hub]` is installed, you can hand the migration to your AI assistant instead of doing
the steps below by hand. The `dlthub ai` subcommand is the bridge between the [dltHub AI Workbench](../../ingestion/rest-api-source.md)
and your coding assistant.

`dlthub ai init` installs project rules, a secrets-management skill, the appropriate ignore files, and configures the
dlt MCP server for your agent:

```sh
# set up AI support (auto-detects your coding assistant)
uv run dlthub ai init

# if multiple coding assistants are detected, specify one explicitly (claude / cursor / codex):
uv run dlthub ai init --agent <agent>
```

`dlthub ai toolkit install` copies additional toolkit components (skills, rules, commands) into the right locations for
your assistant. List the available toolkits and install the ones you need — if you're not sure, install all of them. The
**dlthub-platform** toolkit covers deployment and scheduling:

```sh
uv run dlthub ai toolkit list

uv run dlthub ai toolkit install dlthub-platform
```

Then prompt your agent, for example *"Migrate the dlt pipeline in `sample_shop_pipeline.py` to the dltHub platform and
deploy it."* It performs the same steps described below — profiles, a deployable job, and the deploy.

## 2. Connect and deploy ad-hoc

Log in, bind the directory to a remote workspace, and run your script in the cloud. The **ad-hoc** path uses the `prod`
profile and your existing destination credentials:

```sh
uv run dlthub login
uv run dlthub workspace connect
uv run dlthub run sample_shop_pipeline.py -f
```

Run it locally first to catch issues without a remote slot: `uv run dlthub local run sample_shop_pipeline.py`.

That's the migration. The steps below make it production-grade.

## 3. Separate dev and prod with profiles

[Profiles](../../pipeline-operations/profiles.md) let the same destination **alias** resolve to different credentials.
Reference an alias in code instead of a hardcoded destination:

```py
import dlt

pipeline = dlt.pipeline(
    pipeline_name="sample_shop_pipeline",
    destination="warehouse",   # resolved per profile
    dataset_name="sample_shop",
)
```

Then map it — cheap locally, real in production:

```toml
# .dlt/dev.config.toml
[destination.warehouse]
destination_type = "duckdb"
```

```toml
# .dlt/prod.config.toml
[destination.warehouse]
destination_type = "bigquery"
```

Put prod credentials in `.dlt/prod.secrets.toml` (gitignored). See
[Workspace setup](../../pipeline-operations/workspace-setup.md#credentials-and-configs).

## 4. Schedule it

Ad-hoc runs can't be scheduled. Wrap the run in a decorated job:

```py
import dlt
from dlt.hub import run
from dlt.hub.run import trigger
from dlt.sources.rest_api import rest_api_source


def sample_shop():
    return rest_api_source(
        {
            "client": {"base_url": "https://jaffle-shop.dlthub.com/api/v1/", "paginator": {"type": "header_link"}},
            "resources": [{"name": "customers"}, {"name": "orders"}, {"name": "products"}],
        }
    )


@run.pipeline("sample_shop_pipeline", trigger=trigger.schedule("0 * * * *"))
def load_sample_shop():
    pipeline = dlt.pipeline(
        pipeline_name="sample_shop_pipeline",
        destination="warehouse",
        dataset_name="sample_shop",
    )
    pipeline.run(sample_shop())
```
And declare it in `__deployment__.py`:
```py
# __deployment__.py
from sample_shop_pipeline import load_sample_shop

__all__ = ["load_sample_shop"]
```
Run:
```sh
uv run dlthub deploy
```

See [Deployments](../../pipeline-operations/deployments.md) and
[Triggers and scheduling](../../pipeline-operations/triggers.md).

## 7. Monitor and operate

Open the dashboard to watch runs, read logs, and query your destination:

```sh
uv run dlthub show
```

Or go to [app.dlthub.com](https://app.dlthub.com). See [Monitoring and debugging](../../pipeline-operations/monitoring.md).

## Next steps

- Add [transformations](../../transformations/index.md) that run after a successful load.
- Guard your pipeline with [data quality checks](../../data-quality/index.md).
- Explore the [advanced example](advanced-examples.md) for a full multi-job workspace with a dashboard.
