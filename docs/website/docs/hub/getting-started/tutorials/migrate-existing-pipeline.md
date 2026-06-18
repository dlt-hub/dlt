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
Two different starting points:

- [**`dlthub-start`**](deploy-your-first-pipeline.md) — for **exploring** the platform. Spins up a throwaway
  **playground** workspace and an **AI workbench** that builds and deploys a pipeline for a brand-new source one-shot
  (for example, the GitHub API into DuckDB).
- **This guide** — the other direction: bringing a `dlt` pipeline you **already maintain** onto the platform, with
  minimal code changes and no scaffolding.
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

From your project root, run:

```sh
uvx dlthub-init@latest
```

`dlthub-init` scaffolds a workspace in place — your pipeline script is left untouched. It:

- installs `dlt[hub]`;
- creates the `.dlt/.workspace` marker plus `config.toml`, `secrets.toml`, and a `pyproject.toml`;
- sets up the AI skills your coding agent uses.

See [Add dltHub to an existing project](../installation.md#add-dlthub-to-an-existing-project) for details, or
[Enable workspace mode](../installation.md#enable-workspace-mode) for the manual marker.

## Or let your coding agent migrate it

Step 1 already set up the AI skills your coding agent uses, so you can hand the migration to your AI assistant instead
of doing the steps below by hand. The `dlthub ai` subcommand is the bridge between the
[dltHub AI Workbench](../../ingestion/rest-api-source.md) and your coding assistant.

If you skipped the scaffolding above or want to re-run AI setup on its own, `dlthub ai init` installs project rules, a
secrets-management skill, the appropriate ignore files, and configures the dlt MCP server for your agent:

```sh
# set up AI support (auto-detects your coding assistant)
uv run dlthub ai init

# if multiple coding assistants are detected, specify one explicitly (claude / cursor / codex):
uv run dlthub ai init --agent <agent>
```

`dlthub ai toolkit install` copies additional toolkit components (skills, rules, commands) into the right locations for
your assistant:

- List the available toolkits and install the ones you need — if you're not sure, install all of them.
- The **dlthub-platform** toolkit covers deployment and scheduling.

```sh
uv run dlthub ai toolkit list

uv run dlthub ai toolkit install dlthub-platform
```

Then prompt your agent, for example:

> *"Deploy the dlt pipeline in `sample_shop_pipeline.py` to the dltHub platform."*

It performs the same steps described below — destination config, a deployable job, and the deploy.

## 2. Connect and deploy ad-hoc

The **ad-hoc** path uses the `prod` profile and your existing destination credentials. Three commands:

1. **Log in** to the platform.
2. **Connect** the directory to a remote workspace.
3. **Run** your script in the cloud.

```sh
uv run dlthub login
uv run dlthub run sample_shop_pipeline.py -f
```

:::tip
Run it locally first to catch issues without a remote slot: `uv run dlthub local run sample_shop_pipeline.py`.
:::

That's the migration. The steps below make it production-grade.

## 3. Configure your destination

Reference a destination **alias** in your code instead of a hardcoded destination, so the same script can resolve to
different credentials:

```py
import dlt

pipeline = dlt.pipeline(
    pipeline_name="sample_shop_pipeline",
    destination="warehouse",   # resolved from config
    dataset_name="sample_shop",
)
```

Set the destination type and credentials in `.dlt/secrets.toml` (gitignored):

```toml
[destination.warehouse]
destination_type = "bigquery"

[destination.warehouse.credentials]
project_id = "your_project_id"
private_key = "your_private_key"
client_email = "your_service_account_email"
```

To keep separate settings for local development and production, use
[profiles](../../pipeline-operations/profiles.md).

## 4. Schedule it

Ad-hoc runs can't be scheduled. Wrap the run in a decorated job:

```py
import dlt
from dlt.hub import run
from dlt.hub.run import trigger
from dlt.sources.rest_api import rest_api_source


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

## 5. Monitor and operate

Open the dashboard to watch runs, read logs, and query your destination:

```sh
uv run dlthub show
```

Or go to [app.dlthub.com](https://app.dlthub.com). See [Monitoring and debugging](../../pipeline-operations/monitoring.md).

## Next steps

- Add [transformations](../../transformations/index.md) that run after a successful load.
- Guard your pipeline with [data quality checks](../../data-quality/index.md).
- Explore the [advanced example](../platform-tutorial.md) for a full multi-job workspace with a dashboard.
