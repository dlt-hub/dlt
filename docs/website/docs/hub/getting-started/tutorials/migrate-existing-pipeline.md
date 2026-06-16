---
title: Migrate an existing dlt pipeline to dltHub
description: Turn a working OSS dlt pipeline into a dltHub workspace and deploy it on the managed platform
keywords: [dlthub, migrate, existing pipeline, workspace mode, deploy, schedule, profiles]
---

# Migrate an existing dlt pipeline to dltHub

Already running a `dlt` pipeline as a plain Python script? You don't need to rewrite it to use dltHub. A dltHub
**workspace** is just your existing project plus a marker file and a bit of configuration. This guide turns a working OSS
`dlt` pipeline into a workspace and deploys it on the managed platform — your pipeline code stays the same.

If you're starting from scratch instead, follow
[Deploy your first pipeline](deploy-your-first-pipeline.md), which scaffolds a ready-made workspace.

## What you will do

1. Install the `hub` extra into your existing project.
2. Enable workspace mode.
3. Move your destination credentials into profiles.
4. Run the pipeline locally to confirm nothing broke.
5. Connect to dltHub and deploy it ad-hoc.
6. Wrap the pipeline in a job and schedule it.

## Starting point

Assume you have a typical OSS `dlt` script, `sample_shop_pipeline.py`, that loads a public sample online-shop API into
BigQuery:

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
            "resources": [
                {"name": "customers", "primary_key": "id"},
                {"name": "orders", "primary_key": "id"},
                {"name": "products", "primary_key": "sku"},
            ],
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

This runs fine with `python sample_shop_pipeline.py`. Let's bring it to the platform.

## 1. Install dlt[hub]

Create the project's virtual environment and add the `hub` extra:

```sh
uv pip install "dlt[hub]"
```

See [installation](../installation.md#add-dlthub-to-an-existing-project) for version-pinning details.

## 2. Enable workspace mode

The dltHub feature surface — profiles, the `dlthub` CLI, and the managed-platform commands — is gated behind a
`.dlt/.workspace` marker file. Turn it on from your project root:

```sh
dlthub init
```

This scaffolds the workspace pieces around your existing code: it creates the `.dlt/.workspace` marker plus
`config.toml`, `secrets.toml`, a `.gitignore`, and a `pyproject.toml` (or `requirements.txt`). Your pipeline script is
left untouched.

:::tip
If you'd rather flip the switch by hand without scaffolding anything, just create the empty marker:

```sh
mkdir -p .dlt && touch .dlt/.workspace
```

See [Enable workspace mode](../installation.md#enable-workspace-mode) for the per-OS commands.
:::

## 3. Refer to your destination by name

On the platform, jobs run under [profiles](../../pipeline-operations/profiles.md) — `dev` for local development, `prod`
for deployed batch jobs, and `access` for interactive read-only sessions. Each profile can point the same destination
**alias** at different credentials, so your code references a name instead of hardcoding a destination.

Give your destination an alias in your pipeline by replacing the destination type with a name:

```py
pipeline = dlt.pipeline(
    pipeline_name="sample_shop_pipeline",
    destination="warehouse",   # an alias resolved per profile
    dataset_name="sample_shop",
)
```

Then define what `warehouse` resolves to per profile. For local development, keep it cheap — DuckDB:

**`.dlt/dev.config.toml`**

```toml
[destination.warehouse]
destination_type = "duckdb"
```

For production, point it at your real cloud destination:

**`.dlt/prod.config.toml`**

```toml
[destination.warehouse]
destination_type = "bigquery"
```

**`.dlt/prod.secrets.toml`** (gitignored — never committed):

```toml
[destination.warehouse.credentials]
project_id = "your-project"
private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
client_email = "loader@your-project.iam.gserviceaccount.com"
```

dltHub stores your secrets securely when you sync configuration. For the full credentials model — including the
read-only `access` profile used by notebooks — see
[Workspace setup](../../pipeline-operations/workspace-setup.md#credentials-and-configs).

:::note
Keeping a destination alias is a convention, not a requirement. If you prefer to leave `destination="bigquery"` in code,
you can still scope credentials per profile under `[destination.bigquery.credentials]`. The alias just makes it easy to
use DuckDB locally and a warehouse in production without touching code.
:::

## 4. Run locally first

Confirm the pipeline still works through the `dlthub` CLI before going remote. The `dlthub local …` scope runs everything
on your machine using local profiles (default `dev`):

```sh
# runs against the dev profile (local DuckDB)
uv run dlthub local run sample_shop_pipeline.py

# inspect the locally loaded data
uv run dlthub local show
```

To rehearse the production path locally, pin the `prod` profile (this reads/writes your real credentials and dataset):

```sh
uv run dlthub local profile use prod
uv run dlthub local run sample_shop_pipeline.py
```

## 5. Connect and deploy ad-hoc

Log in and bind this directory to a remote workspace:

```sh
uv run dlthub login
uv run dlthub workspace connect
```

The fastest way to run your existing script in the cloud is an **ad-hoc launch** — point `run` at the file and follow
the logs (this uses the `prod` profile):

```sh
uv run dlthub run sample_shop_pipeline.py -f
```

Under the hood the CLI generates a single-job deployment from that file and syncs it to the platform. Ad-hoc launch is
great for a first run, but it doesn't support scheduling, follow-up jobs, or multi-job workspaces. For those, declare a
job — next step.

## 6. Turn the pipeline into a scheduled job

To schedule the pipeline (or chain other jobs after it), wrap the run in a decorated function and declare it in a
deployment manifest. Decorate the entry point with `@run.pipeline` and attach a trigger:

```py
import dlt
from dlt.hub import run
from dlt.hub.run import trigger
from dlt.sources.rest_api import rest_api_source


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


@run.pipeline(
    "sample_shop_pipeline",
    trigger=trigger.schedule("0 * * * *"),  # hourly
    expose={"tags": ["ingest"], "display_name": "Sample shop ingest"},
)
def load_sample_shop():
    pipeline = dlt.pipeline(
        pipeline_name="sample_shop_pipeline",
        destination="warehouse",
        dataset_name="sample_shop",
    )
    pipeline.run(sample_shop())
```

Declare the job in `__deployment__.py` at the workspace root — the platform discovers jobs by inspecting this module:

```py
"""Sample shop workspace -- ingests data from the Sample Shop API."""

from sample_shop_pipeline import load_sample_shop

__all__ = ["load_sample_shop"]
```

Then deploy the whole workspace:

```sh
uv run dlthub deploy
```

`dlthub deploy` reads `__deployment__.py`, generates a manifest, syncs your code and configuration, and reconciles the
jobs on the platform. Preview changes first with `dlthub deploy --dry-run`. The full model — job decorators, the manifest,
reconciliation, and versioning — is in [Deployments](../../pipeline-operations/deployments.md), and every trigger type is
in [Triggers and scheduling](../../pipeline-operations/triggers.md).

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
