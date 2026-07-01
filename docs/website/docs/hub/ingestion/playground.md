---
title: "Destination: Playground"
description: Zero-config Playground destination for non-production example pipelines on the dltHub platform
keywords: [playground, destination, delta, testing, experiments]
---

# Playground

:::note
Use of the dltHub platform and toolkits is subject to a commercial dltHub License.
:::

The **Playground** is a zero-config destination managed by the dltHub platform. When you run pipelines on the platform, you can load data with `destination="playground"` without configuring any credentials or storage of your own. The platform provisions isolated storage for each [workspace](../getting-started/installation.md#what-is-a-dlthub-workspace) and writes your data as [Delta tables](delta.md) to a managed S3 bucket.

It is meant for quick testing, demos, and a fast first run on the platform.

:::warning
The Playground is intended for demos and a quick start, not for production. For production workloads, use a destination you own and control, such as [Delta](delta.md), [Iceberg](iceberg.md), [Snowflake Plus](snowflake-plus.md), or any of the [dlt destinations](../../dlt-ecosystem/destinations/index.md).
:::

## Why use it

When you run a pipeline on the platform with a local-style destination such as `duckdb`, the data is written to the dltHub's ephemeral storage, which is erased after the run, so it can't be explored afterwards. The Playground instead **persists** your data in managed storage, so you can query it from the platform UI once the run finishes.

Use it for:

* A quick first run on the dltHub platform without setting up a bucket or credentials.
* Running examples and demos.

:::warning
Do not load sensitive or confidential data into the Playground.
:::

## Prerequisites

* A dltHub [workspace](../getting-started/installation.md). If you don't have one yet, scaffold it with `uvx dlthub-start@latest` (see the [installation guide](../getting-started/installation.md)).
* You must be logged in and connected to a workspace:

  ```sh
  uv run dlthub login
  uv run dlthub workspace connect <workspace-name>
  ```

* The `deltalake` package must be installed in your project. The Playground writes Delta tables, and the runtime image installs your project dependencies, so add it to your `pyproject.toml`:

  ```sh
  uv add deltalake
  ```

## Usage

Set the destination to `playground` in your pipeline and declare it as a job so the platform can deploy and run it. No `bucket_url`, credentials, or other configuration is required.

Decorate your pipeline function with `@run.pipeline` in `data_pipeline.py`:

```py
import dlt
from dlt.hub import run

@run.pipeline("pipeline")
def load_data():
    pipeline = dlt.pipeline(
        pipeline_name="pipeline",
        destination="playground",
        dataset_name="data",
    )
    pipeline.run([{"id": 1}], table_name="items")
```

Declare the job in `__deployment__.py` so the platform can discover it:

```py
from data_pipeline import load_data

__all__ = ["load_data"]
```

Deploy the workspace, then run the job on the platform:

```sh
uv run dlthub deploy
uv run dlthub pipeline run pipeline -f
```

See [deployments](../pipeline-operations/deployments.md) for more on the `__deployment__.py` manifest, `@run.pipeline`, and deploying jobs on the platform.

## Working with the data

Once a run completes, open the platform dashboard to explore the persisted data. It includes a SQL query editor against your dataset:

```sh
uv run dlthub dashboard
```

:::note
The platform dashboard is itself a deployed job, provisioned when you run `dlthub deploy` with a `__deployment__.py` manifest. The local dashboard (`uv run dlthub local show`) does not require a manifest.
:::

## How it works

The Playground behaves like the [Delta destination](delta.md): it is a `filesystem` destination that writes Delta tables to a dltHub-managed S3 bucket. Each workspace gets its own isolated prefix (`s3://.../<org_id>/<workspace_id>/...`), so data from different workspaces never mixes. Storage and write dispositions follow the behavior of the [Delta destination](delta.md).

dlt is destination-agnostic, so anything you prototype against the Playground can later be moved to any destination you own with minimal changes. You swap the destination and provide your own storage and credentials.
