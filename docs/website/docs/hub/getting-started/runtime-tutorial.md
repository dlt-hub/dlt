---
title: Deploy trusted dlt pipelines and dashboards
description: Tutorial walking through deployment on the managed dltHub Platform
keywords: [deployment, dlthub, dashboard, dlt pipeline]
---

With dltHub you can not only build data ingestion pipelines and dashboards, but also **run and manage them on a fully managed cloud platform**.
See the [Platform overview](../runtime/overview.md) for more details. You get:

- the flexibility and developer experience of dlt
- the simplicity and reliability of managed infrastructure

## What you will learn

In this tutorial you will:

- Deploy a dlt pipeline on the managed dltHub Platform
- Deploy an always-fresh dashboard on the managed dltHub Platform
- Add Python transformations to your ELT jobs

## Prerequisites

- Python 3.12+
- A [MotherDuck](https://motherduck.com) account (for the starter pack example)
- [uv](https://docs.astral.sh/uv/) package manager (recommended for dependency management)

## Quickstart

To make things easier, we provide a starter repository with a preconfigured dltHub project. It contains a working source, pipeline, transformations, and a small dashboard so you can focus on learning the platform rather than setting everything up from scratch.

This starter pack includes:

1. A dlt pipeline that loads data from the jaffle shop API into a local DuckDB destination.
2. A remote destination configured as MotherDuck. You can swap it for any other cloud destination you prefer (for example
   [BigQuery](../../dlt-ecosystem/destinations/bigquery.md),
   [Snowflake](../../dlt-ecosystem/destinations/snowflake.md),
   [AWS S3](../../dlt-ecosystem/destinations/filesystem.md), …).
3. A simple Marimo dashboard that you can use to explore and analyze the data.
4. A set of custom transformations that are executed after the raw data is loaded.

We'll walk through cloning the repo, installing dependencies, connecting to dltHub, and then deploying both pipelines and dashboards.

### 1. Clone the starter pack

```sh
git clone https://github.com/dlt-hub/runtime-starter-pack.git
cd runtime-starter-pack
```

### 2. Install dependencies and activate the environment

Each workspace inside the starter pack comes with its own `pyproject.toml` that pulls in `dlt[hub]` plus workspace-specific extras. Install everything with `uv`:

```sh
cd jaffle_shop_workspace
uv sync
source .venv/bin/activate
```

### 3. Configure your credentials

Configure your destination credentials. The starter pack uses MotherDuck as the destination, but you can switch to any other destination you prefer.
Details on configuring credentials for the dltHub Platform are available [here](../runtime/workspace-setup.md#credentials-and-configs).
Make sure your destination credentials are valid before running pipelines remotely. Below you can find instructions for configuring credentials for the MotherDuck destination.

**`prod.config.toml`** (for batch jobs running on dltHub):

```toml
[destination.fruitshop_destination]
destination_type = "motherduck"
```

**`prod.secrets.toml`** (for batch jobs - read/write credentials):

```toml
[destination.fruitshop_destination.credentials]
database = "your_database"
password = "your-motherduck-service-token"  # Read/write token
```

**`access.config.toml`** (for interactive notebooks):

```toml
[destination.fruitshop_destination]
destination_type = "motherduck"
```

**`access.secrets.toml`** (for interactive notebooks - read-only credentials):

```toml
[destination.fruitshop_destination.credentials]
database = "your_database"
password = "your-motherduck-read-only-token"  # Read-only token
```

:::tip Getting MotherDuck Credentials
1. Sign up at [motherduck.com](https://motherduck.com)
2. Go to Settings > Service Tokens
3. Create two tokens:
   - A **read/write** token for the `prod` profile
   - A **read-only** token for the `access` profile
:::

:::warning Security
Files matching `*.secrets.toml` and `secrets.toml` are gitignored by default. Never commit secrets to version control. dltHub stores your secrets securely when you sync your configuration.
:::

### 4. Connect to dltHub

Authentication is split into two steps: log in (identity) and connect this workspace directory to a remote workspace.

```sh
# 1. log in (OAuth device flow)
uv run dlthub login

# 2. bind the current workspace directory to a remote workspace
uv run dlthub workspace connect
```

`dlthub workspace connect` writes `workspace_id` (and on the first connect, `organization_id`) into `.dlt/config.toml`. Pass `<name_or_id>` to bind to a specific workspace, or omit it for an interactive picker grouped by organization.

:::tip
The first time you run `dlthub deploy`, `dlthub run`, or `dlthub serve`, the CLI auto-prompts both `login` and `workspace connect` if they haven't been done yet—so you can skip step 4 entirely if you don't mind doing it inline.

For a full list of available commands, see the [CLI reference](../command-line-interface.md).
:::

### Local vs remote scopes

The `dlthub` CLI is split into two scopes:

- **local**—`dlthub local …` runs everything on your machine using local profiles (default `dev`).
- **remote**—`dlthub …` (unqualified) operates on the connected dltHub workspace.

Run the local form first to catch missing dependencies or misconfigured destinations without burning a remote slot.

### Job types

dltHub runs two kinds of jobs:

- **Batch jobs**—Python scripts that run once or on a schedule. Trigger with `dlthub run <script_or_job>`. Use case: ELT pipelines, transformation runs, backfills. Runs with the `prod` profile.
- **Interactive jobs**—long-running processes that serve a notebook or app. Trigger with `dlthub serve <script>`. Use case: Marimo notebooks, dashboards, Streamlit apps, MCP servers. Runs with the `access` profile.

### 5. Run your first pipeline

Smoke-test the pipeline locally, then deploy it:

```sh
# test locally first (uses the `dev` profile)
uv run dlthub local run fruitshop_pipeline.py

# deploy and run remotely (uses the `prod` profile)
uv run dlthub run fruitshop_pipeline.py
```

`dlthub run`:

1. Uploads your code and configuration to dltHub.
2. Creates and starts a batch job.
3. Returns immediately. Add `-f` to follow logs in your terminal until completion:

```sh
uv run dlthub run fruitshop_pipeline.py -f
```

### 6. Open an interactive notebook

```sh
# serve locally first
uv run dlthub local serve fruitshop_notebook.py

# deploy and serve remotely
uv run dlthub serve fruitshop_notebook.py
```

The remote command:

1. Uploads your code and configuration.
2. Starts an interactive notebook session using the `access` profile.
3. Opens the notebook in your browser.

:::note
Interactive notebooks use the `access` profile with read-only credentials, so they are safe for data exploration and dashboarding without the risk of accidental writes.
Read more about profiles in the [profiles documentation](../core-concepts/profiles-dlthub.md).
:::

Interactive jobs are the building block for serving notebooks, dashboards, Streamlit, or similar apps. You can share links to these interactive jobs with your colleagues for collaborative exploration.

### 7. Schedule a pipeline

Scheduling is declarative—define the trigger in code with `@run.pipeline` (or `@run.job`) and redeploy. A pipeline that runs every 10 minutes:

```py
import dlt
from dlt.hub import run
from dlt.hub.run import trigger

@run.pipeline(
    "fruitshop_pipeline",
    trigger=trigger.schedule("*/10 * * * *"),
)
def load_fruitshop():
    pipeline = dlt.pipeline(
        pipeline_name="fruitshop_pipeline",
        destination="fruitshop_destination",
        dataset_name="fruitshop_data",
    )
    pipeline.run(fruitshop())
```

Wire the decorated function into `__deployment__.py` and deploy with:

```sh
uv run dlthub deploy
```

To stop a schedule, remove the trigger from the decorator (or remove the job from `__deployment__.py`) and redeploy. See the [Deployments](../runtime/deploying.md#jobs-and-deployments) page for the full story on jobs and deployments.

## Review and manage jobs in the UI

The command line is great for development, but the dltHub web UI gives you a bird's-eye view of everything running in the cloud.
Visit [dlthub.app](https://dlthub.app)—or open it from the CLI with `uv run dlthub show`—to access the dashboard. You will find:

1. A list of existing jobs.
2. An overview of scheduled runs.
3. Visibility into interactive sessions.
4. Management actions and workspace settings.

### Pipelines and data access in the Dashboard

The dltHub Dashboard lets you see all your pipelines and job runs, inspect job metadata (status, start time, duration, logs, etc.), and access the data in your destination via a SQL interface.
This makes it easy to debug issues, check the health of your pipelines, and quickly validate the data that has been loaded.

### Public links for interactive jobs

Interactive jobs such as notebooks and dashboards can be shared via public links. To manage public links:

1. Open the context menu of a job in the job list or navigate to the job detail page.
2. Click "Manage Public Link".
3. Enable the link to generate a shareable URL, or disable it to revoke access.

Anyone with an active public link can view the running notebook or dashboard, even if they don't have direct dltHub access. This is ideal for sharing dashboards with stakeholders, business users, or other teams.

You can also generate / revoke a public link from the CLI:

```sh
uv run dlthub job publish path/to/notebook.py
uv run dlthub job unpublish path/to/notebook.py
```

## Add transformations

Raw ingested data is rarely enough. Transformations let you reshape, enrich, and prepare data for analytics and downstream tools. Transformations are useful when you want to
aggregate raw data into reporting tables, join multiple tables into enriched datasets, create dimensional models for analytics, and apply business logic to normalize or clean data.

dltHub Transformations let you build new tables or entire datasets from data that has already been ingested using dlt.

Key characteristics:

1. Defined in Python functions decorated with `@dlt.hub.transformation`.
2. Can use Python (via Ibis) or pure SQL.
3. Operate on the destination dataset (`dlt.Dataset`).
4. Executed on the destination compute or locally via DuckDB.

You can find full details in the [Transformations](../features/transformations/index.md) documentation. Below are a few core patterns to get you started.

### Basic example with Ibis

Use the `@dlt.hub.transformation` decorator to define transformations. The function must accept a `dlt.Dataset` parameter and yield an Ibis table expression or SQL query.

```py
import dlt
import typing
from ibis import ir

@dlt.hub.transformation
def customer_orders(dataset: dlt.Dataset) -> typing.Iterator[ir.Table]:
    """Aggregate statistics about previous customer orders"""
    orders = dataset.table("orders").to_ibis()
    yield orders.group_by("customer_id").aggregate(
        first_order=orders.ordered_at.min(),
        most_recent_order=orders.ordered_at.max(),
        number_of_orders=orders.id.count(),
    )
```

This transformation reads the `orders` table from the destination, aggregates per customer, and yields a result that can be materialized as a new table.

### Joining multiple tables

You can join multiple tables and then aggregate or reshape the data:

```py
import dlt
import typing
import ibis
from ibis import ir

@dlt.hub.transformation
def customer_payments(dataset: dlt.Dataset) -> typing.Iterator[ir.Table]:
    """Customer order and payment info"""
    orders = dataset.table("orders").to_ibis()
    payments = dataset.table("payments").to_ibis()
    yield (
        payments.left_join(orders, payments.order_id == orders.id)
        .group_by(orders.customer_id)
        .aggregate(total_amount=ibis._.amount.sum())
    )
```
Here, we join `payments` with `orders` and aggregate total payment amounts per customer.

### Using Pure SQL

If you prefer, you can also write transformations as raw SQL:

```py
@dlt.hub.transformation
def enriched_purchases(dataset: dlt.Dataset) -> typing.Any:
    yield dataset(
        """
        SELECT customers.name, purchases.quantity
        FROM purchases
        JOIN customers
            ON purchases.customer_id = customers.id
        """
    )
```

This is a good option if your team is more comfortable with SQL or you want to port existing SQL models.

### Running transformations locally

The starter pack includes a predefined `jaffle_transformations.py` script that:

1. Combines two resources: data from the jaffle shop API and payments stored in parquet files.
2. Loads them into a local DuckDB (default dev profile).
3. Creates aggregations and loads them into the remote destination.

To run transformations locally (using the default `dev` profile):

```sh
uv run dlthub local run jaffle_transformations.py
```

### Running with the production profile

To run the same transformations against your production destination, pin the `prod` profile first:

```sh
uv run dlthub local profile use prod
uv run dlthub local run jaffle_transformations.py
```

`dlthub local profile use prod` pins `prod` as the active local profile. Subsequent `dlthub local …` commands read from and write to the production credentials and dataset.

### Deploying transformations to dltHub

You can deploy and orchestrate transformations on dltHub just like any other pipeline:

```sh
uv run dlthub run jaffle_transformations.py
```

This uploads the transformation script, runs it on managed infrastructure, and streams logs back to your terminal. You can also schedule this job (declare a `trigger=` on the decorator and run `dlthub deploy`) and monitor it via the dltHub UI.

### Incremental transformations on a schedule

When a transformation runs on a dltHub Platform cron schedule, let the schedule own the cursor window. Set `allow_external_schedulers=True` on a `dlt.sources.incremental` argument and the cursor takes its `[start, end)` bounds from the scheduled interval. Re-running the same window produces the same output, so retries and missed-run backfills are idempotent. See [Incremental transformations](../features/transformations/index.md#incremental-transformations) for the full model and examples.

## Next steps

You've completed the introductory tutorial for the managed dltHub Platform: you've learned how to deploy pipelines, run interactive notebooks, and add transformations.

As next steps, we recommend:

1. Take one of your existing dlt pipelines and schedule it on the managed platform.
2. Add [data checks](../features/quality/data-quality.md) to your pipelines to monitor data quality and catch issues early.

This gives you a trusted, managed environment for both ingestion and analytics, built on dlt and powered by dltHub.
