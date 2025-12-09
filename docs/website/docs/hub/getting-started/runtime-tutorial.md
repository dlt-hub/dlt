---
title: Deploy trusted dlt pipelines and dashboards
description: Tutorial walking through deployment on dltHub Runtime
keywords: [deployment, runtime, dashboard, dlt pipeline]
---

With the dltHub you can not only build data ingestion pipelines and dashboards, but also **run and manage them on a fully managed dltHub Runtime**.
See the [Runtime overview](../runtime/overview.md) for more details. You get:

- The flexibility and developer experience of **dlt**
- The simplicity and reliability of **managed infrastructure**

## What you will learn

In this tutorial you will:

- Deploy a dlt pipeline on the **dltHub managed Runtime**
- Deploy an **always-fresh dashboard** on the dltHub managed Runtime
- Add **Python transformations** to your ELT jobs

## Prerequisites

- Python 3.10+
- A [MotherDuck](https://motherduck.com) account (for the starter pack example)
- [uv](https://docs.astral.sh/uv/) package manager (recommended for dependency management)

## Quickstart

To make things easier, we provide a starter repository with a preconfigured dltHub project. It contains a working source, pipeline, transformations, and a small dashboard so you can focus on learning the Runtime rather than setting everything up from scratch.

This starter pack includes:

1. A **dlt pipeline** that loads data from the **jaffle shop API** into a local **DuckDB** destination.
2. A **remote destination** configured as **MotherDuck**. You can swap it for any other cloud destination you prefer (for example
   [BigQuery](../../dlt-ecosystem/destinations/bigquery.md),
   [Snowflake](../../dlt-ecosystem/destinations/snowflake.md),
   [AWS S3](../../dlt-ecosystem/destinations/filesystem.md), …).
3. A simple **Marimo dashboard** that you can use to explore and analyze the data.
4. A set of **custom transformations** that are executed after the raw data is loaded.

We’ll walk through cloning the repo, installing dependencies, connecting to Runtime, and then deploying both pipelines and dashboards.

### 1. Clone the starter pack

```sh
git clone https://github.com/dlt-hub/runtime-starter-pack.git
cd runtime-starter-pack
```

### 2. Install dependencies and activate the environment

The starter pack comes with a `pyproject.toml` that defines all required dependencies:

```toml
[project]
name = "runtime-starter-pack"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = [
    "dlt[motherduck,workspace,hub]==1.20.0a0",
    "marimo>=0.18.2",
    "numpy>=2.3.5",
]
```

Install everything with uv:

```sh
uv sync
```

Activate the environment:

```sh
source .venv/bin/activate
```

### 3. Configure your credentials

If you are running this tutorial as part of the early access program, you need to add your Runtime invite code to `.dlt/secrets.toml`:

```toml
[runtime]
invite_code="xxx-yyy"
```

Next, configure your destination credentials. The starter pack uses MotherDuck as the destination, but you can switch to any other destination you prefer.
Details on configuring credentials for Runtime are available [here](../runtime/overview.md#credentials-and-configs).
Make sure your destination credentials are valid before running pipelines remotely.

### 4. Log in to dltHub Runtime

Authenticate your local workspace with the managed Runtime:

```sh
uv run dlt runtime login
```

This will:

1. Open a browser window.
2. Use GitHub OAuth for authentication.
3. Link your local workspace to your dltHub Runtime account.

Currently, GitHub-based authentication is the only supported method. Additional authentication options will be added later.

### 5. Run your first pipeline on Runtime

Now let’s deploy and run a pipeline remotely:

```sh
uv run dlt runtime launch fruitshop_pipeline.py
```

This single command:

1. Uploads your code and configuration to Runtime.
2. Creates and starts a batch job.
3. Streams logs and status, so you can follow the run from your terminal. To run it in deatached mode, use `uv run dlt runtime launch fruitshop_pipeline.py -d`

dltHub supports two types of jobs:
* batch job, which are Python scripts, which are supposed to be run once or scheduled
* interactive job, which basically serves the interactive notebook

### 6. Open an interactive notebookk

```sh
uv run dlt runtime serve fruitshop_notebook.py
```

This command:

1. Uploads your code and configuration.
2. Starts an interactive notebook session using the access profile.
3. Opens the notebook in your browser.

:::note
Interactive notebooks use the `access` profile with read-only credentials, so they are safe for data exploration and dashboarding without the risk of accidental writes.
:::

Interactive jobs are the building block for serving notebooks, dashboards , streamlit or similar apps (in the future).
At the moment, only Marimo is supported. You can share links to these interactive jobs with your colleagues for collaborative exploration.

### 7. Schedule a pipeline

To run a pipeline on a schedule, use:

```sh
uv run dlt runtime schedule fruitshop_pipeline.py "*/10 * * * *"
```

This example schedules the pipeline to run every 10 minutes. Use [crontab.guru](https://crontab.guru) to build and test your cron expressions.

To cancel an existing schedule:

```sh
uv run dlt runtime schedule fruitshop_pipeline.py cancel
```

## Review and manage jobs in the UI

The command line is great for development, but the dltHub web UI gives you a bird’s-eye view of everything running on Runtime.
Visit [dlthub.app](https://dlthub.app) to access the dashboard. You will find:

1. A list of existing jobs.
2. An overview of scheduled runs.
3. Visibility into interactive sessions.
4. Management actions and workspace settings

Visit [dlthub.app](https://dlthub.app) to access the web dashboard. The dashboard provides overview of your existing jobs, scheduled and interactive runs and some management and settings.

### Pipelines and data access in the Dashboard

The dltHub Dashboard lets you see all your pipelines and job runs, inspect job metadata (status, start time, duration, logs, etc.), and access the data in your destination via a SQL interface.
This makes it easy to debug issues, check the health of your pipelines, and quickly validate the data that has been loaded.

### Public links for interactive jobs

Interactive jobs such as notebooks and dashboards can be shared via public links. To manage public links:

1. Open the context menu of a job in the job list or navigate to the job detail page.
2. Click "Manage Public Link".
3. Enable the link to generate a shareable URL, or disable it to revoke access.

Anyone with an active public link can view the running notebook or dashboard, even if they don’t have direct Runtime access. This is ideal for sharing dashboards with stakeholders, business users, or other teams.

## Add transformations

Raw ingested data is rarely enough. Transformations let you reshape, enrich, and prepare data for analytics and downstream tools. Transformations are useful when you want to
aggregate raw data into reporting tables, join multiple tables into enriched datasets, create dimensional models for analytics, and apply business logic to normalize or clean data.

dltHub Transformations let you build new tables or entire datasets from data that has already been ingested using dlt.

Key characteristics:

1. Defined in Python functions decorated with `@dlt.hub.transformation`.
2. Can use Python (via Ibis) or pure SQL
3. Operate on the destination dataset (`dlt.Dataset`)
4. Executed on the destination compute or locally via DuckDB

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

```bash
uv run python jaffle_transformations.py
```

### Running with the production profile

To run the same transformations against your production destination:

```bash
uv run dlt profile prod pin
uv run python jaffle_transformations.py
```

* `dlt profile prod pin` sets prod as the active profile.
* The script will now read from and write to the production dataset and credentials.

### Deploying transformations to Runtime

You can deploy and orchestrate transformations on dltHub Runtime just like any other pipeline:

```bash
uv run dlt runtime launch jaffle_transformations.py
```

This uploads the transformation script, runs it on managed infrastructure, and streams logs back to your terminal. You can also schedule this job and monitor it via the dltHub UI.

## Next steps

You’ve completed the introductory tutorial for dltHub Runtime: you’ve learned how to deploy pipelines, run interactive notebooks, and add transformations.

As next steps, we recommend:

1. Take one of your existing dlt pipelines and schedule it on the managed Runtime.
2. Explore our [MCP](../features/mcp-server.md) integration for connecting Runtime to tools and agents.
3. Add  [data checks](../features/quality/data-quality.md) to your pipelines to monitor data quality and catch issues early.

This gives you a trusted, managed environment for both ingestion and analytics, built on dlt and powered by dltHub Runtime.
