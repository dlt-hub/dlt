---
title: Deploy trusted dlt pipelines and dashboards
description: Tutorial walking through deployment on dltHub Runtime
keywords: [deployment, runtime, dashboard, dlt pipeline]
---

With the dltHub product users are able to not only create data ingestion pipelines and dashboards, but also deploy them on dltHub managed runtime.
With dltHub you get the flexibility of the dlt together with simplicity of  the managed infrastructure.

## What you will learn

* Deploying your dlt pipelines on the dltHub managed Runtime.
* Deploying always fresh dashboards on the dltHub managed Runtime.
* Adding Python transformations to your ELT jobs.
* Adding Data Checks to your pipelines.

## Prerequisites

- Python 3.10+
- A [MotherDuck](https://motherduck.com) account (for the starter pack example)
- [uv](https://docs.astral.sh/uv/) package manager (recommended)

## Quickstart

### Clone the starter pack

```sh
git clone https://github.com/dlt-hub/runtime-starter-pack.git
cd runtime-starter-pack
```

### Install dependencies and activate the environment

The starter pack includes all required dependencies in `pyproject.toml`:

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

Install with uv:

```sh
uv sync
```

Activate the environment:

```sh
source .venv/bin/activate
```

### Configure your credentials

If you're running this tutorial as a part of the early access, make sure your put the necessary credentials to your `.dlt/secrets.toml` file:

```toml
[runtime]
invite_code="xxx-yyy"
```

Before running pipelines remotely, you need to set up your destination credentials. The starter pack uses MotherDuck as the destination. Read how to set the correct credentials
[here](../production/overview.md#credentials-and-configs)


### Log In to Runtime

```sh
uv run dlt runtime login
```

This opens a browser for GitHub OAuth authentication and connects your local workspace to the remote Runtime.

### Run Your First Pipeline

```sh
uv run dlt runtime launch fruitshop_pipeline.py
```

This command:
1. Uploads your code and configuration to Runtime
2. Creates and starts a batch job
3. Follows the run status and streams logs

### Open an Interactive Notebook

```sh
uv run dlt runtime serve fruitshop_notebook.py
```

This command:
1. Uploads your code and configuration
2. Starts an interactive notebook session with the `access` profile
3. Opens the notebook in your browser

:::note
Interactive notebooks use the **access** profile with read-only credentials, making them safe for data exploration without risk of accidental writes.
:::

### Schedule a Pipeline

```sh
uv run dlt runtime schedule fruitshop_pipeline.py "*/10 * * * *"
```

This schedules the pipeline to run every 10 minutes. Use [crontab.guru](https://crontab.guru) to build cron expressions.

To cancel a schedule:

```sh
uv run dlt runtime schedule fruitshop_pipeline.py cancel
```

## Review existing jobs in UI

Visit [dlthub.app](https://dlthub.app) to access the web dashboard. The dashboard provides:

### Overview

The workspace overview shows all your jobs and recent runs at a glance. Lists auto-refresh every 10 seconds.

### Jobs

View and manage all jobs in your workspace. A **job** represents a script that can be run on demand or on a schedule.

From the Jobs page you can:
- View job details and run history
- Change or cancel schedules for batch jobs
- Create and manage **public links** for interactive jobs (notebooks/dashboards)

#### Public Links for Interactive Jobs

Interactive jobs like notebooks and dashboards can be shared via public links. To manage public links:
1. Open the context menu on a job in the job list, or go to the job detail page
2. Click "Manage Public Link"
3. Enable the link to generate a shareable URL, or disable it to revoke access

Anyone with an active public link can view the running notebook or dashboard. This is useful for sharing dashboards with stakeholders who don't have Runtime access.

### Runs

Monitor all job runs with:
- Run status (pending, running, completed, failed, cancelled)
- Start time and duration
- Trigger type (manual, scheduled, API)

### Run Details

Click on any run to see:
- Full execution logs
- Run metadata
- Pipeline information

### Deployment & Config

View the files deployed to Runtime:
- Current deployment version
- Configuration profiles
- File listing

### Dashboard

Access the dlt pipeline dashboard to visualize:
- Pipeline schemas
- Load information
- Data lineage

### Settings

Manage workspace settings and view workspace metadata.

## Add transformations

Transformations are useful when you need to:

- Aggregate raw data into reporting tables
- Join multiple tables into enriched datasets
- Create dimensional models for analytics
- Apply business logic to normalize or clean data

### Basic Example with Ibis

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

### Joining Multiple Tables

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

### Using Pure SQL

You can also use raw SQL queries:

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
### Running Locally

Run transformations locally with DuckDB (default `dev` profile):

```bash
uv run python jaffle_transformations.py
```

### Running with Production Profile

Pin to the production profile and run:

```bash
uv run dlt profile prod pin
uv run python jaffle_transformations.py
```

### Deploying to Runtime

Deploy and run transformations on the dltHub runtime:

```bash
uv run dlt workspace deploy jaffle_transformations.py -p prod
```

### Schema Evolution

When executing transformations, `dlt` computes the resulting schema before the transformation runs. This allows `dlt` to:

- Automatically migrate the destination schema, creating new columns or tables as needed
- Fail early if there are schema mismatches that cannot be resolved
- Preserve column-level hints from source to destination

### Column Hint Forwarding

Custom column hints (like PII annotations) are automatically forwarded from source tables to transformation results:

```py
# If your source has a column marked with x-annotation-pii
@dlt.resource(columns={"name": {"x-annotation-pii": True}})
def customers():
    yield [{"id": 1, "name": "alice"}]

# The hint is preserved in transformation output
@dlt.hub.transformation
def enriched_purchases(dataset: dlt.Dataset) -> typing.Any:
    yield dataset(
        """
        SELECT customers.name, purchases.quantity
        FROM purchases
        JOIN customers ON purchases.customer_id = customers.id
        """
    )
```
