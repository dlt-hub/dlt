---
title: Build and deploy a marimo notebook
description: Build, serve, and deploy a marimo notebook on dltHub.
keywords: [marimo, notebook, hub, app, deploy, dltHub]
---

# Build and deploy a marimo notebook

[marimo](https://docs.marimo.io/) is a reactive Python notebook that lives in a plain `.py` file. On dltHub, a notebook is a module that holds a `marimo.App` instance. The runtime detects the framework from the module and serves it as an interactive notebook job, so a notebook needs **no job decorator**. Importing the module into `__deployment__.py` is the whole registration. marimo is also what the [workspace dashboard](../ingestion/dashboard.md) is built on.

This page walks through building a notebook against a loaded dlt dataset and deploying it to [app.dlthub.com](https://app.dlthub.com). To explore data in marimo on your own machine, see [Explore data with marimo](../../general-usage/dataset-access/marimo.md).

## Prerequisites

A workspace created with `dlthub init` already declares `marimo` in its dependencies, so `uv sync` is enough. Add it if your project doesn't have it:

```sh
uv add marimo
```

The example below reads from the `sample_shop_pipeline` that `uvx dlthub-start@latest` scaffolds (see [Deploy your first pipeline](../getting-started/onboarding.md)). The notebook needs that data already loaded against the same destination it'll read from:

```sh
# Load locally (dev profile, DuckDB) so the notebook works under `dlthub local serve`
uv run dlthub local run load_sample_shop

# OR, before deploying remotely, load against the prod destination on dltHub
uv run dlthub run load_sample_shop
```

A notebook can only display data that's already been loaded. If you skip this step the deployed notebook boots but every read returns "table not found".

## Write the notebook

Create the notebook with marimo's editor, which writes the cells into the file as you add them:

```sh
uv run marimo edit sample_shop_notebook.py
```

The finished `sample_shop_notebook.py` in the workspace root looks like this:

```py
"""Sample shop notebook."""

import marimo

app = marimo.App(app_title="Sample shop")


@app.cell
def _():
    import dlt
    import marimo as mo

    return dlt, mo


@app.cell
def _(dlt):
    dataset = dlt.dataset(destination="warehouse", dataset_name="sample_shop")
    return (dataset,)


@app.cell
def _(dataset):
    orders = dataset["orders"].df()
    customers = dataset["customers"].df()
    return customers, orders


@app.cell
def _(customers, mo, orders):
    mo.hstack(
        [
            mo.stat(len(orders), label="Orders"),
            mo.stat(len(customers), label="Customers"),
            mo.stat(round(orders["order_total"].sum(), 2), label="Revenue"),
        ]
    )
    return


@app.cell
def _(mo, orders):
    by_store = (
        orders.groupby("store_id", as_index=False)["order_total"]
        .sum()
        .sort_values("order_total", ascending=False)
    )
    mo.ui.table(by_store, selection=None)
    return


if __name__ == "__main__":
    app.run()
```

Three things in that file matter for the deployment:

- The module-level `marimo.App` instance is what makes the module a notebook job. Any variable name works. The editor writes `app`.
- `app_title` and the module docstring become the job description in dltHub. Here that description is `Sample shop: Sample shop notebook.`
- [`dlt.dataset(destination, dataset_name)`](../../general-usage/dataset-access/dataset.md) reads a loaded dataset without going through a pipeline. Pass a [named destination](../../general-usage/destination.md#use-named-destinations) (`warehouse` here) rather than a destination type, so the same notebook reads DuckDB under the `dev` profile and your warehouse on the platform.

Charting libraries such as Altair or Plotly are not part of `dlt[hub]`. Add the ones your cells import with `uv add`, and check the file before you serve it:

```sh
uvx marimo check sample_shop_notebook.py
```

## Run it locally

```sh
uv run dlthub local serve sample_shop_notebook.py
```

This boots the notebook under the workspace's active local profile (default `dev`, which reads from `.dlt/config.toml`) and opens it in your browser. Pass `--profile <name>` to serve it under a different profile.

## Configure the `access` profile

`dlthub serve` runs interactive jobs under the `access` profile, the read-only production profile. The minimal scaffold doesn't ship an `access` profile, so create one before deploying. Add the destination type in `.dlt/access.config.toml`:

```toml
[destination.warehouse]
destination_type = "motherduck"
```

And the credentials in `.dlt/access.secrets.toml`. **Both `database` and `password` need to be in the secrets file** for MotherDuck:

```toml
[destination.warehouse.credentials]
database = "dlt_test"
password = "<read-only motherduck JWT>"
```

If the deployed workspace configuration has no `access` profile, the notebook falls back to `prod` and runs with production write credentials. A notebook does not need write access, and a workspace [viewer](../platform-capabilities/users-and-roles.md) cannot open a job that runs on `prod`. This is the step people trip on: the notebook deploys and serves, then reads with the wrong credentials.

See [Profiles in dltHub](../pipeline-operations/profiles.md) for the full profile model and [Workspace setup](../pipeline-operations/workspace-setup.md#understanding-workspace-profiles) for the file layout.

## Deploy to dltHub

Add the notebook to your `__deployment__.py` manifest so the workspace knows about it:

```py
"""Sample shop workspace."""

from pipeline import load_sample_shop
import sample_shop_notebook            # module import -> one notebook job

__all__ = ["load_sample_shop", "sample_shop_notebook"]
```

The plain module import is the registration. Framework detection gives the job the ref `jobs.sample_shop_notebook`, an `http:` trigger, and a concurrency of 1. Nothing on the notebook itself declares any of that. See [The deployment module](../pipeline-operations/deployments.md#the-deployment-module) for the rest of the manifest rules.

If the notebook needs dependencies that the other jobs don't, declare them at module level with `__require__`, which takes the same keys as the decorators' `require=` argument:

```py
__require__ = {"dependency_groups": ["charts"]}
```

Then deploy and serve:

```sh
uv run dlthub deploy                                       # publishes manifest + uploads code
uv run dlthub serve sample_shop_notebook.py                # boots the notebook remotely, opens URL
```

`dlthub serve` runs the notebook behind the workspace's auth, so only your account can open the link. To create a publicly shareable URL:

```sh
uv run dlthub job publish sample_shop_notebook.py          # public URL
uv run dlthub job unpublish sample_shop_notebook.py        # revoke
```

## Tune how the notebook is served

The runtime serves notebooks with `marimo run`, so readers get the app without the source cells. Override the launcher settings per notebook under `[jobs.<module_name>.marimo]` in the profile's config file:

```toml
[jobs.sample_shop_notebook.marimo]
include_code = true
session_ttl = 600
```

| Option | Default | What it does |
|---|---|---|
| `command` | `run` | marimo subcommand: `run` serves a read-only app, `edit` serves the editor |
| `include_code` | `false` | Show the notebook source in the served app |
| `session_ttl` | `120` | Seconds before an idle session is closed |
| `token` | unset | Password for the marimo session. Unset leaves marimo's own token auth off. The job is still served behind the workspace auth until you publish a public link |

These resolve like any other dlt configuration, so the environment variable form works too: `JOBS__SAMPLE_SHOP_NOTEBOOK__MARIMO__INCLUDE_CODE=true`.

## When you need `@run.interactive`

marimo notebooks, Streamlit apps, and FastMCP modules are all recognized from the module, so none of them takes a decorator. Reach for `@run.interactive` when you serve the HTTP endpoint yourself, for example a REST API or anything the detectors don't recognize:

```py
"""Orders REST API."""

from dlt.hub import run
from dlt.hub.run import TJobRunContext


@run.interactive(interface="rest_api", idle_timeout="1h")
def orders_api(run_context: TJobRunContext):
    """Serve the orders dataset over HTTP."""
    # the runtime assigns the port and proxies traffic to it
    port = run_context["run_args"]["port"]
    serve_orders(host="0.0.0.0", port=port)  # your own blocking server
```

- `interface` is `"gui"`, `"rest_api"`, or `"mcp"`, and decides how dltHub presents the job.
- The function must block for as long as the job serves traffic. Declare a `run_context` parameter to receive the assigned port in `run_args["port"]`. A function that returns a `FastMCP` instance is the exception: the MCP launcher takes it and runs it for you.
- `idle_timeout` takes seconds or a string such as `"24h"`.

Register it in `__deployment__.py` with a function import: `from orders_api import orders_api`. For every argument the decorator accepts, see [Job configuration](../pipeline-operations/job-configuration.md).
