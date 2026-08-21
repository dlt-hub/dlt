---
title: Build and deploy a marimo notebook
description: Build a marimo notebook on a loaded dlt dataset, serve it locally, and deploy it to dltHub as an interactive job.
keywords: [marimo, notebook, dashboard, hub, data app, deploy, dltHub]
---

# Build and deploy a marimo notebook

[marimo](https://docs.marimo.io/) is a reactive Python notebook: when you change a cell or interact with a UI element, every dependent cell re-runs. On dltHub, a marimo notebook is a plain `.py` file that creates a `marimo.App`, and the runtime serves it as an interactive job. The built-in [workspace dashboard](../ingestion/dashboard.md) is itself a marimo notebook served that way.

This page walks through building a notebook against a loaded dlt dataset and deploying it to [app.dlthub.com](https://app.dlthub.com). To explore data in marimo on your own machine, without a workspace, see [Explore data with marimo](../../general-usage/dataset-access/marimo.md). For the Streamlit version of this walkthrough, see [Build and deploy a Streamlit app](../cookbook/build-streamlit-dashboard.md).

## Prerequisites

Add marimo to your workspace dependencies:

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

A notebook can only display data that's already been loaded. If you skip this step the deployed notebook boots but every read fails with "table not found".

## Write the notebook

Create `sample_shop_notebook.py` in the workspace root:

```py
"""Sample shop notebook: orders and customers loaded by load_sample_shop."""

import marimo

app = marimo.App(width="medium", app_title="Sample shop")


@app.cell
def imports():
    import dlt
    import marimo as mo

    return dlt, mo


@app.cell
def read_dataset(dlt):
    dataset = dlt.dataset(destination="warehouse", dataset_name="sample_shop")
    orders = dataset["orders"].df()
    customers = dataset["customers"].df()
    return orders, customers


@app.cell
def headline(mo, orders, customers):
    mo.hstack(
        [
            mo.stat(len(orders), label="Orders"),
            mo.stat(len(customers), label="Customers"),
            mo.stat(f"${orders['order_total'].sum():,.2f}", label="Revenue"),
        ]
    )
    return


@app.cell
def store_filter(mo, orders):
    store = mo.ui.dropdown(options=sorted(orders["store_id"].unique()), label="Store")
    store
    return (store,)


@app.cell
def orders_table(mo, orders, store):
    selected = orders if store.value is None else orders[orders["store_id"] == store.value]
    mo.ui.table(selected, selection=None)
    return


if __name__ == "__main__":
    app.run()
```

Three things make this file a deployable notebook rather than an ordinary script:

- **A module-level `marimo.App`.** dlt looks for one in the module, preferring a variable called `app` and otherwise taking the first module-level `marimo.App` it finds. Without it, the file is treated as a batch script.
- **`app_title` and the module docstring.** Both are picked up and combined into the job description shown in the dltHub UI.
- **Cells that declare their inputs.** Each cell takes the variables it reads as arguments and returns the ones later cells need. marimo derives the execution order from that, and re-runs only the cells affected by a change, so the dataset read does not need a cache decorator around it.

[`dlt.dataset(destination, dataset_name)`](../../general-usage/dataset-access/dataset.md) connects directly to a loaded dataset. `warehouse` is a [named destination](../../general-usage/destination.md#use-named-destinations), so the same notebook reads from DuckDB locally and from your production destination once deployed.

## Run it locally

Edit the notebook in marimo's own editor while you build it:

```sh
uv run marimo edit sample_shop_notebook.py
```

Then run it the way the platform will, as a workspace job:

```sh
uv run dlthub local serve sample_shop_notebook.py
```

This resolves the job from the deployment manifest exactly as the runtime does, boots it under the workspace's active local profile (default `dev`), and prints `Listening on http://localhost:5000`.

The two commands do different things: `marimo edit` opens the editor, while serving the job runs `marimo run`, the read-only app with the source hidden. That second form is what your viewers get.

## Configure the `access` profile

Interactive jobs run under the `access` profile on dltHub: a production profile meant for notebooks, usually holding read-only credentials. The minimal scaffold doesn't ship one, and without it interactive jobs fall back to the `prod` profile, which the CLI warns about before it launches.

Add the destination type in `.dlt/access.config.toml`:

```toml
[destination.warehouse]
destination_type = "motherduck"
```

And the credentials in `.dlt/access.secrets.toml`:

```toml
[destination.warehouse.credentials]
database = "dlt_test"
password = "<read-only motherduck JWT>"
```

See [Profiles in dltHub](../pipeline-operations/profiles.md) for the full profile model.

## Deploy to dltHub

Add the notebook to your `__deployment__.py` manifest so the workspace knows about it:

```py
"""Minimal dltHub workspace."""

from pipeline import load_sample_shop
import sample_shop_notebook            # module import, one job

__all__ = ["load_sample_shop", "sample_shop_notebook"]
```

Importing the module, rather than a decorated function, is what turns it into a single job. dlt finds the `marimo.App` and registers an interactive notebook job with an HTTP trigger and a concurrency of one. See [the deployment module](../pipeline-operations/deployments.md#the-deployment-module) for the other forms.

Then deploy and serve:

```sh
uv run dlthub deploy                                    # publishes manifest + uploads code
uv run dlthub serve sample_shop_notebook.py             # boots the notebook remotely, opens URL
```

`dlthub serve` runs the notebook behind the workspace's auth, so only members of your workspace can open the link, including [viewers](../platform-capabilities/users-and-roles.md), who may launch interactive jobs. To create a publicly shareable URL:

```sh
uv run dlthub job publish sample_shop_notebook          # public URL
uv run dlthub job unpublish sample_shop_notebook        # revoke
```

:::note
`job publish` and `job unpublish` take the job name, not the file path. Passing `sample_shop_notebook.py` is read as a `section.name` job reference and fails to resolve.
:::
