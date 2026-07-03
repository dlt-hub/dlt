---
title: Build and Deploy Streamlit App
description: Build, serve, and deploy a Streamlit app on dltHub.
keywords: [streamlit, dashboard, hub, app, deploy, dltHub]
---

# Build and Deploy Streamlit App

[Streamlit](https://docs.streamlit.io/) is a Python framework for turning a script into an interactive web app. On dltHub, a Streamlit app is a plain `.py` file that imports `streamlit`, and the runtime serves it as an interactive dashboard.

This page walks through building a small dashboard against a loaded dlt dataset and deploying it to [app.dlthub.com](https://app.dlthub.com).

## Prerequisites

Add Streamlit to your workspace dependencies:

```sh
uv add streamlit
```

The example below reads from the `sample_shop_pipeline` that `uvx dlthub-start@latest` scaffolds. The dashboard needs that data already loaded against the same destination it'll read from:

```sh
# Load locally (dev profile, DuckDB) so the dashboard works under `dlthub local serve`
uv run dlthub local run load_sample_shop

# OR, before deploying remotely, load against the prod destination on dltHub
uv run dlthub run load_sample_shop
```

A dashboard can only display data that's already been loaded. If you skip this step the deployed app boots but every read returns "table not found".

## Write the dashboard

Create `sample_shop_dashboard.py` in the workspace root:

```py
"""Sample shop dashboard."""

import dlt
import streamlit as st


st.set_page_config(page_title="Sample shop", layout="wide")
st.title("Sample shop orders")


@st.cache_data
def load_data():
    dataset = dlt.dataset(destination="warehouse", dataset_name="sample_shop")
    return dataset["orders"].df(), dataset["customers"].df()


orders, customers = load_data()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Orders", f"{len(orders):,}")
col2.metric("Customers", f"{len(customers):,}")
col3.metric("Total revenue", f"${orders['order_total'].sum():,.2f}")
col4.metric("Avg order value", f"${orders['order_total'].mean():.2f}")

st.subheader("Top customers by spend")
by_customer = (
    orders.merge(customers, left_on="customer_id", right_on="id", suffixes=("_o", "_c"))
    .groupby("name", as_index=False)["order_total"].sum()
    .sort_values("order_total", ascending=False)
    .head(10)
)
st.dataframe(
    by_customer.rename(columns={"name": "Customer", "order_total": "Total spend ($)"}),
    width="stretch", hide_index=True,
)

st.subheader("Orders by store")
by_store = orders.groupby("store_id").size().rename("orders").sort_values(ascending=False)
st.bar_chart(by_store)
```

[`dlt.dataset(destination, dataset_name)`](../../general-usage/dataset-access/dataset.md) connects directly to a loaded dataset. Wrap the call in `@st.cache_data` so each widget interaction doesn't requery the destination.

## Run it locally

```sh
uv run dlthub local serve sample_shop_dashboard.py
```

This boots the dashboard under the workspace's active local profile (default `dev`, which reads from `.dlt/config.toml`) and opens it in your browser.

## Configure the `access` profile

`dlthub serve` runs interactive jobs under the `access` profile by default. The minimal scaffold doesn't ship an `access` profile, so create one before deploying. Add the destination type in `.dlt/access.config.toml`:

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

See the [Profiles in dltHub](../pipeline-operations/profiles.md) page for the full profile model.

## Deploy to dltHub

Add the dashboard to your `__deployment__.py` manifest so the workspace knows about it:

```py
"""Minimal dltHub workspace."""

from pipeline import load_sample_shop
import sample_shop_dashboard            # module-import → one job

__all__ = ["load_sample_shop", "sample_shop_dashboard"]
```

Then deploy and serve:

```sh
uv run dlthub deploy                                       # publishes manifest + uploads code
uv run dlthub serve sample_shop_dashboard.py               # boots the app remotely, opens URL
```

`dlthub serve` runs the app behind the workspace's auth — only your account can open the link. To create a publicly shareable URL:

```sh
uv run dlthub job publish sample_shop_dashboard.py         # public URL
uv run dlthub job unpublish sample_shop_dashboard.py       # revoke
```
