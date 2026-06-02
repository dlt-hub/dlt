---
title: Datasets
description: Datasets in dltHub — the serving layer for loaded data.
keywords: [dataset, datasets, hub, governance, profile, catalog, dlthub]
---

# Datasets

A **dataset** is a physical collection of data and dlt metadata at a destination — including the schema, load history, traces, and quality results. In dltHub, datasets are the **serving layer**: schemas, annotations, and run metadata propagate from sources through transformations and end up inside the dataset, so notebooks, dashboards, and downstream agents read from a single source of truth.

For the Python access surface — `pipeline.dataset()`, `ReadableRelation`, `.df()`/`.arrow()`, ibis, SQL — see the [OSS Dataset reference](../../general-usage/dataset-access/dataset.md). This page focuses on what's specific to running datasets on dltHub.

## One destination, many datasets

A destination is a physical system (DuckDB, MotherDuck, Snowflake, BigQuery, etc.). One destination can host any number of datasets, addressed by their `dataset_name`. Two pipelines writing into the same destination but different datasets stay isolated:

```py
import dlt

# `warehouse` resolves via .dlt/<profile>.config.toml + .dlt/<profile>.secrets.toml

orders_pipeline = dlt.pipeline(
    pipeline_name="orders_pipeline",
    destination="warehouse",
    dataset_name="orders",
)
customers_pipeline = dlt.pipeline(
    pipeline_name="customers_pipeline",
    destination="warehouse",
    dataset_name="customers",
)
```

Both datasets live side-by-side in the same MotherDuck database as separate schemas.

## Profile-aware materialization

The same logical destination (and therefore the same dataset) resolves to **different physical systems per profile**. You write code once; switching profiles redirects materialization.

In a workspace, the binding lives in `.dlt/<profile>.config.toml`:

```toml
# .dlt/dev.config.toml
[destination.warehouse]
destination_type = "duckdb"
```

```toml
# .dlt/prod.config.toml
[destination.warehouse]
destination_type = "motherduck"
```

```py
# pipeline.py — unchanged across profiles
import dlt

pipeline = dlt.pipeline(
    pipeline_name="orders_pipeline",
    destination="warehouse",          # resolves via the active profile
    dataset_name="orders",
)
```

| Profile | `destination_type` | Where data lives |
|---|---|---|
| `dev` | `duckdb` | `.dlt/data/dev/warehouse.duckdb` (local file) |
| `prod` | `motherduck` | MotherDuck (cloud) |
| `access` | `motherduck`, read-only credentials | MotherDuck (cloud), read-only |

See [Profiles in dltHub](../pipeline-operations/profiles.md) for the full profile model and [Workspace setup](../pipeline-operations/workspace-setup.md) for the configuration file layout.

## Catalog and discovery in the dashboard

Every load writes schema, traces, and (if enabled) data-quality results into the dataset itself. The dltHub dashboard at [app.dlthub.com](https://app.dlthub.com) reads those tables and surfaces them across two main views:

- **Datasets** — every dataset in your workspace listed with its destination, owning pipeline, runs, success rate, rows and bytes loaded, schema migrations, average run time, and last-run status.
- **Notebooks** — the `dashboard (workspace)` notebook renders per-pipeline panels (schema inspection, data browsing, data-quality results, pipeline state, run traces, and load history). You can also add your own marimo notebooks.

No additional configuration is needed; landing data into a workspace-configured destination is what populates these views.

## What lives inside a dataset

Alongside your tables, dlt writes a small set of system tables. They're present in every dataset and are the substrate for the dashboard, data quality, and downstream tooling.

| Table | Source | Purpose |
|---|---|---|
| `_dlt_loads` | OSS | One row per load package — load_id, schema name, status, timestamp. |
| `_dlt_pipeline_state` | OSS | Pipeline state across runs (incremental cursors, source state). |
| `_dlt_version` | OSS | Schema versions over time. |
| `_dlt_dq_metrics` | Hub | Per-call metric snapshots written by `dq.run_metrics()`. |
| `_dlt_checks` | Hub | Per-call check pass/fail summaries written by `dq.run_checks()`. |

The first three are documented in the [OSS internal tables](../../general-usage/dataset-access/dataset.md#internal-dlt-tables) reference; the last two are written by the [Data Quality](../data-quality/index.md) runners.

## Reading a dataset from another job or notebook

A transformation, downstream job, or marimo notebook can read a dataset another pipeline produced — without re-extracting from source. In a workspace where the destination is configured in `.dlt/<profile>.config.toml`, this is enough:

```py
import dlt

pipeline = dlt.attach("orders_pipeline")
orders = pipeline.dataset()["orders"].df()
```

`dlt.attach` reconstructs the pipeline from its persisted state, picking up the destination from the active profile.

:::note Platform deploys
When deploying a notebook to the Platform, pass `destination` and `dataset_name` explicitly:

```py
pipeline = dlt.attach(
    pipeline_name="orders_pipeline",
    destination="warehouse",
    dataset_name="orders",
)
```

Notebooks running on the Platform require these arguments explicitly; local scripts can rely on the workspace configuration alone.
:::

A quick sanity check that returns a frame of `(table_name, row_count)` across the whole dataset:

```py
pipeline.dataset().row_counts().df()
```

## Serving datasets with marimo

Marimo notebooks deployed against the `access` profile turn a dataset into a shareable read-only app: schema-aware widgets, SQL/Python access, and a URL that stakeholders open without provisioning credentials of their own. See [Marimo notebooks](../../general-usage/dataset-access/marimo.md) for the full integration.
