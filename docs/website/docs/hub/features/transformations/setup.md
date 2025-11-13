---
title: Setup
description: Define and execute local transformations
---

dltHub provides a powerful mechanism for executing transformations on your data using a locally spun-up cache. It automatically creates and manages the cache before execution and cleans it up afterward.

A transformation consists of functions that modify data stored in a [cache](../../core-concepts/cache.md). These transformations can be implemented using:

* [dbt models](./dbt-transformations.md)

By combining a cache and transformations, you can efficiently process data loaded via dlt and move it to a new destination.

:::warning
Local transformations are currently limited to specific use cases and are only compatible with data stored in filesystem-based destinations:

* [Iceberg](../../ecosystem/iceberg.md)
* [Delta](../../ecosystem/delta.md)
* [Cloud storage and filesystem](../../../dlt-ecosystem/destinations/filesystem.md)

Make sure to specify a dataset located in a filesystem-based destination when [defining a cache](#defining-the-cache).
:::

To use this feature, follow these steps:

1. [Configure the `dlt.yml` file](#configure-dltyml-file): define a cache and specify transformations.
2. [Generate scaffolding](#generate-scaffolding): automatically create transformation templates.
3. [Modify transformations](#modify-transformations): update the generated Python functions or dbt models.
4. [Run transformations](#run-transformations): execute them on your data.

## Configure `dlt.yml` file

Before setting up the transformations in the `dlt.yml` file, you need to make sure you have defined the cache.

### Defining the cache

You can find detailed instructions on how to define a cache in the [cache core concept](../../core-concepts/cache.md#define-the-cache). Here's an example:

```yaml
caches:
  github_events_cache:
    inputs:
      - dataset: github_events_dataset
        tables:
          items: items
    outputs:
      - dataset: github_events_dataset
        tables:
          items: items
          items_aggregated: items_aggregated
```

:::warning
Please make sure that the input dataset for the cache is located in a filesystem-based destination ([Iceberg](../../ecosystem/iceberg.md), [Delta](../../ecosystem/delta.md), or [Cloud storage and filesystem](../../../dlt-ecosystem/destinations/filesystem.md)).
:::

### Defining transformations

Specify transformations in `dlt.yml` with the following parameters:

* unique identifier for the transformation.
* engine – choose between:
  * `arrow` for Python-based transformations
  * `dbt` for dbt-based transformations
* cache – the cache that the transformation will run on.

For example,

```yaml
transformations:
  github_events_transformations:
    engine: dbt
    cache: github_events_cache
```

## Generate scaffolding

To create transformation scaffolding based on your dlt pipeline:

1. Run the dlt pipeline at least once; this ensures dlt has the dataset schemas.
2. Execute the following CLI command:

```sh
dlt transformation <transformation-name> render-t-layer
```

This will generate transformation files inside the `./transformations` folder. Depending on the engine:

* For dbt transformations: dbt models ([learn more](./dbt-transformations.md))

Each generated transformation includes models for managing incremental loading states via `dlt_load_id`.

## Modify transformations

Now you can update the generated transformations and create new ones to reflect the desired behavior. We recommend keeping the incremental approach as in the generated models.

## Run transformations

dltHub offers comprehensive CLI support for executing transformations. You can find the full list of available commands in the command line interface.

To run the defined transformation, use the following command:

```sh
dlt transformation <transformation_name> run
```

This command populates the local cache, applies the defined transformations, and then flushes the transformed tables to the specified destination.

