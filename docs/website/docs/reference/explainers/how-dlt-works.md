---
title: How dlt works
description: How data load tool (dlt) works
keywords: [architecture, extract, normalize, load]
---

# How `dlt` works

In a nutshell, `dlt` automatically turns data from a number of available [sources](../../general-usage/source) (e.g., an API, a PostgreSQL database, or Python data structures) into a live dataset stored in a [destination](../../general-usage/destination) of your choice (e.g., Google BigQuery, a Deltalake on Azure, or by pushing the data back via reverse ETL). You can easily implement your own sources, as long as you yield data in a way that is compatible with `dlt`, such as JSON objects, Python lists and dictionaries, pandas dataframes, and arrow tables. `dlt` will be able to automatically compute the schema and move the data to your destination.

![architecture-diagram](/img/dlt-onepager.png)

## A concrete example

The main building block of `dlt` is the [pipeline](../../general-usage/glossary.md#pipeline), which orchestrates the loading of data from your source into your destination in three discrete steps when you call its `run` method. Consider this intentionally short example:

```py
import dlt

pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
pipeline.run([{"id": 1}, {"id": 2}, {"id": 3}], table_name="items")
```

This is what happens when the `run` method is executed:

1. [Extract](how-dlt-works.md#extract) - Fully extracts the data from your source to your hard drive. In the example above, an implicit source with one resource with 3 items is created and extracted.
2. [Normalize](how-dlt-works.md#normalize) - Inspects and normalizes your data and computes a schema compatible with your destination. For the example above, the normalizer will detect one column `id` of type `int` in one table named `items` and no further normalization is needed.
3. [Load](how-dlt-works#load) - Runs schema migrations if necessary on your destination and loads your data into the destination. For the example above, a new dataset on a local duckdb database is created that contains one table with one column `id` of type `int` and three rows of data.

## The three phases

### Extract

During the extract phase, `dlt` fully extracts the data from your [sources](../../general-usage/source) to your hard drive into a new [load package](../../general-usage/destination-tables#load-packages-and-load-ids), which will be assigned a unique ID and will contain your raw data as received from your sources. Additionally, you can [supply schema hints](../../general-usage/source#define-schema) to define the data types of some of the columns or add a primary key and unique indexes. You can also control this phase by [limiting](../../general-usage/resource#sample-from-large-data) the number of items extracted in one run, using [incremental cursor fields](../../general-usage/incremental-loading#incremental-loading-with-a-cursor-field), and by tuning the performance with [parallelization](../../reference/performance#extract). You can also apply filters and maps to [obfuscate](../../general-usage/customising-pipelines/pseudonymizing_columns) or [remove](../../general-usage/customising-pipelines/removing_columns) personal data, and you can use [transformers](../../examples/transformers) to create derivative data.

### Normalize

During the normalization phase, `dlt` inspects and normalizes your data and computes a [schema](../../general-usage/schema) corresponding to the input data. The schema will automatically evolve to accommodate any future source data changes, for example, new columns or tables. `dlt` will also unnest nested data structures into child tables and create variant columns if detected values do not match a schema computed during a previous run. The result of the normalization phase is an updated load package that holds your normalized data in a format your destination understands and a full schema which can be used to migrate your data to your destination. You can control the normalization phase, for example, by [defining the allowed nesting level](../../general-usage/source#reduce-the-nesting-level-of-generated-tables) of input data, by [applying schema contracts](../../general-usage/schema-contracts) that govern how the schema might evolve, and how rows that do not fit are treated. Performance settings are [also available](../../reference/performance#normalize).

### Load

During the loading phase, `dlt` first runs schema migrations as needed on your destination and then loads your data into the destination. `dlt` will load your data in smaller chunks called load jobs to be able to parallelize large loads. If the connection to the destination fails, it is safe to rerun the pipeline, and `dlt` will continue to load all load jobs from the current load package. `dlt` will also create special tables that store the internal dlt schema, information about all load packages, and some state information which, among other things, are used by the incrementals to be able to restore the incremental state from a previous run to another machine. Some ways to control the loading phase are by using different [`write_dispositions`](../../general-usage/incremental-loading#choosing-a-write-disposition) to replace the data in the destination, simply append to it, or merge on certain merge keys that you can configure per table. For some destinations, you can use a remote staging dataset on a bucket provider, and `dlt` even supports modern open table formats like [deltables and iceberg](../../dlt-ecosystem/destinations/delta-iceberg), and [reverse ETL](../../dlt-ecosystem/destinations/destination) is also possible.

## Other notable features

`ToDo`

* Runs everywhere
* Data access and Ibis integration
* Transformations with dbt
* Runner integrations...

