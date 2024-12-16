---
title: How dlt works
description: How data load tool (dlt) works
keywords: [architecture, extract, normalize, load]
---

# How `dlt` works

In a nutshell, `dlt` automatically turns data from a number of available [sources](../../general-usage/source) (e.g., an API, a PostgreSQL database, or Python data structures) into a live dataset stored in a [destination](../../general-usage/destination) of your choice (e.g., Google BigQuery, a Deltalake on Azure, or by pushing the data back via reverse ETL). You can easily implement your own sources, as long as you yield data in a way that is compatible with `dlt`, such as JSON objects, Python lists and dictionaries, pandas dataframes, and arrow tables. `dlt` will be able to automatically compute the schema and move the data to your destination.

![architecture-diagram](/img/architecture-diagram.png)

The main building block of `dlt` is the [pipeline](../../general-usage/glossary.md#pipeline), which orchestrates the loading of data from your source into your destination in three discrete steps when you call `my_pipeline.run(my_source())`:

1. [Extract](how-dlt-works.md#extract) - Fully extracts the data from your source to your hard drive.
2. [Normalize](how-dlt-works.md#normalize) - Inspects and normalizes your data and computes a schema compatible with your destination.
3. [Load](how-dlt-works#load) - Runs schema migrations if necessary on your destination and loads your data into the destination.


## Extract

During the extract phase, `dlt` fully extracts the data from your sources to your hard drive into a new load package, which will be assigned a unique ID and will contain your raw data as received from your sources. Additionally, you can supply schema hints to, for example, define the data types of some of the columns. You can also control this phase by limiting the amount of items extracted in one run, using incrementals, and by tuning the performance with parallelization. You can also apply filters and maps to obfuscate or remove personal data, and you can use transformers to create derivative data.

## Normalize

During the normalization phase, `dlt` inspects and normalizes your data and computes a schema corresponding to the input data. The schema will automatically evolve to accommodate any future source data changes, for example, new columns or tables. `dlt` will also unnest nested data structures into child tables and create variant columns if detected values do not match a schema computed during a previous run. The result of the normalization phase is an updated load package that holds your normalized data in a format your destination understands and a full schema which can be used to migrate your data to your destination. You can control the normalization phase, for example, by defining the allowed nesting level of input data, by applying schema contracts that govern how the schema might evolve, and how rows that do not fit are treated.

## Load

During the loading phase, `dlt` first runs schema migrations, if necessary, on your destination and then loads your data into the destination. `dlt` will load your data in smaller chunks called load jobs to be able to parallelize large loads. If the connection to the destination fails, it is safe to rerun the pipeline, and `dlt` will continue to load all load jobs from the current load package. `dlt` will also create special tables that store the internal dlt schema, information about all load packages, and some state information which, among other things, are used by the incrementals to be able to restore the incremental state from a previous run to another machine. You can control the loading phase, for example, by using different `write_dispositions` to replace the data in the destination, simply append to it, or merge on certain merge keys that you can configure per table. For some destinations, you can use a remote staging dataset on a bucket provider, and `dlt` even supports modern open table formats like deltables and iceberg, and Reverse ETL is also possible.

