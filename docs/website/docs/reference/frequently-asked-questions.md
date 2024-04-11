---
title: Frequently Asked Questions
description: Questions asked frequently by users in technical help or github issues
keywords: [faq, usage information, technical help]
---


## Can I configure different nesting levels for each resource in a source within DLT?

Currently, configuring different nesting levels for each resource directly is not supported, but there's an open GitHub issue ([#945](https://github.com/dlt-hub/dlt/issues/945)) addressing this. Meanwhile, you can use these workarounds.

Resources can be seperated based on nesting needs, for example separate the execution of pipelines for resources based on their required maximum table nesting levels.

**For resources that don't require nesting (resource1, resource2), configure nesting as:**

```py
source_data = my_source.with_resources("resource1", "resource2")
source_data.max_table_nesting = 0
load_info = pipeline.run(source_data)
```

**For resources that require deeper nesting (resource3, resource4), configure nesting as:**

```py
source_data = my_source.with_resources("resource3", "resource4")
source_data.max_table_nesting = 2
load_info = pipeline.run(source_data)
```

**Apply hints for complex columns**
If certain columns should not be normalized, you can mark them as `complex`. This can be done in two ways.

1. When fetching the source data.
   ```py
   source_data = my_source()
   source_data.resource3.apply_hints(
       columns={
           "column_name": {
               "data_type": "complex"
           }
       }
   )
   ```

1. During resource definition.
   ```py
   @dlt.resource(columns={"column_name": {"data_type": "complex"}})
   def my_resource():
       # Function body goes here
       pass
   ```
In this scenario, the specified column (column_name) will not be broken down into nested tables, regardless of the data structure.

These methods allow for a degree of customization in handling data structure and loading processes, serving as interim solutions until a direct feature is developed.

## Can I configure dlt to load data in chunks of 10,000 records for more efficient processing, and how does this affect data resumption and retries in case of failures?

`dlt` buffers to disk and can resume and retry so there is not big benefit of to doing that unless you run serverless. If you go this way you will have to manage atomicity after the fact, otherwise `dlt` will do the same via disk and load atomically. So the benefit to loading every 10k records would be that chunks arrive sooner (if you are actively reading) or that in case it breaks, if state is well handled and records are sorted, you would resume from where you last loaded on breakages. 

It is to be noted that `dlt` has a request library replacement that has built-in retries. So if you pull 10m records one by one your data should be safe even with some network issues. Note that for resuming jobs after a failure, running the pipeline in its own virtual machine (VM) is necessary, as ephemeral storage solutions like Cloud Run won't support resumption.

## How to contribute a verified source?

To submit your source code to our verified source repository, please refer to [this guide](https://github.com/dlt-hub/verified-sources/blob/master/CONTRIBUTING.md).

## If you don't have the source required listed in verified sources?

In case you don't have the source required listed in the [verified sources](../../docs/dlt-ecosystem/verified-sources/), you could create your own pipeline by refering to the [following docs](../../docs/walkthroughs/create-a-pipeline). 

## How can I retrieve the complete schema of a `dlt` source?

To retrieve the complete schema of a `dlt` source as `dlt` recognizes it, you need to execute both the extract and normalization steps. This process can be conducted on a small sample size to avoid processing large volumes of data. You can limit the sample size by applying an `add_limit` to your resources. Here is a sample code snippet demonstrating this process:

```py
# Initialize a new DLT pipeline with specified properties
p = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination="duckdb",
    dataset_name="my_dataset"
)

# Extract data using the predefined source `my_source`
p.extract(my_source)

# Normalize the data structure for consistency
p.normalize()

# Print the default schema of the pipeline in pretty YAML format for review
print(p.default_schema.to_pretty_yaml())
```

This method ensures you obtain the full schema details, including all columns, as seen by `dlt`, without needing a predefined contract on these resources.

## Can `dlt` periodically commit updates to a resource state to an external database during a pipeline run?

Currently, `dlt` does not offer the functionality to checkpoint the extraction process during its execution. This limitation means that any updates to a resource state are not committed until the pipeline has fully completed its run. This issue has been recognized by the `dlt` development team and is currently being tracked under a specific GitHub issue ([#215](https://github.com/dlt-hub/dlt/issues/215)). Unfortunately, there is no specified timeline for when this feature will be available.

In light of this limitation, `dlt` advises a workaround for incremental loading. This involves specifying both the start and end dates or page numbers for your data extraction process and conducting the backfill using smaller, manageable ranges, which are usually divided into around 100 loads. Implementing this strategy provides a higher degree of control and significantly mitigates the risk of losing any progress should the pipeline experience a failure or if it needs to be stopped midway through execution. For a more comprehensive understanding of how to implement this workaround, it is recommended to consult `dlt's` documentation on incremental loading with a cursor field, which can be found here: [Incremental Loading with a Cursor Field](../../docs/general-usage/incremental-loading#incremental-loading-with-a-cursor-field).

## Is truncating or deleting a staging table safe?

You can safely truncate those or even drop the whole staging dataset. However, it will have to be recreated on the next load and might incur extra loading time or cost.

## How can I develop a "custom" pagination tracker?

There are two ways. One, you can use `dlt.sources.incremental` to create a custom cursor for tracking pagination in data streams that lack an explicit cursor field.

Second, while it's possible to utilize `dlt.sources.incremental` for tracking changes in data streams, you're not limited to this method for managing pagination or custom cursors. Instead, you can leverage the flexibility of managing state directly in Python. Access and modify the state like a standard Python dictionary: `state = dlt.current.resource_state(); state["your_custom_key"] = "your_value"`. This approach allows you to define your custom pagination logic based on your specific needs.

However, be cautious about overusing the state dictionary, especially in cases involving substreams for each user, as it might become unwieldy. A better strategy might involve tracking users incrementally. Then, upon updates, you only refresh the affected users' substreams entirely. This consideration helps maintain efficiency and manageability in your custom pagination implementation.

## What's the best way to add an "_orchestrator_pipeline_run_id" to datasets during ingestion to trace back to the specific run or code?

To include an `_orchestrator_pipeline_run_id`, utilize environmental variables by adding this identifier to your data (e.g., in a dictionary or dataframe) before yielding it in your resource. Alternatively, create a dedicated resource for a lookup table mapping `_dlt_load_id` to `_orchestrator_pipeline_run_id`. This method ensures minimal storage impact while maintaining efficient traceability of your data assets.