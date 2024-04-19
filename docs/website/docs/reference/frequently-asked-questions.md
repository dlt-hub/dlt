---
title: Frequently Asked Questions
description: Questions asked frequently by users in technical help or github issues
keywords: [faq, usage information, technical help]
---


## Can I configure different nesting levels for each resource?

Currently, configuring different nesting levels for each resource directly is not supported, but there's an open GitHub issue ([#945](https://github.com/dlt-hub/dlt/issues/945)) addressing this. Meanwhile, you can use these workarounds.

Resources can be separated based on nesting needs, for example, separate the execution of pipelines for resources based on their required maximum table nesting levels.

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

`dlt` buffers to disk, and has built-in resume and retry mechanisms. This makes it less beneficial to manually manage atomicity after the fact unless you're running serverless. If you choose to load every 10k records instead, you could potentially see benefits like quicker data arrival if you're actively reading, and easier resumption from the last loaded point in case of failure, assuming that state is well-managed and records are sorted.

It's worth noting that `dlt` includes a request library replacement with [built-in retries](../../docs/reference/performance#using-the-built-in-requests-client). This means if you pull 10 million records individually, your data should remain safe even in the face of network issues. To resume jobs after a failure, however, it's necessary to run the pipeline in its own virtual machine (VM). Ephemeral storage solutions like Cloud Run don't support job resumption.

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
p.extract(my_source().add_limit(10))

# Normalize the data structure for consistency
p.normalize()

# Print the default schema of the pipeline in pretty YAML format for review
print(p.default_schema.to_pretty_yaml())
```

This method ensures you obtain the full schema details, including all columns, as seen by `dlt`, without needing a predefined contract on these resources.

## Can `dlt` periodically commit updates to a resource state?
https://dlthub-community.slack.com/archives/C04DQA7JJN6/p1710534100683919

Currently, `dlt` does not offer the functionality to checkpoint the extraction process during its execution. This limitation means that any updates to a resource state are not committed until the pipeline has fully completed its run. This issue has been recognized by the `dlt` development team and is currently being tracked under a specific GitHub issue ([#215](https://github.com/dlt-hub/dlt/issues/215)). Unfortunately, there is no specified timeline for when this feature will be available.

In light of this limitation, `dlt` advises a workaround using incremental loading. This involves specifying the start and end dates or page numbers for your data extraction process and conducting the backfill using smaller, manageable ranges. For e.g. 100 loads. Implementing this strategy provides a higher degree of control. It significantly mitigates the risk of losing any progress should the pipeline experience a failure or if it needs to be stopped midway through execution. For a more comprehensive understanding of how to implement this workaround, it is recommended to consult `dlt's` documentation on incremental loading with a cursor field, which can be found here: [Incremental Loading with a Cursor Field](../../docs/general-usage/incremental-loading#incremental-loading-with-a-cursor-field).

## Is truncating or deleting a staging table safe?

You can safely truncate those or even drop the whole staging dataset. However, it will have to be recreated on the next load and might incur extra loading time or cost.
You can also delete it with Python using [Bigquery client.](https://cloud.google.com/bigquery/docs/samples/bigquery-delete-dataset#bigquery_delete_dataset-python)

## How can I develop a "custom" pagination tracker?

There are two ways. One, you can use `dlt.sources.incremental` to create a custom cursor for tracking pagination in data streams that lack an explicit cursor field.

Second, while it's possible to utilize `dlt.sources.incremental` for tracking changes in data streams, you're not limited to this method for managing pagination or custom cursors. Instead, you can leverage the flexibility of managing state directly in Python. Access and modify the state like a standard Python dictionary: `state = dlt.current.resource_state(); state["your_custom_key"] = "your_value"`. This approach allows you to define your custom pagination logic based on your specific needs. Here's an example: [Link.](https://github.com/dlt-hub/verified-sources/blob/master/sources/chess/__init__.py#L95)

However, be cautious about overusing the state dictionary, especially in cases involving substreams for each user, as it might become unwieldy. A better strategy might involve tracking users incrementally. Then, upon updates, you only refresh the affected users' substreams entirely. This consideration helps maintain efficiency and manageability in your custom pagination implementation.

## What is the best way to add a "run_id" from the orchestrator runs to datasets during ingestion to trace back to the specific run or code?

To include an orchestrator's `run_id`, if it is present as an environmental variable, you can add an identifier to your data (e.g., in a dictionary or data frame) before yielding it in your resource. Alternatively, you can create a dedicated resource for a lookup table mapping `_dlt_load_id` to `run_id`. This method ensures minimal storage impact while maintaining efficient traceability of your data assets.
