---
title: Frequently asked questions
description: Questions asked frequently by users in technical help or github issues
keywords: [faq, usage information, technical help]
---

## Can I configure different nesting levels for each resource?

Yes, [this feature is available](../general-usage/resource.md#reduce-the-nesting-level-of-generated-tables). You can also control the nesting on a level of a particular column:

**Apply hints for nested columns**
If certain columns should not be normalized, you can mark them as `json`. This can be done in two ways.

1. When fetching the source data.
   ```py
   source_data = my_source()
   source_data.resource3.apply_hints(
       columns={
           "column_name": {
               "data_type": "json"
           }
       }
   )
   ```

1. During resource definition.
   ```py
   @dlt.resource(columns={"column_name": {"data_type": "json"}})
   def my_resource():
       # Function body goes here
       pass
   ```
In this scenario, the specified column (column_name) will not be broken down into nested tables, regardless of the data structure.

These methods allow for a degree of customization in handling data structure and loading processes, serving as interim solutions until a direct feature is developed.

## Can I configure dlt to load data in chunks of 10,000 records for more efficient processing, and how does this affect data resumption and retries in case of failures?

`dlt` buffers to disk and has built-in resume and retry mechanisms. This makes it less beneficial to manually manage atomicity after the fact unless you're running serverless. If you choose to load every 10k records instead, you could potentially see benefits like quicker data arrival if you're actively reading, and easier resumption from the last loaded point in case of failure, assuming that state is well-managed and records are sorted.

It's worth noting that `dlt` includes a request library replacement with [built-in retries](../reference/performance#using-the-built-in-requests-client). This means if you pull 10 million records individually, your data should remain safe even in the face of network issues. To resume jobs after a failure, however, it's necessary to run the pipeline in its own virtual machine (VM). Ephemeral storage solutions like Cloud Run don't support job resumption.

## How to contribute a verified source?

To submit your source code to our verified source repository, please refer to [this guide](https://github.com/dlt-hub/verified-sources/blob/master/CONTRIBUTING.md).

## If you don't have the source required listed in verified sources?

In case you don't have the source required listed in the [verified sources](../dlt-ecosystem/verified-sources/), you could create your own pipeline by referring to the [following docs](../walkthroughs/create-a-pipeline).

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

## Is truncating or deleting a staging table safe?

You can safely truncate or even drop the whole staging dataset. However, it will have to be recreated on the next load and might incur extra loading time or cost.
You can also delete it with Python using the [Bigquery client](https://cloud.google.com/bigquery/docs/samples/bigquery-delete-dataset#bigquery_delete_dataset-python).

## How can I develop a "custom" pagination tracker?

You can use `dlt.sources.incremental` to create a custom cursor for tracking pagination in data streams that lack a specific cursor field. An example can be found in the [Incremental loading with a cursor](../general-usage/incremental-loading.md#incremental-loading-with-a-cursor-field).

Alternatively, you can manage the state directly in Python. You can access and modify the state like a standard Python dictionary:
```py
state = dlt.current.resource_state()
state["your_custom_key"] = "your_value"
```
This method allows you to create custom pagination logic based on your requirements. An example of using `resource_state()` for pagination can be found [here](../general-usage/incremental-loading#custom-incremental-loading-with-pipeline-state).

However, be cautious about overusing the state dictionary, especially in cases involving substreams for each user, as it might become unwieldy. A better strategy might involve tracking users incrementally. Then, upon updates, you only refresh the affected users' substreams entirely. This consideration helps maintain efficiency and manageability in your custom pagination implementation.

