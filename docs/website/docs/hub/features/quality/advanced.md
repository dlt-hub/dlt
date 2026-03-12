---
title: "Data quality (advanced)"
description: Validate your data and control its quality
keywords: ["dlthub", "data quality", "contracts", "check", "metrics"]
---

:::warning
🚧 This feature is under development. Interested in becoming an early tester? [Join dltHub early access](https://info.dlthub.com/waiting-list).
:::

This page covers more in-depth details about data quality features, such as metrics and checks. It shows the full flexibility available for advanced use cases. It can also serve as an FAQ and helper for debugging.

Note that these APIs are more likely to change than the basics page. 

## Metrics and checks are `DltSource` objects.
Internally, metrics and checks are defined as `dlt.source` objects that are executed by the pipeline. You can interact with these objects directly, unlocking the ability to:

- load data into destination A and write metrics and checks to destination B
- run data quality checks and metrics disjointed from the primary `pipeline.run()` (e.g., on a schedule, after another trigger, etc.)
- dynamically modify metrics and checks to execute via Python code
- have a central data quality pipeline that you run for multiple pipelines / datasets


## Metrics and checks use a special `dlt.Schema`
Currently, a `dlt.Dataset` is associated with a single `dlt.Schema`. This defines what tables are known and available to the dataset's API (e.g., `dataset.table(...)`).

But a `dlt.Pipeline` can be associated with multiple `dlt.Schema`, such as when loading several sources.

The data quality metrics and checks sources have a dedicated `_dlt_data_quality` schema with internal tables for metrics and checks results. These tables are not directly visible via the `dlt.Dataset` API. (this may change in the future)

The `dq.read_metric()` and `dq.read_check()` retrieve data from these internal tables.


## Checks anatomy: result, decision, outcome, level

In its simplest form, a check is a function that returns an **outcome** (boolean) that indicates success or failure. Under the hood, it involves a **result** (any type) that is computed and converted to an **outcome** via a **decision** function.

For example, the check `is_in(column_name, accepted_values)` has a success outcome if all values are valid values.

A more granular implementation could be:
- count the number of records with valid values (**result**)
- count the number of records
- if the ratio of valid records is higher than `0.95` (**decision**)
- the **outcome** is a success, else it is a failure

Most built-in checks return the outcome directly, but they can be configured to store intermediary results. Those are most useful for custom checks or when wanting to assign different tolerance thresholds based on the environment / context.

### Check level
So far, we explained how a result is converted into an outcome. The check **level** describes the granularity of the **result**.

For instance:
- **Row-level** checks produce a result per record. It's possible to inspect which specific records pass / failed the check.

- **Table-level** checks produce a result per table (e.g., result is "the number of unique values" and decision is "is this greater than 5?"). 

    These checks can often be rewritten as row-level checks (e.g., "is this value unique?")

- **Dataset-level** checks produce a result per dataset. This typically involves multiple tables, temporal comparisons, or pipeline information (e.g., "the number of rows in `orders` is higher than the number of rows in 'customers')

:::important
Notice that the **check level** relates to the result and not the **input data** of the check. For instance, a row-level check can involve multiple tables as input.
:::
