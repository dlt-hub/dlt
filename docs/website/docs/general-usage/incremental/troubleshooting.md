---
title: Troubleshooting incremental loading
description: Common issues and how to fix them
keywords: [incremental loading, troubleshooting]
---

If you see that the incremental loading is not working as expected and the incremental values are not modified between pipeline runs, check the following:

1. Make sure the `destination`, `pipeline_name`, and `dataset_name` are the same between pipeline runs.

2. Check if `dev_mode` is `False` in the pipeline configuration. Check if `refresh` for associated sources and resources is not enabled.

3. Check the logs for the `Bind incremental on <resource_name> ...` message. This message indicates that the incremental value was bound to the resource and shows the state of the incremental value.

4. After the pipeline run, check the state of the pipeline. You can do this by running the following command:

```sh
dlt pipeline -v <pipeline_name> info
```

For example, if your pipeline is defined as follows:

```py
@dlt.resource
def my_resource(
    incremental_object = dlt.sources.incremental("some_key", initial_value=0),
):
    ...

pipeline = dlt.pipeline(
    pipeline_name="example_pipeline",
    destination="duckdb",
)

pipeline.run(my_resource)
```

You'll see the following output:

```text
Attaching to pipeline <pipeline_name>
...

sources:
{
  "example": {
    "resources": {
      "my_resource": {
        "incremental": {
          "some_key": {
            "initial_value": 0,
            "last_value": 42,
            "unique_hashes": [
              "nmbInLyII4wDF5zpBovL"
            ]
          }
        }
      }
    }
  }
}
```

Verify that the `last_value` is updated between pipeline runs.

### Growing pipeline state from boundary deduplication

To avoid loading the same row twice, dlt keeps a deduplication hash in `unique_hashes` for **every record that shares the current boundary (maximum) cursor value**. This list is stored in the pipeline state and re-written on every run. When many records share the same cursor value — for example a low-resolution cursor (a date without a time) or a backfill that stamps every row with the same timestamp — `unique_hashes` can grow to tens of MB and bloat the `_dlt_pipeline_state` table.

dlt logs a warning once the number of boundary records passes `Incremental.duplicate_cursor_warning_threshold` (200 by default). To address it:

- Use a cursor column with higher resolution so fewer records share the same value.
- If you do not need boundary deduplication (for example because a `primary_key` merge already removes duplicates, or rows never overlap between runs), switch to `merge_key` together with `range_start="open"`, which excludes records equal to the last value and disables the deduplication state.
- Set `Incremental.duplicate_cursor_error_threshold` (disabled by default) to fail with `IncrementalCursorThresholdExceeded` instead of silently accumulating state once the boundary record count crosses the threshold:

```py
dlt.sources.incremental.duplicate_cursor_error_threshold = 100_000
```

### Type mismatch errors

If you encounter an `IncrementalCursorInvalidCoercion` error, it typically means the `initial_value` type does not match the data type of the field in your source data.

#### Example

This fails because the `initial_value` is an integer, but the `created_at` values are string-formatted timestamps:
```py
# This fails: integer initial_value with string timestamps
@dlt.resource
def my_data(
    created_at=dlt.sources.incremental("created_at", initial_value=9999)
):
    yield [{"id": 1, "created_at": "2024-01-01 00:00:00"}]
```

To fix this, use a string timestamp that matches the format of the source field:
```py
created_at = dlt.sources.incremental("created_at", initial_value="2024-01-01 00:00:00")
```

To avoid similar issues:

- Always ensure the `initial_value` type matches the data type in the source field.
- If the field requires transformation, apply `add_map` to convert the type before incremental tracking.
- Use a separate column if needed to retain the original format for downstream processing or reference.
