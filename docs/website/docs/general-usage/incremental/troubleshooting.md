---
title: Troubleshooting incremental loading
description: Common issues and how to fix them
keywords: [incremental loading, troubleshooting]
---
# Troubleshooting incremental loading

If you see that the incremental loading is not working as expected and the incremental values are not modified between pipeline runs, check the following:

1. Make sure the `destination`, `pipeline_name`, and `dataset_name` are the same between pipeline runs.

2. Check if `dev_mode` is `False` in the pipeline configuration. Check if `refresh` for associated sources and resources is not enabled.

3. Check the logs for the `Bind incremental on <resource_name> ...` message. This message indicates that the incremental value was bound to the resource and shows the state of the incremental value.

4. After the pipeline run, check the state of the pipeline. You can do this by running the following command:

```cli
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

## Growing pipeline state from boundary deduplication

With a closed start range, dlt stores a deduplication hash in `unique_hashes` for each distinct record at the current boundary cursor value. The boundary is the maximum for the default `max` cursor, or the minimum when using `min`. Low-resolution cursors and backfills that assign the same timestamp to many rows can produce a large list, which is persisted in pipeline state.

dlt logs a warning when the boundary hash count crosses `Incremental.duplicate_cursor_warning_threshold` (200 by default). To reduce the state size:

- Use a cursor column with higher resolution so fewer records share the boundary value.
- When a destination `primary_key` merge handles replayed rows, keep the start range closed and disable only extractor deduplication with `primary_key=()` on the incremental object. Keep the resource's primary key and merge disposition. Boundary rows will be extracted and merged again, including late arrivals with the same cursor value:

```py
@dlt.resource(primary_key="id", write_disposition="merge")
def events(updated_at=dlt.sources.incremental("updated_at", primary_key=())):
    ...
```

Do not disable extractor deduplication for append loads unless replayed rows are acceptable. An open start range excludes all rows equal to the saved cursor value, including late arrivals; use it only when the source guarantees those rows cannot arrive later. Destination merging cannot recover rows excluded during extraction.

To stop extraction when the boundary hash count exceeds an application-wide limit, set `Incremental.duplicate_cursor_error_threshold` before running the pipeline:

```py
dlt.sources.incremental.duplicate_cursor_error_threshold = 100_000
```

The limit is disabled (`None`) by default. Exceeding it raises `IncrementalCursorThresholdExceeded` without truncating the hashes. A failed extraction does not commit the new pipeline state. Raise the limit if the larger state is acceptable. The check runs after each item or batch, so it limits persisted state rather than peak batch memory.

## Type mismatch errors

If you encounter an `IncrementalCursorInvalidCoercion` error, it typically means the `initial_value` type does not match the data type of the field in your source data.

### Example

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
