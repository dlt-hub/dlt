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
