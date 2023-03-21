---
sidebar_position: 3
---

# Pipeline

  > **💡** Moves the data from the source to the destination, according to instructions provided
  in the schema (i.e. extracting, normalizing, and loading the data).


A [pipeline](../glossary.md#pipeline) is a connection to the destination. We pass sources or resources to the pipeline. We can also pass generators to the pipeline. When the pipeline runs, the resources get executed and the data is loaded at destination.

Arguments:
- `source` Source may be a dlt source, resource or a generator
- `name` is the unique identifier for the pipeline. This unique identifier is used to reference the logs and the state of the pipeline.
- `full_refresh` (bool) is a toggle you can use during development to avoid conflicts between pipeline states as you develop them. Full refresh will create a new dataset version with a timestamp in the name. Once you are done developing, make sure to turn it off to funnel the data back into the right dataset.

Example: This pipeline will load the data the generator `gen(10)` produces

```python
import dlt

def gen(nr):
    for i in range(nr):
        yield {'id':1}

pipeline = dlt.pipeline(destination='bigquery',dataset_name='sql_database_data')

info = pipeline.run(gen(10))

print(info)
```

A pipeline’s unique identifier is the name. If no name is given, then the pipeline takes the name from the currently executing file.
