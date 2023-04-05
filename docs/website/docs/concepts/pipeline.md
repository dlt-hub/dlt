# Pipeline

  > **💡** Moves the data from the source to the destination, according to instructions provided
  in the schema (i.e. extracting, normalizing, and loading the data).


A [pipeline](../glossary.md#pipeline) is a connection to the destination. We pass sources or resources to the pipeline. We can also pass generators to the pipeline. When the pipeline runs, the resources get executed and the data is loaded at destination.

Arguments:
- `source` may be a dlt source, resource, generator function, or any iterator / iterable (i.e. a list or the result of `map` function)
- `name` is the unique identifier for the pipeline, which is used to reference the logs and the state of the pipeline
- `full_refresh` is a boolean you can toggle during development to avoid conflicts between pipeline states as you develop them. Full refresh will create a new dataset version with a timestamp in the name. Once you are done developing, make sure to turn it off to funnel the data back into the right dataset.

Example: This pipeline will load a list of objects into table with a name "three"
```python
import dlt

pipeline = dlt.pipeline(destination="duckdb", dataset_name="sequence")

info = pipeline.run([{'id':1}, {'id':2}, {'id':3}], table_name="three")

print(info)
```

Example: This pipeline will load the data the generator `gen(10)` produces
```python
import dlt

def gen(nr):
    for i in range(nr):
        yield {'id':1}

pipeline = dlt.pipeline(destination='bigquery', dataset_name='sql_database_data')

info = pipeline.run(gen(10))

print(info)
```

A pipeline’s unique identifier is the name. If no name is given, then the pipeline takes the name from the currently executing file.
