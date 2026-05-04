# Destination tables & lineage

> **Full documentation lives at:** [dlthub.com/docs/general-usage/destination-tables](https://dlthub.com/docs/general-usage/destination-tables)

## Data lineage

Data lineage can be super relevant for architectures like the [data vault architecture](https://www.data-vault.co.uk/what-is-data-vault/) or when troubleshooting. The data vault architecture is a data warehouse that large organizations use when representing the same process across multiple systems, which adds data lineage requirements. Using the pipeline name and `load_id` provided out of the box by `dlt`, you are able to identify the source and time of data.

You can save complete lineage info for a particular `load_id` including a list of loaded files, error messages (if any), elapsed times, and schema changes. This can be helpful, for example, when troubleshooting problems.

### Load IDs

Each pipeline run produces a unique `load_id` (a Unix timestamp). This ID appears in every top-level table row and in the `_dlt_loads` system table, letting you trace exactly when and from which source each record was loaded.

### Row-level lineage

Every row in every table gets a `_dlt_id` column — a unique, stable identifier. Child (nested) tables reference their parent rows via `_dlt_parent_id`, forming a complete audit trail from source to destination.

### Schema versioning

dlt tracks schema changes using a content-based `version_hash`. You can correlate a `load_id` to the schema version active at that time, enabling column-level lineage: you can assign the origin of any column to a specific load package, identified by source and time.

### Saving lineage info

```py
import dlt

pipeline = dlt.pipeline(pipeline_name="my_pipeline", destination="duckdb")
load_info = pipeline.run(my_source())

# Persist load info back into the destination for lineage tracking
pipeline.run([load_info], write_disposition="append", table_name="load_info")
```

For full details see the [hosted documentation](https://dlthub.com/docs/general-usage/destination-tables#data-lineage).
