---
title: Arrow Table / Pandas
description: dlt source for Arrow tables and Pandas dataframes
keywords: [arrow, pandas, parquet, source]
---
import Header from './_source-info-header.md';

# Arrow table / Pandas

<Header/>

You can load data directly from an Arrow table or Pandas dataframe.
This is supported by all destinations, but it is especially recommended when using destinations that support the `parquet` file format natively (e.g., [Snowflake](../destinations/snowflake.md) and [Filesystem](../destinations/filesystem.md)).
See the [destination support](#destination-support-and-fallback) section for more information.

When used with a `parquet` supported destination, this is a more performant way to load structured data since `dlt` bypasses many processing steps normally involved in passing JSON objects through the pipeline.
`dlt` automatically translates the Arrow table's schema to the destination table's schema and writes the table to a parquet file, which gets uploaded to the destination without any further processing.

## Usage

To write an Arrow source, pass any `pyarrow.Table`, `pyarrow.RecordBatch`, or `pandas.DataFrame` object (or list thereof) to the pipeline's `run` or `extract` method, or yield table(s)/dataframe(s) from a `@dlt.resource` decorated function.

This example loads a Pandas dataframe to a Snowflake table:

```py
import dlt
from dlt.common import pendulum
import pandas as pd


df = pd.DataFrame({
    "order_id": [1, 2, 3],
    "customer_id": [1, 2, 3],
    "ordered_at": [pendulum.DateTime(2021, 1, 1, 4, 5, 6), pendulum.DateTime(2021, 1, 3, 4, 5, 6), pendulum.DateTime(2021, 1, 6, 4, 5, 6)],
    "order_amount": [100.0, 200.0, 300.0],
})

pipeline = dlt.pipeline("orders_pipeline", destination="snowflake")

pipeline.run(df, table_name="orders")
```

A `pyarrow` table can be loaded in the same way:

```py
import pyarrow as pa

# Create dataframe and pipeline same as above
...

table = pa.Table.from_pandas(df)
pipeline.run(table, table_name="orders")
```

Note: The data in the table must be compatible with the destination database as no data conversion is performed. Refer to the documentation of the destination for information about supported data types.

## Destination support

Destinations that support the `parquet` format natively will have the data files uploaded directly as possible. Rewriting files can be avoided completely in many cases.

When the destination does not support `parquet`, the rows are extracted from the table and written in the destination's native format (usually `insert_values`), and this is generally much slower
as it requires processing the table row by row and rewriting data to disk.

The output file format is chosen automatically based on the destination's capabilities, so you can load arrow or pandas frames to any destination, but performance will vary.

### Destinations that support parquet natively for direct loading
* duckdb & motherduck
* redshift
* bigquery
* snowflake
* filesystem
* athena
* databricks
* dremio
* synapse


## Add `_dlt_load_id` and `_dlt_id` to your tables

`dlt` does not add any data lineage columns by default when loading Arrow tables. This is to give the best performance and avoid unnecessary data copying.

But if you need them, the `_dlt_load_id` (ID of the load operation when the row was added) and `_dlt_id` (unique ID for the row) columns can be added respectively with the following configuration options:

```toml
[normalize.parquet_normalizer]
add_dlt_load_id = true
add_dlt_id = true
```

Keep in mind that enabling these incurs some performance overhead:

- `add_dlt_load_id` has minimal overhead since the column is added to the arrow table in memory during the `extract` stage, before the parquet file is written to disk
- `add_dlt_id` adds the column during the `normalize` stage after the file has been extracted to disk. The file needs to be read back from disk in chunks, processed, and rewritten with new columns

## Incremental loading with Arrow tables

You can use incremental loading with Arrow tables as well.
Usage is the same as with other dlt resources. Refer to the [incremental loading](../../general-usage/incremental-loading.md) guide for more information.

Example:

```py
import dlt
from dlt.common import pendulum
import pandas as pd

# Create a resource that yields a dataframe, using the `ordered_at` field as an incremental cursor
@dlt.resource(primary_key="order_id")
def orders(ordered_at = dlt.sources.incremental('ordered_at')):
    # Get a dataframe/arrow table from somewhere
    # If your database supports it, you can use the last_value to filter data at the source.
    # Otherwise, it will be filtered automatically after loading the data.
    df = get_orders(since=ordered_at.last_value)
    yield df

pipeline = dlt.pipeline("orders_pipeline", destination="snowflake")
pipeline.run(orders)
# Run again to load only new data
pipeline.run(orders)
```

:::tip
Look at the [Connector X + Arrow Example](../../examples/connector_x_arrow/) to see how to load data from production databases fast.
:::

## Loading JSON documents
If you want to skip the default `dlt` JSON normalizer, you can use any available method to convert JSON documents into tabular data.
* **pandas** has `read_json` and `json_normalize` methods
* **pyarrow** can infer the table schema and convert JSON files into tables with `read_json`
* **duckdb** can do the same with `read_json_auto`

```py
import duckdb

conn = duckdb.connect()
table = conn.execute(f"SELECT * FROM read_json_auto('{json_file_path}')").fetch_arrow_table()
```

Note that **duckdb** and **pyarrow** methods will generate [nested types](#loading-nested-types) for nested data, which are only partially supported by `dlt`.

## Supported Arrow data types

The Arrow data types are translated to dlt data types as follows:

| Arrow type        | dlt type    | Notes                                                      |
|-------------------|-------------|------------------------------------------------------------|
| `string`          | `text`      |                                                            |
| `float`/`double`  | `double`    |                                                            |
| `boolean`         | `bool`      |                                                            |
| `timestamp`       | `timestamp` | Precision is determined by the unit of the timestamp.      |
| `date`            | `date`      |                                                            |
| `time<bit_width>` | `time`      | Precision is determined by the unit of the time.           |
| `int<bit_width>`  | `bigint`    | Precision is determined by the bit width.                  |
| `binary`          | `binary`    |                                                            |
| `decimal`         | `decimal`   | Precision and scale are determined by the type properties. |
| `struct`          | `json`      |                                                            |
|                   |             |                                                            |


## Loading nested types
All struct types are represented as `json` and will be loaded as JSON (if the destination permits) or a string. Currently, we do not support **struct** types,
even if they are present in the destination (except **BigQuery** which can be [configured to handle them](../destinations/bigquery.md#use-bigquery-schema-autodetect-for-nested-fields))

If you want to represent nested data as separate tables, you must yield panda frames and arrow tables as records. In the examples above:
```py
# yield panda frame as records
pipeline.run(df.to_dict(orient='records'), table_name="orders")

# yield arrow table
pipeline.run(table.to_pylist(), table_name="orders")
```
Both Pandas and Arrow allow streaming records in batches.

