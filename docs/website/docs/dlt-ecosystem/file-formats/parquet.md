---
title: Parquet
description: The parquet file format
keywords: [parquet, file formats]
---
import SetTheFormat from './_set_the_format.mdx';

# Parquet file format

[Apache Parquet](https://en.wikipedia.org/wiki/Apache_Parquet) is a free and open-source column-oriented data storage format in the Apache Hadoop ecosystem. `dlt` is capable of storing data in this format when configured to do so.

To use this format, you need the `pyarrow` package. You can get this package as a `dlt` extra as well:

```sh
pip install "dlt[parquet]"
```

## Supported destinations

Supported by: **BigQuery**, **DuckDB**, **Snowflake**, **Filesystem**, **Athena**, **Databricks**, **Synapse**

## How to configure

<SetTheFormat file_type="parquet"/>

## Destination autoconfig
`dlt` uses [destination capabilities](../../walkthroughs/create-new-destination.md#3-set-the-destination-capabilities) to configure the parquet writer:
* It uses decimal and wei precision to pick the right **decimal type** and sets precision and scale.
* It uses timestamp precision to pick the right **timestamp type** resolution (seconds, micro, or nano).

## Writer settings

Under the hood, `dlt` uses the [pyarrow parquet writer](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html) to create the files. The following options can be used to change the behavior of the writer:

- `flavor`: Sanitize schema or set other compatibility options to work with various target systems. Defaults to None, which is the **pyarrow** default.
- `version`: Determine which Parquet logical types are available for use, whether the reduced set from the Parquet 1.x.x format or the expanded logical types added in later format versions. Defaults to "2.6".
- `data_page_size`: Set a target threshold for the approximate encoded size of data pages within a column chunk (in bytes). Defaults to None, which is the **pyarrow** default.
- `row_group_size`: Set the number of rows in a row group. [See here](#row-group-size) how this can optimize parallel processing of queries on your destination over the default setting of `pyarrow`.
- `timestamp_timezone`: A string specifying the timezone, default is UTC.
- `coerce_timestamps`: resolution to which to coerce timestamps, choose from **s**, **ms**, **us**, **ns**
- `allow_truncated_timestamps` - will raise if precision is lost on truncated timestamps.

:::tip
The default parquet version used by `dlt` is 2.4. It coerces timestamps to microseconds and truncates nanoseconds silently. Such a setting
provides the best interoperability with database systems, including loading panda frames which have nanosecond resolution by default.
:::

Read the [pyarrow parquet docs](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html) to learn more about these settings.

Example:

```toml
[normalize.data_writer]
# the default values
flavor="spark"
version="2.4"
data_page_size=1048576
timestamp_timezone="Europe/Berlin"
```

Or using environment variables:

```sh
NORMALIZE__DATA_WRITER__FLAVOR
NORMALIZE__DATA_WRITER__VERSION
NORMALIZE__DATA_WRITER__DATA_PAGE_SIZE
NORMALIZE__DATA_WRITER__TIMESTAMP_TIMEZONE
```

### Timestamps and timezones
`dlt` adds timezone (UTC adjustment) to all timestamps regardless of the precision (from seconds to nanoseconds). `dlt` will also create TZ-aware timestamp columns in
the destinations. [DuckDB is an exception here](../destinations/duckdb.md#supported-file-formats).

### Disable timezones / UTC adjustment flags
You can generate parquet files without timezone adjustment information in two ways:
1. Set the **flavor** to spark. All timestamps will be generated via the deprecated `int96` physical data type, without the logical one.
2. Set the **timestamp_timezone** to an empty string (i.e., `DATA_WRITER__TIMESTAMP_TIMEZONE=""`) to generate a logical type without UTC adjustment.

To our best knowledge, Arrow will convert your timezone-aware DateTime(s) to UTC and store them in parquet without timezone information.


### Row group size

The `pyarrow` parquet writer writes each item, i.e., table or record batch, in a separate row group. This may lead to many small row groups, which may not be optimal for certain query engines. For example, `duckdb` parallelizes on a row group. `dlt` allows controlling the size of the row group by [buffering and concatenating tables](../../reference/performance.md#controlling-in-memory-buffers) and batches before they are written. The concatenation is done as a zero-copy to save memory. You can control the size of the row group by setting the maximum number of rows kept in the buffer.

```toml
[extract.data_writer]
buffer_max_items=10e6
```

Keep in mind that `dlt` holds the tables in memory. Thus, 1,000,000 rows in the example above may consume a significant amount of RAM.

The `row_group_size` configuration setting has limited utility with the `pyarrow` writer. It may be useful when you write single very large pyarrow tables or when your in-memory buffer is really large.

