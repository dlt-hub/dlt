---
title: Parquet
description: The parquet file format
keywords: [parquet, file formats]
---

# Parquet file format

[Apache Parquet](https://en.wikipedia.org/wiki/Apache_Parquet) is a free and open-source
column-oriented data storage format in the Apache Hadoop ecosystem. `dlt` is able to store data in
this format when configured to do so.

To use this format you need a `pyarrow` package. You can get this package as a `dlt` extra as well:

```sh
pip install dlt[pyarrow]
```

## Supported destinations

Supported by: **BigQuery**, **DuckDB**, **Snowflake**, **filesystem**.

By setting the `loader_file_format` argument to `parquet` in the run command, the pipeline will
store your data in the parquet format to the destination:

```python
info = pipeline.run(some_source(), loader_file_format="parquet")
```

## Options

Under the hood `dlt` uses the
[pyarrow parquet writer](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html)
to create the files. The following options can be used to change the behavior of the writer:

- `flavor`: Sanitize schema or set other compatibility options to work with various target systems.
  Defaults to "spark".
- `version`: Determine which Parquet logical types are available for use, whether the reduced set
  from the Parquet 1.x.x format or the expanded logical types added in later format versions.
  Defaults to "2.4".
- `data_page_size`: Set a target threshold for the approximate encoded size of data pages within a
  column chunk (in bytes). Defaults to "1048576".

Read the
[pyarrow parquet docs](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html)
to learn more about these settings.

Example:

```toml
[normalize.data_writer]
# the default values
flavor="spark"
version="2.4"
data_page_size=1048576
```

or using environment variables:

```
NORMALIZE__DATA_WRITER__FLAVOR
NORMALIZE__DATA_WRITER__VERSION
NORMALIZE__DATA_WRITER__DATA_PAGE_SIZE
```
