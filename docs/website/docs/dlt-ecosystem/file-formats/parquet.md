---
title: Parquet
description: The parquet file format
keywords: [parquet, file-formats]
---

[Apache Parquet](https://en.wikipedia.org/wiki/Apache_Parquet) is a free and open-source column-oriented data storage format in the Apache Hadoop ecosystem. dlthub is able to store data in this format when configured to do so. 

By setting the `loader_file_format` argument to `parquet` in the run command, the pipeline will store your data in the parquet format to the destination:

`info = pipeline.run(some_source(), loader_file_format="parquet")`

Under the hood dlt uses the [pyarrow parquet writer](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html) to create the files. The following options can be used to change the behavior of the writer:

```
NORMALIZE__DATA_WRITER__FLAVOR
NORMALIZE__DATA_WRITER__VERSION
NORMALIZE__DATA_WRITER__DATA_PAGE_SIZE
```

* `flavor`: Sanitize schema or set other compatibility options to work with various target systems. Defaults to spark.
* `version`: Determine which Parquet logical types are available for use, whether the reduced set from the Parquet 1.x.x format or the expanded logical types added in later format versions. Defaults to 2.4.
* `data_page_size`: Set a target threshold for the approximate encoded size of data pages within a column chunk (in bytes).

Read the [pyarrow parquet docs](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html) to learn more about these settings.
