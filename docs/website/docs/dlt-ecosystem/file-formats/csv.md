---
title: csv
description: The csv file format
keywords: [csv, file formats]
---

# CSV File Format

**csv** is the most basic file format to store tabular data, where all the values are strings and are separated by a delimiter (typically comma).
`dlt` uses it for specific use cases - mostly for the performance and compatibility reasons.

Internally we use two implementations:
- **pyarrow** csv writer - very fast, multithreaded writer for the [arrow tables](../verified-sources/arrow-pandas.md)
- **python stdlib writer** - a csv writer included in the Python standard library for Python objects


## Supported Destinations

Supported by: **Postgres**, **Filesystem**

By setting the `loader_file_format` argument to `csv` in the run command, the pipeline will store your data in the csv format at the destination:

```py
info = pipeline.run(some_source(), loader_file_format="csv")
```

## Default Settings
`dlt` attempts to make both writers to generate similarly looking files
* separators are commas
* quotes are **"** and are escaped as **""**
* `NULL` values are empty strings
* UNIX new lines are used
* dates are represented as ISO 8601

## Limitations

* binary columns are supported only if they contain valid UTF-8 characters
* complex (nested, struct) types are not supported
