---
title: csv
description: The csv file format
keywords: [csv, file formats]
---

# CSV file format

**csv** is the most basic file format to store tabular data, where all the values are strings and are separated by a delimiter (typically comma).
`dlt` uses it for specific use cases - mostly for the performance and compatibility reasons.

Internally we use two implementations:
- **pyarrow** csv writer - very fast, multithreaded writer for the [arrow tables](../verified-sources/arrow-pandas.md)
- **python stdlib writer** - a csv writer included in the Python standard library for Python objects


## Supported Destinations

Supported by: **Postgres**, **Filesystem**, **snowflake**

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
* quoting style is "when needed"

### Change settings
You can change basic **csv** settings, this may be handy when working with **filesystem** destination. Other destinations are tested
with standard settings:

* delimiter: change the delimiting character (default: ',')
* include_header: include the header row (default: True)
* quoting: **quote_all** - all values are quoted, **quote_needed** - quote only values that need quoting (default: `quote_needed`)

When **quote_needed** is selected: in case of Python csv writer all non-numeric values are quoted. In case of pyarrow csv writer, the exact behavior is not described in the documentation. We observed that in some cases, strings are not quoted as well.


```toml
[normalize.data_writer]
delimiter="|"
include_header=false
quoting="quote_all"
```

Or using environment variables:

```sh
NORMALIZE__DATA_WRITER__DELIMITER=|
NORMALIZE__DATA_WRITER__INCLUDE_HEADER=False
NORMALIZE__DATA_WRITER__QUOTING=quote_all
```

### Destination settings
A few additional settings are available when copying `csv` to destination tables:
* **on_error_continue** - skip lines with errors (only Snowflake)
* **encoding** - encoding of the `csv` file

:::tip
You'll need those setting when [importing external files](../../general-usage/resource.md#import-external-files)
:::

## Limitations
**arrow writer**

* binary columns are supported only if they contain valid UTF-8 characters
* complex (nested, struct) types are not supported

**csv writer**
* binary columns are supported only if they contain valid UTF-8 characters (easy to add more encodings)
* complex columns dumped with json.dumps
* **None** values are always quoted