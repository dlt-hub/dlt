---
title: CSV
description: The CSV file format
keywords: [csv, file formats]
---
import SetTheFormat from './_set_the_format.mdx';

# CSV file format

**CSV** is the most basic file format for storing tabular data, where all values are strings and are separated by a delimiter (typically a comma).
`dlt` uses it for specific use cases - mostly for performance and compatibility reasons.

Internally, we use two implementations:
- [Python standard library CSV writer](https://docs.python.org/3/library/csv.html)
- PyArrow CSV writer - a very fast, multithreaded writer for [Arrow tables](../verified-sources/arrow-pandas.md)

## Supported destinations

- [Postgres](../destinations/postgres.md)
- [Filesystem](../destinations/filesystem.md)
- [Snowflake](../destinations/snowflake.md)

## How to configure

<SetTheFormat file_type="csv"/>

## Default settings
`dlt` attempts to make both writers generate similarly looking files:
* separators are commas
* quotes are **"** and are escaped as **""**
* `NULL` values are both empty strings and empty tokens as in the example below
* UNIX new lines (`"\n"`) are used by default
* dates are represented as ISO 8601
* quoting style is "when needed"

Example of NULLs:
```sh
text1,text2,text3
A,B,C
A,,""
```

In the last row, both `text2` and `text3` values are NULL. The Python `csv` writer
is not able to write unquoted `None` values, so we had to settle for `""`.

Note: all destinations capable of writing CSVs must support it.

### Change settings
You can change basic `csv` settings; this may be handy when working with the `filesystem` destination. Other destinations are tested
with standard settings:

* `delimiter`: change the delimiting character (default: ',')
* `include_header`: include the header row (default: True)
* `lineterminator`: specify the string used to terminate lines (default: `\n` - UNIX line endings, use `\r\n` for Windows line endings)
* `quoting`: controls when quotes should be generated around field values. Available options:

    - `quote_needed` (default): quote only values that need quoting, i.e., non-numeric values
      - Python CSV writer: All non-numeric values are quoted
      - PyArrow CSV writer: The exact behavior is not fully documented. We observed that in some cases, strings are not quoted as well

    - `quote_all`: all values are quoted
      - Supported by both Python CSV writer and PyArrow CSV writer

    - `quote_minimal`: quote only fields containing special characters (delimiter, quote character, or line terminator)
      - Supported by Python CSV writer only

    - `quote_none`: never quote fields
        - Python CSV writer: Uses escape character when delimiter appears in data
        - PyArrow CSV writer: Raises an error if data contains special characters

```toml
[normalize.data_writer]
delimiter="|"
include_header=false
quoting="quote_all"
lineterminator="\r\n"
```

Or using environment variables:

```sh
NORMALIZE__DATA_WRITER__DELIMITER=|
NORMALIZE__DATA_WRITER__INCLUDE_HEADER=False
NORMALIZE__DATA_WRITER__QUOTING=quote_all
NORMALIZE__DATA_WRITER__LINETERMINATOR=$"\r\n"
```

Note the `"$"` prefix before `"\r\n"` to escape the newline character when using environment variables.

### Destination settings
A few additional settings are available when copying `csv` to destination tables:
* **on_error_continue** - skip lines with errors (only Snowflake)
* **encoding** - encoding of the `csv` file

:::tip
You'll need these settings when [importing external files](../../general-usage/resource.md#import-external-files).
:::

## Limitations
**arrow writer**

* binary columns are supported only if they contain valid UTF-8 characters
* json (nested, struct) types are not supported

**csv writer**
* binary columns are supported only if they contain valid UTF-8 characters (easy to add more encodings)
* json columns dumped with json.dumps
* **None** values are always quoted

