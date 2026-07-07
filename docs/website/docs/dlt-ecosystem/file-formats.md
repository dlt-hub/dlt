---
title: Loader file format
description: Loader file format determines how data is prepared and written to the destination by the pipeline
keywords: [loader file format, jsonl, parquet, csv, insert-values, insert]
---

# Loader file format

## Configure

To set the pipeline's loader file format, you can either:

1. Set the parameter at the resource level: `@dlt.resource(file_format=...)` ([learn more](../general-usage/resource.md#pick-loader-file-format-for-a-particular-resource))

2. Set the parameter at the pipeline level: `pipeline.run(..., loader_file_format=...)`

## Parquet

[Apache Parquet](https://en.wikipedia.org/wiki/Apache_Parquet) is a free and open-source column-oriented data storage format in the Apache Hadoop ecosystem. `dlt` is capable of storing data in this format when configured to do so.

To use this format, you need the `pyarrow` package. You can get this package as a `dlt` extra as well:

```sh
pip install "dlt[parquet]"
```

### Destination autoconfig
`dlt` uses [destination capabilities](../walkthroughs/create-new-destination.md#3-set-the-destination-capabilities) to configure the parquet writer:
* It uses decimal and wei precision to pick the right **decimal type** and sets precision and scale.
* It uses timestamp precision to pick the right **timestamp type** resolution (seconds, microseconds, or nanoseconds).
* It uses `supports_dictionary_encoding` to control whether constant columns (like `_dlt_load_id`) use dictionary-encoded Arrow arrays. Dictionary encoding is memory-efficient for repeated values but not supported by all destinations. Defaults to `true`.

### Writer settings

Under the hood, `dlt` uses the [pyarrow parquet writer](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html) to create the files. The following options can be used to change the behavior of the writer:

- `flavor`: Sanitize schema or set other compatibility options to work with various target systems. Defaults to None, which is the **pyarrow** default.
- `version`: Determine which Parquet logical types are available for use, whether the reduced set from the Parquet 1.x.x format or the expanded logical types added in later format versions. `dlt` defaults to "2.4".
- `compression`: Select the internal Parquet compression codec. Choose from `"snappy"`, `"gzip"`, `"brotli"`, `"zstd"`, `"lz4"`, or `"none"`. Defaults to `"snappy"`. Make sure that your database can decompress the codec you select here.
- `data_page_size`: Set a target threshold for the approximate encoded size of data pages within a column chunk (in bytes). Defaults to None, which is the **pyarrow** default.
- `row_group_size`: Set the number of rows in a row group. [See here](#row-group-size) how this can optimize parallel processing of queries on your destination over the default setting of `pyarrow`.
- `timestamp_timezone`: A string specifying the timezone, default is UTC.
- `coerce_timestamps`: resolution to which to coerce timestamps, choose from **s**, **ms**, **us**, **ns**
- `allow_truncated_timestamps` - will raise if precision is lost on truncated timestamps.
- `write_page_index`: Boolean specifying whether a [page index](https://github.com/apache/parquet-format/blob/master/PageIndex.md) is written. Defaults to `False`.
- `use_content_defined_chunking`: Boolean specifying whether [Content-Defined Chunking](https://github.com/apache/arrow/pull/45360) is used. Defaults to `False`. Requires `pyarrow>=21.0.0`, ignored otherwise.
- `arrow_concat_promote_options`: Controls type promotion when concatenating multiple Arrow tables/DataFrames. Accepts `"none"` (default), `"default"`, or `"permissive"`. See [Handling schema mismatches](./verified-sources/arrow-pandas.md#handling-schema-mismatches-across-batches) for details.

:::tip
The default parquet version used by `dlt` is 2.4. It coerces timestamps to microseconds and truncates nanoseconds silently. Such a setting
provides the best interoperability with database systems, including loading pandas DataFrames which have nanosecond resolution by default.
Set `version="2.6"` if you need to preserve nanosecond precision timestamps.
:::

Read the [pyarrow parquet docs](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html) to learn more about these settings.

Example:

```toml
[data_writer]
# example values
flavor="spark"
version="2.4"
compression="zstd"
data_page_size=1048576
timestamp_timezone="Europe/Berlin"
```

Or using environment variables:

```sh
DATA_WRITER__FLAVOR
DATA_WRITER__VERSION
DATA_WRITER__COMPRESSION
DATA_WRITER__DATA_PAGE_SIZE
DATA_WRITER__TIMESTAMP_TIMEZONE
DATA_WRITER__ARROW_CONCAT_PROMOTE_OPTIONS
```

:::tip
You can apply data writer settings to parquet created in normalize stage only:
`NORMALIZE__DATA_WRITER__FLAVOR=spark`

To set the Parquet codec for normalize-stage files, use `NORMALIZE__DATA_WRITER__COMPRESSION=zstd`. This setting applies to Parquet's internal codec only. It does not change `jsonl`, `csv`, or other text file compression; use `data_writer.disable_compression` to disable their gzip wrapper.

When your source/resource yields arrow tables / pandas DataFrames / polars DataFrames, you can control settings per source:
`SOURCES__<SOURCE_MODULE>__<SOURCE_NAME>__DATA_WRITER__FLAVOR=spark`

Find more similar examples [here](../reference/performance.md#extract)
:::



### Timestamps and timezones
`dlt` adds timezone (UTC adjustment) to all timestamps regardless of the precision (from seconds to nanoseconds). `dlt` will also create TZ-aware timestamp columns in
the destinations. [DuckDB is an exception here](./destinations/duckdb.md#supported-file-formats).

#### Disable timezones / UTC adjustment flags
You can generate parquet files without timezone adjustment information in two ways:
1. Set the **flavor** to spark. All timestamps will be generated via the deprecated `int96` physical data type, without the logical one.
2. Set the **timestamp_timezone** to an empty string (i.e., `DATA_WRITER__TIMESTAMP_TIMEZONE=""`) to generate a logical type without UTC adjustment.

To our best knowledge, Arrow will convert your timezone-aware DateTime(s) to UTC and store them in parquet without timezone information.


### Row group size

The `pyarrow` parquet writer writes each item, i.e., table or record batch, in a separate row group. This may lead to many small row groups, which may not be optimal for certain query engines. For example, `duckdb` parallelizes on a row group. `dlt` allows controlling the size of the row group by [buffering and concatenating tables](../reference/performance.md#controlling-in-memory-buffers) and batches before they are written. The concatenation is done as a zero-copy to save memory. You can control the size of the row group by setting the maximum number of rows kept in the buffer.

```toml
[data_writer]
buffer_max_items=10e6
```

Keep in mind that `dlt` holds the tables in memory. Thus, 10,000,000 rows in the example above may consume a significant amount of RAM.

The `row_group_size` configuration setting has limited utility with the `pyarrow` writer. It may be useful when you write single very large pyarrow tables or when your in-memory buffer is really large.


## CSV

**CSV** is the most basic file format for storing tabular data, where all values are strings and are separated by a delimiter (typically a comma).
`dlt` uses it for specific use cases - mostly for performance and compatibility reasons.

Internally, we use two implementations, picked based on the shape of the data items:
- [Python standard library CSV writer](https://docs.python.org/3/library/csv.html) - used when resources yield Python objects (dicts)
- PyArrow CSV writer - a very fast, multithreaded writer, used when resources yield [Arrow tables, pandas DataFrames, or polars DataFrames](./verified-sources/arrow-pandas.md)

### Settings
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

Note: all destinations that support the `csv` format accept files written with the standard settings above.

#### Write settings
The settings below control how `dlt` writes `csv` files during **normalize** and are configured in the `[normalize.data_writer]` section. Changing them may be handy when working with the `filesystem` destination. Other destinations are tested
with standard settings:

* `delimiter`: change the delimiting character (default: ',')
* `include_header`: include the header row (default: True)
* `lineterminator`: specify the string used to terminate lines (default: `\n` - UNIX line endings, use `\r\n` for Windows line endings). Applies to the Python CSV writer only; the PyArrow writer always uses `\n`
* `encoding`: encoding used to write `csv` files (default: `utf-8`). Use, e.g., `utf-8-sig` to add a BOM for older Excel or `latin-1`/`cp1252` for legacy importers. Both writers honor it
* `encoding_errors`: how characters that cannot be represented in `encoding` are treated (default: `strict` - the load fails). Use a [Python error handler name](https://docs.python.org/3/library/codecs.html#error-handlers), e.g., `replace` to substitute them with `?` or `backslashreplace` to keep them as escape sequences
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
encoding="latin-1"
```

Or using environment variables:

```sh
NORMALIZE__DATA_WRITER__DELIMITER=|
NORMALIZE__DATA_WRITER__INCLUDE_HEADER=False
NORMALIZE__DATA_WRITER__QUOTING=quote_all
NORMALIZE__DATA_WRITER__LINETERMINATOR=$"\r\n"
NORMALIZE__DATA_WRITER__ENCODING=latin-1
```

Note the `"$"` prefix before `"\r\n"` to escape the newline character when using environment variables.

#### Read settings
Destinations that copy `csv` files into tables (**postgres** and **snowflake**) read them according to their own `csv_format` configuration. These settings do not change how `dlt` writes files - they describe the file the destination is loading and are set on the destination, e.g.:

```toml
[destination.postgres.csv_format]
delimiter="|"
encoding="latin-1"
```

When reading, `encoding` tells the destination how to decode the `csv` file (default: `utf-8`) and one option is used only when reading:
* `on_error_continue`: skip lines with errors (only Snowflake)

`csv_format` also accepts the write settings above - set them when the file being loaded deviates from the defaults, e.g., uses a different delimiter or has no header row.

:::caution
Write settings (`[normalize.data_writer]`) and destination read settings (`[destination.<name>.csv_format]`) are resolved independently - which one to set depends on who writes and who reads the files:

* **dlt does both** - in the standard flow (e.g. loading into **postgres** or **snowflake**), `dlt` writes the files and the destination copies them right back. Keep the defaults on both sides: the files exist only as an internal transport format and there is no reason to change how it is encoded.
* **External systems read the files** - with the `filesystem` destination as the final target, the files are the product. Set the `[normalize.data_writer]` options to whatever the consumer expects, e.g. `encoding="utf-8-sig"` for older Excel or `cp1252` for a legacy importer.
* **dlt did not write the files** - when [importing external files](../general-usage/resource.md#import-external-files), describe them in `[destination.<name>.csv_format]` so the destination can decode them. Mind that `encoding` goes verbatim into the destination's COPY statement, so it must be an encoding name the destination accepts - and those names do not always match Python's (e.g. `latin-1` vs `ISO_8859_1`).

If you nevertheless combine a custom write encoding with a database destination, mirror the value in the destination `csv_format` - otherwise the destination decodes the files as `utf-8` and the load fails or garbles non-ASCII characters.
:::

### Limitations
**arrow writer**

* binary columns are supported only if they contain valid UTF-8 characters
* json (nested, struct) types are not supported

**csv writer**
* binary columns are supported only if they contain valid UTF-8 characters (easy to add more encodings)
* json columns dumped with json.dumps
* **None** values are always quoted

## JSONL

JSONL (or JSON Lines, JSON Delimited) is a file format that stores several JSON documents in one file. The JSON documents are separated by a new line.

Additional data types are stored as follows:

- `datetime` and `date` are stored as ISO strings;
- `decimal` is stored as a text representation of a decimal number;
- `binary` is stored as a base64 encoded string;
- `HexBytes` is stored as a hex encoded string;
- `json` is serialized as a string.

This file format is [compressed](../reference/performance.md#disabling-and-enabling-file-compression) by default.

## SQL INSERT

This file format contains an INSERT...VALUES statement to be executed on the destination during the `load` stage.

Additional data types are stored as follows:

- `datetime` and `date` are stored as ISO strings;
- `decimal` is stored as a text representation of a decimal number;
- `binary` storage depends on the format accepted by the destination;
- `json` storage also depends on the format accepted by the destination.

This file format is [compressed](../reference/performance.md#disabling-and-enabling-file-compression) by default.
