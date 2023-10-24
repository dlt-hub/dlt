---
title: Schema
description: Schema
keywords: [schema, dlt schema, yaml]
---

# Schema

The schema describes the structure of normalized data (e.g. tables, columns, data types, etc.) and
provides instructions on how the data should be processed and loaded. `dlt` generates schemas from
the data during the normalization process. User can affect this standard behavior by providing
**hints** that change how tables, columns and other metadata is generated and how the data is
loaded. Such hints can be passed in the code ie. to `dlt.resource` decorator or `pipeline.run`
method. Schemas can be also exported and imported as files, which can be directly modified.

> 💡 `dlt` associates a schema with a [source](source.md) and a table schema with a
> [resource](resource.md).

## Schema content hash and version

Each schema file contains content based hash `version_hash` that is used to:

1. Detect manual changes to schema (ie. user edits content).
1. Detect if the destination database schema is synchronized with the file schema.

Each time the schema is saved, the version hash is updated.

Each schema contains a numeric version which increases automatically whenever schema is updated and
saved. Numeric version is meant to be human-readable. There are cases (parallel processing) where
the order is lost.

> 💡 Schema in the destination is migrated if its hash is not stored in `_dlt_versions` table. In
> principle many pipelines may send data to a single dataset. If table name clash then a single
> table with the union of the columns will be created. If columns clash, and they have different
> types etc. then the load may fail if the data cannot be coerced.

## Naming convention

`dlt` creates tables, child tables and column schemas from the data. The data being loaded,
typically JSON documents, contains identifiers (i.e. key names in a dictionary) with any Unicode
characters, any lengths and naming styles. On the other hand the destinations accept very strict
namespaces for their identifiers. Like Redshift that accepts case-insensitive alphanumeric
identifiers with maximum 127 characters.

Each schema contains `naming convention` that tells `dlt` how to translate identifiers to the
namespace that the destination understands.

The default naming convention:

1. Converts identifiers to snake_case, small caps. Removes all ascii characters except ascii
   alphanumerics and underscores.
1. Adds `_` if name starts with number.
1. Multiples of `_` are converted into single `_`.
1. The parent-child relation is expressed as double `_` in names.
1. It shorts the identifier if it exceed the length at the destination.

> 💡 Standard behavior of `dlt` is to **use the same naming convention for all destinations** so
> users see always the same tables and columns in their databases.

> 💡 If you provide any schema elements that contain identifiers via decorators or arguments (i.e.
> `table_name` or `columns`) all the names used will be converted via the naming convention when
> adding to the schema. For example if you execute `dlt.run(... table_name="CamelCase")` the data
> will be loaded into `camel_case`.

> 💡 Use simple, short small caps identifiers for everything!

The naming convention is configurable and users can easily create their own
conventions that i.e. pass all the identifiers unchanged if the destination accepts that (i.e.
DuckDB).

## Data normalizer

Data normalizer changes the structure of the input data, so it can be loaded into destination. The
standard `dlt` normalizer creates a relational structure from Python dictionaries and lists.
Elements of that structure: table and column definitions, are added to the schema.

The data normalizer is configurable and users can plug their own normalizers i.e. to handle the
parent-child table linking differently or generate parquet-like data structs instead of child
tables.

## Tables and columns

The key components of a schema are tables and columns. You can find a dictionary of tables in
`tables` key or via `tables` property of Schema object.

A table schema has the following properties:

1. `name` and `description`.
1. `parent` with a parent table name.
1. `columns` with dictionary of table schemas.
1. `write_disposition` hint telling `dlt` how new data coming to the table is loaded.

Table schema is extended by data normalizer. Standard data normalizer adds propagated columns to it.

A column schema contains following properties:

1. `name` and `description` of a column in a table.
1. `data_type` with a column data type.
1. `precision` a precision for **text**, **timestamp**, **time**, **bigint**, **binary**, and **decimal** types
1. `scale` a scale for **decimal** type
1. `is_variant` telling that column was generated as variant of another column.

A column schema contains following basic hints:

1. `nullable` tells if column is nullable or not.
1. `primary_key` marks a column as a part of primary key.
1. `merge_key` marks a column as a part of merge key used by
   [incremental load](./incremental-loading.md#merge-incremental_loading).
1. `foreign_key` marks a column as a part of foreign key.
1. `root_key` marks a column as a part of root key which is a type of foreign key always referring to the
   root table.
1. `unique` tells that column is unique. on some destination that generates unique index.

`dlt` lets you define additional performance hints:

1. `partition` marks column to be used to partition data.
1. `cluster` marks column to be part to be used to cluster data
1. `sort` marks column as sortable/having order. on some destinations that non-unique generates
   index.

:::note
Each destination can interpret the hints in its own way. For example `cluster` hint is used by
Redshift to define table distribution and by BigQuery to specify cluster column. DuckDB and
Postgres ignore it when creating tables.
:::

### Variant columns

Variant columns are generated by a normalizer when it encounters data item with type that cannot be
coerced in existing column. Please see our [`coerce_row`](https://github.com/dlt-hub/dlt/blob/7d9baf1b8fdf2813bcf7f1afe5bb3558993305ca/dlt/common/schema/schema.py#L205) if you are interested to see how internally it works.

Let's consider our [getting started](../getting-started#quick-start) example with slightly different approach,
where `id` is an integer type at the beginning

```py
data = [
  {"id": 1, "human_name": "Alice"}
]
```

once pipeline runs we will have the following schema:

| name          | data_type     | nullable |
| ------------- | ------------- | -------- |
| id            | bigint        | true     |
| human_name    | text          | true     |

Now imagine the data has changed and `id` field also contains strings

```py
data = [
  {"id": 1, "human_name": "Alice"}
  {"id": "idx-nr-456", "human_name": "Bob"}
]
```

So after you run the pipeline `dlt` will automatically infer type changes and will add a new field in the schema `id__v_text`
to reflect that new data type for `id` so for any type which is not compatible with integer it will create a new field.

| name          | data_type     | nullable |
| ------------- | ------------- | -------- |
| id            | bigint        | true     |
| human_name    | text          | true     |
| id__v_text    | text          | true     |

On the other hand if `id` field was already a string then introducing new data with `id` containing other types
will not change schema because they can be coerced to string.

Now go ahead and try to add a new record where `id` is float number, you should see a new field `id__v_double` in the schema.

### Data types

| dlt Data Type | Source Value Example                                | Precision and Scale                                     |
| ------------- | --------------------------------------------------- | ------------------------------------------------------- |
| text          | `'hello world'`                                     | Supports precision, typically mapping to **VARCHAR(N)** |
| double        | `45.678`                                            |                                                         |
| bool          | `True`                                              |                                                         |
| timestamp     | `'2023-07-26T14:45:00Z'`, `datetime.datetime.now()` | Supports precision expressed as parts of a second       |
| date          | `datetime.date(2023, 7, 26)`                        |                                                         |
| time          | `'14:01:02'`, `datetime.time(14, 1, 2)`             | Supports precision - see **timestamp**                  |
| bigint        | `9876543210`                                        | Supports precision as number of bits                    |
| binary        | `b'\x00\x01\x02\x03'`                               | Supports precision, like **text**                       |
| complex       | `[4, 5, 6]`, `{'a': 1}`                             |                                                         |
| decimal       | `Decimal('4.56')`                                   | Supports precision and scale                            |
| wei           | `2**56`                                             |                                                         |

`wei` is a datatype tries to best represent native Ethereum 256bit integers and fixed point
decimals. It works correctly on Postgres and BigQuery. All the other destinations have insufficient
precision.

`complex` data type tells `dlt` to load that element as JSON or struct and do not attempt to flatten
or create a child table out of it.

`time` data type is saved in destination without timezone info, if timezone is included it is stripped. E.g. `'14:01:02+02:00` -> `'14:01:02'`.

:::tip
The precision and scale are interpreted by particular destination and are validated when a column is created. Destinations that
do not support precision for a given data type will ignore it.

The precision for **timestamp** is useful when creating **parquet** files. Use 3 - for milliseconds, 6 for microseconds, 9 for nanoseconds

The precision for **bigint** is mapped to available integer types ie. TINYINT, INT, BIGINT. The default is 64 bits (8 bytes) precision (BIGINT)
:::

## Schema settings

The `settings` section of schema file lets you define various global rules that impact how tables
and columns are inferred from data.

> 💡 It is the best practice to use those instead of providing the exact column schemas via `columns`
> argument or by pasting them in `yaml`.

### Data type autodetectors

You can define a set of functions that will be used to infer the data type of the column from a
value. The functions are run from top to bottom on the lists. Look in `detections.py` to see what is
available.

```yaml
settings:
  detections:
    - timestamp
    - iso_timestamp
    - iso_date
```

### Column hint rules

You can define a global rules that will apply hints of a newly inferred columns. Those rules apply
to normalized column names. You can use column names directly or with regular expressions.

Example from ethereum schema:

```yaml
settings:
  default_hints:
    foreign_key:
      - _dlt_parent_id
    not_null:
      - re:^_dlt_id$
      - _dlt_root_id
      - _dlt_parent_id
      - _dlt_list_idx
    unique:
      - _dlt_id
    cluster:
      - block_hash
    partition:
      - block_timestamp
```

### Preferred data types

You can define rules that will set the data type for newly created columns. Put the rules under
`preferred_types` key of `settings`. On the left side there's a rule on a column name, on the right
side is the data type.

> ❗See the column hint rules for naming convention!

Example:

```yaml
settings:
  preferred_types:
    timestamp: timestamp
    re:^inserted_at$: timestamp
    re:^created_at$: timestamp
    re:^updated_at$: timestamp
    re:^_dlt_list_idx$: bigint
```

## Export and import schema files

Please follow the guide on [how to adjust a schema](../walkthroughs/adjust-a-schema.md) to export and import `yaml`
schema files in your pipeline.

## Attaching schemas to sources

We recommend to not create schemas explicitly. Instead, user should provide a few global schema
settings and then let the table and column schemas to be generated from the resource hints and the
data itself.

The `dlt.source` decorator accepts a schema instance that you can create yourself and modify in
whatever way you wish. The decorator also support a few typical use cases:

### Schema created implicitly by decorator

If no schema instance is passed, the decorator creates a schema with the name set to source name and
all the settings to default.

### Automatically load schema file stored with source python module

If no schema instance is passed, and a file with a name `{source name}_schema.yml` exists in the
same folder as the module with the decorated function, it will be automatically loaded and used as
the schema.

This should make easier to bundle a fully specified (or pre-configured) schema with a source.

### Schema is modified in the source function body

What if you can configure your schema or add some tables only inside your schema function, when i.e.
you have the source credentials and user settings available? You could for example add detailed
schemas of all the database tables when someone requests a table data to be loaded. This information
is available only at the moment source function is called.

Similarly to the `source_state()` and `resource_state()` , source and resource function has current
schema available via `dlt.current.source_schema()`.

Example:

```python
@dlt.source
def textual(nesting_level: int):
    # get the source schema from the `current` context
    schema = dlt.current.source_schema()
    # remove date detector
    schema.remove_type_detection("iso_timestamp")
    # convert UNIX timestamp (float, withing a year from NOW) into timestamp
    schema.add_type_detection("timestamp")
    schema.compile_settings()

    return dlt.resource(...)
```
