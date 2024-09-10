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

Each schema contains [naming convention](naming-convention.md) that tells `dlt` how to translate identifiers to the
namespace that the destination understands. This convention can be configured, changed in code or enforced via
destination.

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

To retain the original naming convention (like keeping `"createdAt"` as it is instead of converting it to `"created_at"`), you can use the direct naming convention, in "config.toml" as follows:
```toml
[schema]
naming="direct"
```
:::caution
Opting for `"direct"` naming bypasses most name normalization processes. This means any unusual characters present will be carried over unchanged to database tables and columns. Please be aware of this behavior to avoid potential issues.
:::

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
1. `timezone` a flag indicating TZ aware or NTZ **timestamp** and **time**. Default value is **true**
1. `nullable` tells if column is nullable or not.
1. `is_variant` telling that column was generated as variant of another column.

A column schema contains following basic hints:

1. `primary_key` marks a column as a part of primary key.
1. `row_key` a special for of primary key created by `dlt` to identify particular rows and link nested tables
1. `parent_key` a special form of foreign key used by nested tables to refer to parent tables
1. `root_key` marks a column as a part of root key which is a type of foreign key always referring to the
   root table.
1. `unique` tells that column is unique. on some destination that generates unique index.
1. `merge_key` marks a column as a part of merge key used by
   [incremental load](./incremental-loading.md#merge-incremental_loading).

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
  {"id": 1, "human_name": "Alice"},
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
| json          | `[4, 5, 6]`, `{'a': 1}`                             |                                                         |
| decimal       | `Decimal('4.56')`                                   | Supports precision and scale                            |
| wei           | `2**56`                                             |                                                         |

`wei` is a datatype tries to best represent native Ethereum 256bit integers and fixed point
decimals. It works correctly on Postgres and BigQuery. All the other destinations have insufficient
precision.

`json` data type tells `dlt` to load that element as JSON or string and do not attempt to flatten
or create a child table out of it. Note that structured types like arrays or maps are not supported by `dlt` at this point.

`time` data type is saved in destination without timezone info, if timezone is included it is stripped. E.g. `'14:01:02+02:00` -> `'14:01:02'`.

:::tip
The precision and scale are interpreted by particular destination and are validated when a column is created. Destinations that
do not support precision for a given data type will ignore it.

The precision for **timestamp** is useful when creating **parquet** files. Use 3 - for milliseconds, 6 for microseconds, 9 for nanoseconds

The precision for **bigint** is mapped to available integer types ie. TINYINT, INT, BIGINT. The default is 64 bits (8 bytes) precision (BIGINT)
:::

## Schema settings

The `settings` section of schema file lets you define various global rules that impact how tables
and columns are inferred from data. For example you can assign **primary_key** hint to all columns with name `id` or force **timestamp** data type on all columns containing `timestamp` with an use of regex pattern.

> 💡 It is the best practice to use those instead of providing the exact column schemas via `columns`
> argument or by pasting them in `yaml`.

### Data type autodetectors

You can define a set of functions that will be used to infer the data type of the column from a
value. The functions are run from top to bottom on the lists. Look in `detections.py` to see what is
available. **iso_timestamp** detector that looks for ISO 8601 strings and converts them to **timestamp**
is enabled by default.

```yaml
settings:
  detections:
    - timestamp
    - iso_timestamp
    - iso_date
    - large_integer
    - hexbytes_to_text
    - wei_to_double
```

Alternatively you can add and remove detections from code:
```py
  source = data_source()
  # remove iso time detector
  source.schema.remove_type_detection("iso_timestamp")
  # convert UNIX timestamp (float, withing a year from NOW) into timestamp
  source.schema.add_type_detection("timestamp")
```
Above we modify a schema that comes with a source to detect UNIX timestamps with **timestamp** detector.

### Column hint rules

You can define a global rules that will apply hints of a newly inferred columns. Those rules apply
to normalized column names. You can use column names directly or with regular expressions. `dlt` is matching
the column names **after they got normalized with naming convention**.

By default, schema adopts hints rules from json(relational) normalizer to support correct hinting
of columns added by normalizer:

```yaml
settings:
  default_hints:
    row_key:
      - _dlt_id
    parent_key:
      - _dlt_parent_id
    not_null:
      - _dlt_id
      - _dlt_root_id
      - _dlt_parent_id
      - _dlt_list_idx
      - _dlt_load_id
    unique:
      - _dlt_id
    root_key:
      - _dlt_root_id
```
Above we require exact column name match for a hint to apply. You can also use regular expression (which we call `SimpleRegex`) as follows:
```yaml
settings:
    partition:
      - re:_timestamp$
```
Above we add `partition` hint to all columns ending with `_timestamp`. You can do same thing in the code
```py
  source = data_source()
  # this will update existing hints with the hints passed
  source.schema.merge_hints({"partition": ["re:_timestamp$"]})
```

### Preferred data types

You can define rules that will set the data type for newly created columns. Put the rules under
`preferred_types` key of `settings`. On the left side there's a rule on a column name, on the right
side is the data type. You can use column names directly or with regular expressions.
`dlt` is matching the column names **after they got normalized with naming convention**.

Example:

```yaml
settings:
  preferred_types:
    re:timestamp: timestamp
    inserted_at: timestamp
    created_at: timestamp
    updated_at: timestamp
```

Above we prefer `timestamp` data type for all columns containing **timestamp** substring and define a few exact matches ie. **created_at**.
Here's same thing in code
```py
  source = data_source()
  source.schema.update_preferred_types(
    {
      "re:timestamp": "timestamp",
      "inserted_at": "timestamp",
      "created_at": "timestamp",
      "updated_at": "timestamp",
    }
  )
```
### Applying data types directly with `@dlt.resource` and `apply_hints`
`dlt` offers the flexibility to directly apply data types and hints in your code, bypassing the need for importing and adjusting schemas. This approach is ideal for rapid prototyping and handling data sources with dynamic schema requirements.

### Direct specification in `@dlt.resource`
Directly define data types and their properties, such as nullability, within the `@dlt.resource` decorator. This eliminates the dependency on external schema files. For example:

```py
@dlt.resource(name='my_table', columns={"my_column": {"data_type": "bool", "nullable": True}})
def my_resource():
    for i in range(10):
        yield {'my_column': i % 2 == 0}
```
This code snippet sets up a nullable boolean column named `my_column` directly in the decorator.

#### Using `apply_hints`
When dealing with dynamically generated resources or needing to programmatically set hints, `apply_hints` is your tool. It's especially useful for applying hints across various collections or tables at once.

For example, to apply a `json` data type across all collections from a MongoDB source:

```py
all_collections = ["collection1", "collection2", "collection3"]  # replace with your actual collection names
source_data = mongodb().with_resources(*all_collections)

for col in all_collections:
    source_data.resources[col].apply_hints(columns={"column_name": {"data_type": "json"}})

pipeline = dlt.pipeline(
    pipeline_name="mongodb_pipeline",
    destination="duckdb",
    dataset_name="mongodb_data"
)
load_info = pipeline.run(source_data)
```
This example iterates through MongoDB collections, applying the **json** [data type](schema#data-types) to a specified column, and then processes the data with `pipeline.run`.

## View and print the schema
To view and print the default schema in a clear YAML format use the command:

```py
pipeline.default_schema.to_pretty_yaml()
```
This can be used in a pipeline as:

```py
# Create a pipeline
pipeline = dlt.pipeline(
               pipeline_name="chess_pipeline",
               destination='duckdb',
               dataset_name="games_data")

# Run the pipeline
load_info = pipeline.run(source)

# Print the default schema in a pretty YAML format
print(pipeline.default_schema.to_pretty_yaml())
```
This will display a structured YAML representation of your schema, showing details like tables, columns, data types, and metadata, including version, version_hash, and engine_version.

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

```py
@dlt.source
def textual(nesting_level: int):
    # get the source schema from the `current` context
    schema = dlt.current.source_schema()
    # remove date detector
    schema.remove_type_detection("iso_timestamp")
    # convert UNIX timestamp (float, withing a year from NOW) into timestamp
    schema.add_type_detection("timestamp")

    return dlt.resource([])
```
