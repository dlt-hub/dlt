# Schema

Schema describes the structure of normalized data (e.g. tables, columns, data types, etc.) and provides instructions on how the data should be processed and loaded. `dlt` generates schemas from the data during the normalization process. User can affect this standard behavior by providing **hints** that change how tables, columns and other metadata is generated and how the data is loaded. Such hints can be passed in the code ie. to `dlt.resource` decorator or `pipeline.run` method. Schemas can be also exported and imported as files, which can be directly modified.

> 💡 `dlt` associates a schema with a [source](source.md) and a table schema with a [resource](resource.md)

## Schema content hash and version
Each schema file contains content based hash `version_hash` that is used to
1. detect manual changes to schema (ie. user edits content)
2. detect if the destination database schema is synchronized with the file schema

Each time the schema is saved, the version hash is updated.

Each schema contains a numeric version which increases automatically whenever schema is updated and saved. Numeric version is meant to be human readable. There are cases (parallel processing) where the order is lost.

> 💡 Schema in the destination is migrated if its hash is not stored in `_dlt_versions` table. In principle many pipelines may send data to a single dataset. If table name clash then a single table with the union of the columns will be created. If columns clash and they have different types etc. then the load may fail if the data cannot be coerced.

## Naming convention
`dlt` creates tables, child tables and column schemas from the data. The data being loaded, typically JSON documents, contains identifiers (ie. key names in a dictionary) with any Unicode characters, any lengths and naming styles. On the other hand the destinations accept very strict namespaces for their identifiers. Like Redshift that accepts case insensitive alphanumeric identifiers with maximum 127 characters.

Each schema contains `naming convention` that tells `dlt` how to translate identifiers to the namespace that the destination understands.

The default naming convention:
1. Converts identifiers to snake_case, small caps. Removes all ascii characters except ascii alphanumerics and underscores
2. Adds `_` if name starts with number
3. Multiples of `_` are converted into single `_`
4. The parent-child relation is expressed as double `_` in names.
5. It shorts the identifier if it exceed the length at the destination

> 💡 Standard behavior of `dlt` is to **use the same naming convention for all destinations** so users see always the same tables and columns in their databases.

> 💡 If you provide any schema elements that contain identifiers via decorators or arguments (ie. `table_name` or `columns`) all the names used will be converted via the naming convention when adding to the schema. For example if you execute `dlt.run(... table_name="CamelCase")` the data will be loaded into `camel_case`

> 💡 Use simple, short small caps identifiers for everything!

The naming convention is [configurable](../customization/configuration.md) and users can easily create their own conventions that ie. pass all the identifiers unchanged if the destination accepts that (ie. duckdb).

## Data normalizer
Data normalizer changes the structure of the input data so it can be loaded into destination. The standard `dlt` normalizer creates a relational structure from Python dictionaries and lists. Elements of that structure: table and column definitions, are added to the schema.

The data normalizer is configurable and users can plug their own normalizers ie. to handle the parent-child table linking differently or generate parquet-like data structs instead of child tables.

## Tables and columns
The key components of a schema are tables and columns. You can find a dictionary of tables in `tables` key or via `tables` property of Schema object.

A table schema has following properties:
1. `name` and `description`
2. `parent` with a parent table name
3. `columns` with dictionary of table schemas
4. `write_disposition` hint telling `dlt` how new data coming to the table is loaded

Table schema is extended by data normalizer. Standard data normalizer adds propagated columns to it.

A column schema contains following properties:
1. `name` and `description` of a column in a table
2. `data_type` with a column data type
3. `is_variant` telling that column was generated as variant of another column

A column schema contains following basic hints
1. `nullable` tells if column is nullable or not
2. `primary_key` marks a column as a part of primary key
3. `merge_key` marks a column as a part of merge key used by [incremental load](../customization/incremental-loading.md#merge-incremental-loading)
4. `foreign_key` marks a column as a part of foreign key
5. `root_key` marks a column as a part of root key which is a type of foreign key referring to the root table.
6. `unique` tells that column is unique. on some destination that generates unique index

`dlt` let's you define additional performance hints.
1. `partition` marks column to be used to partition data
2. `sort` marks column as sortable/having order. on some destinations that non-unique generates index.

> 💡 Each destination can interpret the hints in its own way. For example `cluster` hint is used by Redshift to define table distribution and by BigQuery to specify cluster column. Duckdb and Postgres ignore it when creating tables.

### Variant columns
Variant columns are generated by a normalizer when it encounters data item with type that cannot be coerced in existing column.

### Data types
"text", "double", "bool", "timestamp", "bigint", "binary", "complex", "decimal", "wei", "date"


> ⛔ you cannot specify scale and precision for bigint, binary, text and decimal

`wei` is a datatype tries to best represent native Ethereum 256bit integers and fixed point decimals. it works correctly on postgres and bigquery. All the other destinations have insufficient precision

`complex` data type tells `dlt` to load that element as JSON or struct and do not attempt to flatten or create a child table out of it.

## Schema settings
The `settings` section of schema file let's you define various global rules that impact how tables and columns are inferred from data.

> 💡 it is the best practice to use those instead of providing the exact column schemas via `columns` argument or by pasting them in `yaml`.

### data type autodetectors
You can define a set of functions that will be used to infer the data type of the column from a value. The functions are run from top to bottom on the lists. Look in `detections.py` to see what is available.
```yaml
settings:
    detections:
    - timestamp
    - iso_timestamp
```

### Column hint rules
You can define a global rules that will apply hints of a newly inferred columns. Those rules apply to normalized column names. You can use column names directly or with regular expressions.

Example from ethereum schema
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
You can define rules that will set the data type for newly created columns. Put the rules under `preferred_types` key of `settings`. On the left side there's a rule on a column name, on the right side is the data type. ❗See the column hint rules for naming convention!

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
Please follow this [walkthrough](../walkthroughs/work-with-schemas-in-files.md) to export and import `yaml` schema files in your pipeline.

## Attaching schemas to sources
We recommend to not create schemas explicitly. Instead user should provide a few global schema settings and then let the table and column schemas to be generated from the resource hints and the data itself.

The `dlt.source` decorator accepts a schema instance that you can create yourself and modify in whatever way you wish. The decorator also support a few typical use cases:

### Schema created implicitly by decorator
If no schema instance is passed, the decorator creates a schema with the name set to source name and all the settings to default.

### Automatically load schema file stored with source python module
If no schema instance is passed, and a file with a name `{source name}_schema.yml` exists in the same folder as the module with the decorated function, it will be automatically loaded and used as the schema.

This should make easier to bundle a fully specified (or pre-configured) schema with a source.

### Schema is modified in the source function body
What if you can configure your schema or add some tables only inside your schema function, when ie. you have the source credentials and user settings available? You could for example add detailed schemas of all the database tables when someone requests a table data to be loaded. This information is available only at the moment source function is called.

Similarly to the `state`, source and resource function has current schema available via `dlt.current.source_schema`

Example:

```python

@dlt.source
def textual(nesting_level: int):
    # get the source schema from the `current` context
    schema = dlt.current.source_schema()
    # remove date detector and add type detector that forces all fields to strings
    schema._settings["detections"].remove("iso_timestamp")
    schema._settings["detections"].insert(0, "all_text")
    schema.compile_settings()

    return dlt.resource(...)

```