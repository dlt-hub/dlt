## General approach to define schemas
marks features that are:

⛔ not implemented, hard to add

☮️ not implemented, easy to add

## Schema components

### Schema content hash and version
Each schema file contains content based hash `version_hash` that is used to
1. detect manual changes to schema (ie. user edits content)
2. detect if the destination database schema is synchronized with the file schema

Each time the schema is saved, the version hash is updated.

Each schema contains also numeric version which increases automatically whenever schema is updated and saved. This version is mostly for informative purposes and there are cases where the increasing order will be lost.

> Schema in the database is only updated if its hash is not stored in `_dlt_versions` table. In principle many pipelines may send data to a single dataset. If table name clash then a single table with the union of the columns will be created. If columns clash and they have different types etc. then the load will fail.

### ❗ Normalizer and naming convention

The parent table is created from all top level fields, if field are dictionaries they will be flattened. **all the key names will be converted with the configured naming convention**. The current naming convention
1. converts to snake_case, small caps. removes all ascii characters except alphanum and underscore
2. add `_` if name starts with number
3. multiples of `_` are converted into single `_`
4. the parent-child relation is expressed as double `_` in names.

The nested lists will be converted into child tables.

The data normalizer and the naming convention are part of the schema configuration. In principle the source can set own naming convention or json unpacking mechanism. Or user can overwrite those in `config.toml`

> The table and column names are mapped automatically. **you cannot rename the columns or tables by changing the `name` property - you must rename your source documents**

> if you provide any schema elements that contain identifiers via decorators or arguments (ie. `table_name` or `columns`) all the names used will be converted via the naming convention when adding to the schema. For example if you execute `dlt.run(... table_name="CamelCase")` the data will be loaded into `camel_case`

> 💡 use simple, short small caps identifiers for everything!

☠️ not implemented!

⛔ The schema holds lineage information (from json paths to tables/columns) and (1) automatically adapts to destination limits ie. postgres 64 chars by recomputing all names (2) let's user to change the naming convention ie. to verbatim naming convention of `duckdb` where everything is allowed as identifier.

⛔ Any naming convention generates name clashes. `dlt` detects and fixes name clashes using lineage information


#### JSON normalizer settings
Yes those are part of the normalizer module and can be plugged in.
1. column propagation from parent to child tables
2. nesting level

```yaml
normalizers:
  names: dlt.common.normalizers.names.snake_case
  json:
    module: dlt.common.normalizers.json.relational
    config:
      max_nesting: 5
      propagation:
        # for all root tables
        root:
          # propagate root dlt id
          _dlt_id: _dlt_root_id
        tables:
          # for particular tables
          blocks:
            # propagate timestamp as block_timestamp to child tables
            timestamp: block_timestamp
            hash: block_hash
```

## Data types
"text", "double", "bool", "timestamp", "bigint", "binary", "complex", "decimal", "wei"
⛔ you cannot specify scale and precision for bigint, binary, text and decimal

☮️ there's no time and date type

wei is a datatype that tries to best represent native Ethereum 256bit integers and fixed point decimals. it works correcly on postgres and bigquery

## Schema settings
The `settings` section of schema let's you define various global rules that impact how tables and columns are inferred from data.

> 💡 it is the best practice to use those instead of providing the exact column schemas via `columns` argument or by pasting them in `yaml`. Any ideas for improvements? tell me.

### Column hint rules
You can define a global rules that will apply hints to a newly inferred columns. Those rules apply to normalized column names. You can use column names directly or with regular expressions. ❗ when lineages are implemented the regular expressions will apply to lineages not to column names.

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

### data type autodetectors
You can define a set of functions that will be used to infer the data type of the column from a value. The functions are run from top to bottom on the lists. Look in `detections.py` to see what is available.
```yaml
settings:
    detections:
    - timestamp
    - iso_timestamp
    - iso_date
```

⛔ we may define `all_text` function that will generate string only schemas by telling `dlt` that all types should be coerced to strings.

### Table exclude and include filters
You can define the include and exclude filters on tables but you are much better off transforming and filtering your source data in python. The current implementation is both weird and quite powerful. In essence you can exclude columns and whole tables with regular expressions to which the inputs are normalized lineages of the values.
Example
```yaml
event_user:
    columns: {}
    write_disposition: append
    filters:
      excludes:
      - re:^parse_data
      includes:
      - re:^parse_data__(intent|entities|message_id$|text$)
```

This will exclude all the child tables and columns of `event_user` table that start with `parse_data` but will include child tables containing `intent` and `entities` in their names and all tables with column names that end with `message_id` and `text`.

⛔ Once the lineages are implemented the exclude and include filters will work with them. now it is better not to use them.

## Working with schema files
`dlt` automates working with schema files by setting up schema import and export folders. Settings are available via config providers (ie. `config.toml`) or via `dlt.pipeline(import_schema_path, export_schema_path)` settings. Example:
```python
dlt.pipeline(import_schema_path="schemas/import", export_schema_path="schemas/export")
```
will create following folder structure in project root folder
```
schemas
    |---import/
    |---export/
```

Which will expose pipeline schemas to the user in `yml` format.

1. When new pipeline is created and source function is extracted for the first time a new schema is added to pipeline. This schema is created out of global hints and resource hints present in the source extractor function. It **does not depend on the data - which happens in normalize stage**.
2. Every such new schema will be saved to `import` folder (if not existing there already) and used as initial version for all future pipeline runs.
3. Once schema is present in `import` folder, **it is writable by the user only**.
4. Any change to the schemas in that folder are detected and propagated to the pipeline automatically on the next run (in fact any call to `Pipeline` object does that sync.). It means that after an user update, the schema in `import` folder resets all the automatic updates from the data.
4. Otherwise **the schema evolves automatically in the normalize stage** and each update is saved in `export` folder. The export folder is **writable by dlt only** and provides the actual view of the schema.
5. The `export` and `import` folders may be the same. In that case the evolved schema is automatically "accepted" as the initial one.


## Working with schema in code
`dlt` user can "check-out" any pipeline schema for modification in the code.

> ⛔ I do not have any cool API to work with the table, columns and other hints in the code - the schema is a typed dictionary and currently it is the only way.

`dlt` will "commit" all the schema changes with any call to `run`, `extract`, `normalize` or `load` methods.

Examples:

```python
# extract some to "table" resource using default schema
p = dlt.pipeline(destination=redshift)
p.extract([1,2,3,4], name="table")
# get live schema
schema = p.default_schema
# we want the list data to be text, not integer
schema.tables["table"]["columns"]["value"] = schema_utils.new_column("value", "text")
# `run` will apply schema changes and run the normalizer and loader for already extracted data
p.run()
```

> The `normalize` stage creates standalone load packages each containing data and schema with particular version. Those packages are of course not impacted by the "live" schema changes.

## Attaching schemas to sources
The general approach when creating a new pipeline is to setup a few global schema settings and then let the table and column schemas to be generated from the resource hints and data itself.

> ⛔ I do not have any cool "schema builder" api yet to see the global settings.

The `dlt.source` decorator accepts a schema instance that you can create yourself and whatever you want. It also support a few typical use cases:

### Schema created implicitly by decorator
If no schema instance is passed, the decorator creates a schema with the name set to source name and all the settings to default.

### Automatically load schema file stored with source python module
If no schema instance is passed, and a file with a name `{source name}_schema.yml` exists in the same folder as the module with the decorated function, it will be automatically loaded and used as the schema.

This should make easier to bundle a fully specified (or non trivially configured) schema with a source.

### Schema is modified in the source function body
What if you can configure your schema or add some tables only inside your schema function, when ie. you have the source credentials and user settings? You could for example add detailed schemas of all the database tables when someone requests a table data to be loaded. This information is available only at the moment source function is called.

Similarly to the `state`, source and resource function has current schema available via `dlt.current.source_schema`

Example:

```python

# apply schema to the source
@dlt.source
def createx(nesting_level: int):

    schema = dlt.current.source_schema()

    # get default normalizer config
    normalizer_conf = dlt.schema.normalizer_config()
    # set hash names convention which produces short names without clashes but very ugly
    if short_names_convention:
        normalizer_conf["names"] = dlt.common.normalizers.names.hash_names

    # apply normalizer conf
    schema = Schema("createx", normalizer_conf)
    # set nesting level, yeah it's ugly
    schema._normalizers_config["json"].setdefault("config", {})["max_nesting"] = nesting_level
    # remove date detector and add type detector that forces all fields to strings
    schema._settings["detections"].remove("iso_timestamp")
    schema._settings["detections"].insert(0, "all_text")
    schema.compile_settings()

    return dlt.resource(...)

```

Also look at the following [test](/tests/extract/test_decorators.py) : `test_source_schema_context`
