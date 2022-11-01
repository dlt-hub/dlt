## General approach to define schemas

## Schema components

### Schema content hash and version
Each schema file contains content based hash `version_hash` that is used to
1. detect manual changes to schema (ie. user edits content)
2. detect if the destination database schema is synchronized with the file schema

Each time the schema is saved, the version hash is updated.

Each schema contains also numeric version which increases automatically whenever schema is updated and saved. This version is mostly for informative purposes and currently the user can easily reset it by wiping out the pipeline working dir (until we restore the current schema from the destination)

> Currently the destination schema sync procedure uses the numeric version. I'm changing it to hash based versioning.

### Normalizer and naming convention
The data normalizer and the naming convention are part of the schema configuration. In principle the source can set own naming convention or json unpacking mechanism. Or user can overwrite those in `config.toml`

#### Relational normalizer config
Yes those are part of the normalizer module and can be plugged in.
1. column propagation from parent -> child
2. nesting level
3. parent -> child table linking type
### Global hints, preferred data type hints, data type autodetectors

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

> I do not have any cool API to work with the table, columns and other hints in the code - the schema is a typed dictionary and currently it is the only way.

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

> I do not have any cool "schema builder" api yet to se the global settings.

Example:

```python

schema: Schema = None

def setup_schema(nesting_level, hash_names_convention=False):
    nonlocal schema

    # get default normalizer config
    normalizer_conf = dlt.schema.normalizer_config()
    # set hash names convention which produces short names without clashes but very ugly
    if short_names_convention:
        normalizer_conf["names"] = dlt.common.normalizers.names.hash_names
    # remove date detector and add type detector that forces all fields to strings
    normalizer_conf["detections"].remove("iso_timestamp")
    normalizer_conf["detections"].insert(0, "all_text")

    # apply normalizer conf
    schema = Schema("createx", normalizer_conf)
    # set nesting level, yeah it's ugly
    schema._normalizers_config["json"].setdefault("config", {})["max_nesting"] = nesting_level

# apply schema to the source
@dlt.source(schema=schema)
def createx():
    ...

```

Two other behaviors are supported
1. bare `dlt.source` will create empty schema with the source name
2. `dlt.source(name=...)` will first try to load `{name}_schema.yml` from the same folder the source python file exist. If not found, new empty schema will be created


## Open issues

1. Name clashes.
2. Lack of lineage.
3. Names, types and hints interpretation depend on destination
