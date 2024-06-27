
## Schema settings
The `settings` section of schema let's you define various global rules that impact how tables and columns are inferred from data.

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
