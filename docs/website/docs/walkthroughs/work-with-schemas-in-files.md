# Work with schema in files
When you [create](create-a-pipeline.md) a pipeline and then [run](run-a-pipeline.md) it you may want to inspect and change the [schema](../concepts/schema.md) that `dlt` generated for you. Here's how you do it

## 1. Export your schemas on each run
Setup an export folder to which `dlt` saves actual view of the schema by providing the `export_schema_path` argument to `dlt.pipeline`. Setup an import folder from which `dlt` will read your modifications by providing `import_schema_path` argument. Following our example in [run a pipeline](run-a-pipeline.md):
```python
dlt.pipeline(import_schema_path="schemas/import", export_schema_path="schemas/export", pipeline_name="chess_pipeline", destination='duckdb', dataset_name="games_data")
```
will create following folder structure in project root folder
```
schemas
    |---import/
    |---export/
```

Instead of modifying the code, you can put those settings in `config.toml`
```toml
export_schema_path="schemas/export"
import_schema_path="schemas/import"
```

## 2. Run the pipeline to see the schemas
To actually see the schemas, you must run your pipeline again. The `schemas` and `import`/`export` folders will be created. In each you'll see a `yaml` file with a file `chess.schema.toml`.

Look at the export schema (in export folder): this is the schema that got inferred from the data and was used to load it to `duckdb`.


## 3. Make changes in import schema
Now look at the import schema (in import folder): it contains only the tables, columns and hints that were explicitly declared in the `chess` source. You'll use this schema to make modifications, typically by pasting relevant snippets from export schema and modifying them. You should keep the import schema as simple as possible and let `dlt` do the rest.

> 💡 How importing schema works:
> 1. When new pipeline is created and source function is extracted for the first time a new schema is added to pipeline. This schema is created out of global hints and resource hints present in the source extractor function.
> 2. Every such new schema will be saved to `import` folder (if not existing there already) and used as initial version for all future pipeline runs.
> 3. Once schema is present in `import` folder, **it is writable by the user only**.
> 4. Any change to the schemas in that folder are detected and propagated to the pipeline automatically on the next run. It means that after an user update, the schema in `import` folder reverts all the automatic updates from the data.

In next steps we'll experiment a lot. **Set `full_refresh=True` to the `dlt.pipeline` until we are done**

### Change the data type
In export schema we see that `end_time` column in `players_games` has a `text` data type while we know that there is a timestamp. Let's change it and see if it works.
Copy the column:
```yaml
      end_time:
        nullable: true
        data_type: text
```
from export to import schema and change the data type to get
```yaml
  players_games:
    columns:
      end_time:
        nullable: true
        data_type: timestamp
```
Run the pipeline script again and make sure that the change is visible in export schema. Then launch the Streamlit app to see the changed data.

### Load data as json instead of generating child table or columns from flattened dicts
In the export schema you can see that white and black players properties got flattened into
```yaml
      white__rating:
        nullable: true
        data_type: bigint
      white__result:
        nullable: true
        data_type: text
      white__aid:
        nullable: true
        data_type: text
```
For some reason you'd rather deal with a single JSON (or struct) column. Just declare the `white` column as `complex` which will instruct `dlt` not to flatten it (or not convert into child table in case of a list). Do the same with `black` column.
```yaml
  players_games:
    columns:
      end_time:
        nullable: true
        data_type: timestamp
      white:
        nullable: false
        data_type: complex
      black:
        nullable: false
        data_type: complex
```

Run the pipeline script again and now you can query `black` and `white` columns with JSON expressions.

### Add performance hints
Let's say you are done with local experimentation and want to load your data to `BigQuery` instead of `duckdb`. You'd like to partition your data to save on query costs. The `end_time` column we just fixed looks like a good candidate.
```yaml
  players_games:
    columns:
      end_time:
        nullable: false
        data_type: timestamp
        partition: true
      white:
        nullable: false
        data_type: complex
      black:
        nullable: false
        data_type: complex
```

## 4. Keep your import schema.
Just add and push the import folder to git. It will be used automatically when cloned. Alternatively [bundle such schema with your source](../concepts/schema.md#attaching-schemas-to-sources)

