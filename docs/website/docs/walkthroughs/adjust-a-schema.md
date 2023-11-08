---
title: Adjust a schema
description: How to adjust a schema
keywords: [how to, adjust a schema]
---

# Adjust a schema

When you [create](create-a-pipeline.md) and then [run](run-a-pipeline.md) a pipeline, you may want
to manually inspect and change the [schema](../general-usage/schema.md) that `dlt` generated for
you. Here's how you do it.

## 1. Export your schemas on each run

Set up an export folder by providing the `export_schema_path` argument to `dlt.pipeline` to save the
schema. Set up an import folder from which `dlt` will read your modifications by providing
`import_schema_path` argument.

Following our example in [run a pipeline](run-a-pipeline.md):

```python
dlt.pipeline(
    import_schema_path="schemas/import",
    export_schema_path="schemas/export",
    pipeline_name="chess_pipeline",
    destination='duckdb',
    dataset_name="games_data"
)
```

The following folder structure in the project root folder will be created:

```
schemas
    |---import/
    |---export/
```

Instead of modifying the code, you can put those settings in `config.toml`:

```toml
export_schema_path="schemas/export"
import_schema_path="schemas/import"
```

## 2. Run the pipeline to see the schemas

To see the schemas, you must run your pipeline again. The `schemas` and `import`/`export`
directories will be created. In each directory, you'll see a `yaml` file (e.g. `chess.schema.yaml`).

Look at the export schema (in the export folder): this is the schema that got inferred from the data
and was used to load it into the destination (e.g. `duckdb`).

## 3. Make changes in import schema

Now look at the import schema (in the import folder): it contains only the tables, columns, and
hints that were explicitly declared in the `chess` source. You'll use this schema to make
modifications, typically by pasting relevant snippets from your export schema and modifying them.
You should keep the import schema as simple as possible and let `dlt` do the rest.

💡 How importing a schema works:

1. When a new pipeline is created and the source function is extracted for the first time, a new
   schema is added to the pipeline. This schema is created out of global hints and resource hints
   present in the source extractor function.
1. Every such new schema will be saved to the `import` folder (if it does not exist there already)
   and used as the initial version for all future pipeline runs.
1. Once a schema is present in `import` folder, **it is writable by the user only**.
1. Any changes to the schemas in that folder are detected and propagated to the pipeline
   automatically on the next run. It means that after a user update, the schema in `import`
   folder reverts all the automatic updates from the data.

In next steps we'll experiment a lot, you will be warned to set `full_refresh=True` until we are done experimenting.

:::caution
`dlt` will **not modify** tables after they are created.
So if you have a `yaml` file, and you change it (e.g. change a data type or add a hint),
then you need to **delete the dataset**
or set `full_refresh=True`:
```python
dlt.pipeline(
    import_schema_path="schemas/import",
    export_schema_path="schemas/export",
    pipeline_name="chess_pipeline",
    destination='duckdb',
    dataset_name="games_data",
    full_refresh=True,
)
```
:::

### Change the data type

In export schema we see that `end_time` column in `players_games` has a `text` data type while we
know that there is a timestamp. Let's change it and see if it works.

Copy the column:

```yaml
end_time:
  nullable: true
  data_type: text
```

from export to import schema and change the data type to get:

```yaml
players_games:
  columns:
    end_time:
      nullable: true
      data_type: timestamp
```

Run the pipeline script again and make sure that the change is visible in the export schema. Then,
[launch the Streamlit app](../dlt-ecosystem/visualizations/exploring-the-data.md) to see the changed data.

:::note
Do not rename the tables or columns in the yaml file. `dlt` infers those from the data so the schema will be recreated.
You can [adjust the schema](../general-usage/resource.md#adjust-schema) in Python before resource is loaded.
:::


### Load data as json instead of generating child table or columns from flattened dicts

In the export schema, you can see that white and black players properties got flattened into:

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

For some reason, you'd rather deal with a single JSON (or struct) column. Just declare the `white`
column as `complex`, which will instruct `dlt` not to flatten it (or not convert into child table in
case of a list). Do the same with `black` column:

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

Run the pipeline script again, and now you can query `black` and `white` columns with JSON
expressions.

### Add performance hints

Let's say you are done with local experimentation and want to load your data to `BigQuery` instead
of `duckdb`. You'd like to partition your data to save on query costs. The `end_time` column we just
fixed looks like a good candidate.

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

## 4. Keep your import schema

Just add and push the import folder to git. It will be used automatically when cloned. Alternatively,
[bundle such schema with your source](../general-usage/schema.md#attaching-schemas-to-sources).
