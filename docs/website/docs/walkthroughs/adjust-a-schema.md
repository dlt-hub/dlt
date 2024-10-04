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
the `import_schema_path` argument.

Following our example in [run a pipeline](run-a-pipeline.md):

```py
dlt.pipeline(
    import_schema_path="schemas/import",
    export_schema_path="schemas/export",
    pipeline_name="chess_pipeline",
    destination='duckdb',
    dataset_name="games_data"
)
```

The following folder structure in the project root folder will be created:

```text
schemas
    |---import/
    |---export/
```

Rather than providing the paths in the `dlt.pipeline` function, you can also set them
in the `config.toml` file:

```toml
export_schema_path="schemas/export"
import_schema_path="schemas/import"
```

## 2. Run the pipeline to see the schemas

To see the schemas, you must run your pipeline again. The `schemas` and `import`/`export`
directories will be created. In each directory, you'll see a `yaml` file (e.g., `chess.schema.yaml`).

Look at the export schema (in the export folder): this is the schema that got inferred from the data
and was used to load it into the destination (e.g., `duckdb`).

## 3. Make changes in import schema

Now look at the import schema (in the import folder): it contains only the tables, columns, and
hints that were explicitly declared in the `chess` source. You'll use this schema to make
modifications, typically by pasting relevant snippets from your export schema and modifying them.
You should keep the import schema as simple as possible and let `dlt` do the rest.

💡 How importing a schema works:

1. When a new pipeline is created and the source function is extracted for the first time, a new
   schema is added to the pipeline. This schema is created out of global hints and resource hints
   present in the source extractor function.
2. Every such new schema will be saved to the `import` folder (if it does not exist there already)
   and used as the initial version for all future pipeline runs.
3. Once a schema is present in the `import` folder, **it is writable by the user only**.
4. Any changes to the schemas in that folder are detected and propagated to the pipeline
   automatically on the next run. It means that after a user update, the schema in the `import`
   folder reverts all the automatic updates from the data.

In the next steps, we'll experiment a lot; you will be warned to set `dev_mode=True` until we are done experimenting.

:::caution
`dlt` will **not modify** tables after they are created.
So if you have a `yaml` file, and you change it (e.g., change a data type or add a hint),
then you need to **delete the dataset**
or set `dev_mode=True`:
```py
dlt.pipeline(
    import_schema_path="schemas/import",
    export_schema_path="schemas/export",
    pipeline_name="chess_pipeline",
    destination='duckdb',
    dataset_name="games_data",
    dev_mode=True,
)
```
:::

### Change the data type

In the export schema, we see that the `end_time` column in `players_games` has a `text` data type, while we know that it is a timestamp. Let's change it and see if it works.

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
Do not rename the tables or columns in the YAML file. `dlt` infers those from the data, so the schema will be recreated.
You can [adjust the schema](../general-usage/resource.md#adjust-schema) in Python before the resource is loaded.
:::

### Reorder columns
To reorder the columns in your dataset, follow these steps:

1. Initial Run: Execute the pipeline to obtain the import and export schemas.
1. Modify Export Schema: Adjust the column order as desired in the export schema.
1. Sync Import Schema: Ensure that these changes are mirrored in the import schema to maintain consistency.
1. Delete Dataset: Remove the existing dataset to prepare for the reload.
1. Reload Data: Reload the data. The dataset should now reflect the new column order as specified in the import YAML.

These steps ensure that the column order in your dataset matches your specifications.

**Another approach** to reorder columns is to use the `add_map` function. For instance, to rearrange ‘column1’, ‘column2’, and ‘column3’, you can proceed as follows:

```py
# Define the data source and reorder columns using add_map
data_source = resource().add_map(lambda row: {
    'column3': row['column3'],
    'column1': row['column1'],
    'column2': row['column2']
})

# Run the pipeline
load_info = pipeline.run(data_source)
```

In this example, the `add_map` function reorders columns by defining a new mapping. The lambda function specifies the desired order by rearranging the key-value pairs. When the pipeline runs, the data will load with the columns in the new order.

### Load data as JSON instead of generating nested tables or columns from flattened dicts

In the export schema, you can see that the properties of white and black players got flattened into:

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
column as `json`, which will instruct `dlt` not to flatten it (or not convert into a nested table in
case of a list). Do the same with the `black` column:

```yaml
players_games:
  columns:
    end_time:
      nullable: true
      data_type: timestamp
    white:
      nullable: false
      data_type: json
    black:
      nullable: false
      data_type: json
```

Run the pipeline script again, and now you can query the `black` and `white` columns with JSON
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
      data_type: json
    black:
      nullable: false
      data_type: json
```

## 4. Keep your import schema

Just add and push the import folder to git. It will be used automatically when cloned. Alternatively,
[bundle such schema with your source](../general-usage/schema.md#attaching-schemas-to-sources).

