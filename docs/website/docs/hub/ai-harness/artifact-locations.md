---
title: Artifact locations
description: Where the dltHub AI Harness writes schemas, business knowledge, exploration plans and platform files in a workspace, and what to commit.
keywords: [ai harness, workspace, artifacts, .dlthub, schemas, ontology, explorations, gitignore]
---
# Artifact locations

The toolkits of the [AI Harness](introduction.md) write what they produce into your workspace at fixed locations, all under `.dlthub/` in the project root. Each kind of artifact has one location, so the next skill, another agent or a teammate finds it there.

## The locations

| Location                       | What it holds                                                                                                                                                                       | Written by                           |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------ |
| `.dlthub/schemas/`             | dlt schemas. It is the default `import_schema_path` of a pipeline                                                                                                                   | dlt, toolkits                        |
| `.dlthub/schemas/export/`      | exported dlt schemas, the `export_schema_path`, only when you want them committed                                                                                                   | dlt                                  |
| `.dlthub/ontology/<name>/`     | the business knowledge and semantic information of one topic: the business context, the annotated source schemas, the taxonomy, the ontology, the data model and the semantic views | `transformations`, platform toolkits |
| `.dlthub/explorations/<name>/` | data exploration plans: the questions, and the query and chart of each answer                                                                                                       | `data-exploration`                   |
| `.dlthub/<platform>/`          | the files of one platform toolkit, for example deploy records in `deploy/<environment>/`                                                                                            | that platform toolkit                |

`<name>` in `.dlthub/ontology/<name>/` is the topic, for example `order_fulfillment`. It is chosen once, and every skill that works on the topic reuses it. A semantic view of any platform sits next to the model it describes.

Each platform toolkit owns one folder: `.dlthub/platform/` belongs to `dlthub-platform` and `.dlthub/snowflake/` to `snowflake-platform`. No other toolkit writes there.

Read the [toolkit catalog](toolkits.md) for what each toolkit does.

## What to commit

Commit everything under `.dlthub/`. It holds no secrets, and the files are the source of truth for what the agent built: the model, the semantic views, the deploy records. The workspace `.gitignore` that `dlthub init` and `dlthub-init` create does not ignore `.dlthub/`.

dlt keeps its own local files in `.dlt/`. The secrets files, the pipeline state (`.dlt/state/`), the local data (`.dlt/data/`) and the active profile (`.dlt/profile-name`) are gitignored. See [Workspace setup](../pipeline-operations/workspace-setup.md).

## Your code is yours

The harness does not decide where your sources, pipelines and transformations live. A path in a skill is an example: the agent follows the layout your workspace already has, or asks you. Two defaults remain:

- `dlthub pipeline init` writes a new pipeline script to the project root. You can move it.
- The `transformations` toolkit puts transformation scripts in `transformations/` unless your workspace keeps them elsewhere.

Throwaway scripts that the agent writes to explore a database or to check something run from the system temp folder, not from your workspace.

## Use the schema locations in your pipelines

To keep a schema you edit by hand, point the pipeline at the import location:

```py
import dlt

pipeline = dlt.pipeline(
    "orders",
    destination="duckdb",
    import_schema_path=".dlthub/schemas",
)
```

On the first run, dlt writes the schema to `.dlthub/schemas/<schema name>.schema.yaml` if the file does not exist. Later runs import your changes from it. Add `export_schema_path=".dlthub/schemas/export"` to also keep the schema that dlt inferred after each run. See [Adjust a schema](../../walkthroughs/adjust-a-schema.md).
