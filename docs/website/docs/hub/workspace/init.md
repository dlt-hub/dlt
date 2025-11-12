---
title: Initialize a pipeline
description: How to initialize and develop a dlt pipeline using CLI, workspace, or verified sources
keywords: [create a pipeline, verified source, workspace, rest client, how to, dlt init]
---

# Initialize a pipeline

This guide walks you through creating and initializing a `dlt` pipeline in dltHub Workspace — whether manually, with the LLM help, or from one of the **verified sources** maintained by dltHub team.



## Overview

A `dlt` pipeline moves data from a source (like an API or database) into a destination (like DuckDB, Snowflake, or Iceberg). Initializing a pipeline is the first step in the data workflow.
You can create one in three CLI-based ways:

| Method          | Command | Best for |
|-----------------|----------|----------|
| Manual          | `dlt init <source> <destination>` | Developers who prefer manual setup |
| LLM-native      | `dlt init dlthub:<source> <destination>` | AI-assisted development with editors like Cursor |
| Verified source | `dlt init <verified_source> <destination>` | Prebuilt, tested connectors from the community and dltHub team |


## Step 0: Install dlt with workspace support

Before you start, make sure that you followed [installation instructions](../getting-started/installation.md) and enabled [additional Workspace features](../getting-started/installation.md#enable-dlthub-free-tier-features)

**dltHub Workspace** is a unified environment for developing, running, and maintaining data pipelines — from local development to production.

[More about dlt Workspace](../workspace/overview.md)


## Step 1: Initialize a custom pipeline

### Manual setup (standard workflow)

A lightweight, code-first approach ideal for developers comfortable with Python.

```sh
dlt init {source_name} duckdb
````

for example:

```sh
dlt init my_github_pipeline duckdb
```

It scaffolds the pipeline template — a minimal starter project with a single Python script that shows three quick ways to load data into DuckDB using dlt:

- fetch JSON from a public REST API (Chess.com as an example) with requests,
- read a public CSV with pandas, and
- pull rows from a SQL database via SQLAlchemy.

The file also includes an optional GitHub REST client example (a `@dlt.resource` + `@dlt.source`) that can use a token from `.dlt/secrets.toml`, but will work unauthenticated at low rate limits.
It’s meant as a hands-on playground you can immediately run and then adapt into a real pipeline.

Learn how to build you own dlt pipeline with [dlt Fundamentals course.](https://dlthub.learnworlds.com/course/dlt-fundamentals)


### LLM-native setup

A collaborative AI-human workflow that integrates `dlt` with AI editors and agents like:
- **Cursor**,
- **Continue**,
- **Copilot**,
- [the full list](../../dlt-ecosystem/llm-tooling/llm-native-workflow#prerequisites)


**Initialize your first workspace pipeline**

dltHub provides prepared contexts for thousands of different sources, available at [https://dlthub.com/workspace](https://dlthub.com/workspace).
To get started, search for your API and follow the tailored instructions.

![search for your source](https://storage.googleapis.com/dlt-blog-images/workspace-images/workspace.png)

To initialize a dltHub workspace, execute the following:

```sh
dlt init dlthub:{source_name} duckdb
```

For example:

```sh
dlt init dlthub:github duckdb
```

The command scaffolds a **workspace-ready REST API pipeline project** with AI-assisted development support.

It creates:

* A **`{source_name}_pipeline.py`** file containing a placeholder REST API source (`@dlt.source`) using `RESTAPIConfig` and `rest_api_resources`, preconfigured for the DuckDB destination.
* A **`.dlt/secrets.toml`** file where you can store API credentials and tokens.
* Dependency instructions suggesting adding `dlt[duckdb]>=1.18.0a0` to your `pyproject.toml`.
* **AI assistant rule files** that enable `dlt ai` workflows.
* A **`{source_name}-docs.yaml`** file providing source-specific context for the LLM.

It will first prompt you to choose an AI editor/agent.
If you pick the wrong one, no problem.
After initializing the workspace, you can delete the incorrect editor rules and run `dlt ai setup` to select the editor again.


**Generate code**

To get started quickly, we recommend using our pre-defined prompts tailored for each API. Visit [https://dlthub.com/workspace](https://dlthub.com/workspace) and copy the prompt for your selected source.
Prompts are adjusted per API to provide the most accurate and relevant context.

Here's a general prompt template you can adapt:

```text
Please generate a REST API source for {source} API, as specified in @{source}-docs.yaml
Start with endpoints {endpoints you want} and skip incremental loading for now.
Place the code in {source}_pipeline.py and name the pipeline {source}_pipeline.
If the file exists, use it as a starting point.
Do not add or modify any other files.
Use @dlt_rest_api as a tutorial.
After adding the endpoints, allow the user to run the pipeline with python {source}_pipeline.py and await further instructions.
```

In this prompt, we use `@` references to link source specifications and documentation. Make sure Cursor (or whichever AI editor/agent you use) recognizes the referenced docs.
For example, see [Cursor’s guide](https://docs.cursor.com/context/@-symbols/overview) to @ references.

* `@{source}-docs.yaml` contains the source specification and describes the source with endpoints, parameters, and other details.
* `@dlt_rest_api` contains the documentation for dlt's REST API source.

For more on the workspace concept, [see LLM-native workflow](../../dlt-ecosystem/llm-tooling/llm-native-workflow)


### Verified source setup (community connectors)

You can also initialize a [verified source](../../dlt-ecosystem/verified-sources) — prebuilt connectors contributed and maintained by the dlt team and community.

**List and select a verified source**

List available sources:

```sh
dlt init -l
```

Pick one, for example:

```sh
dlt init github duckdb
```
**Project structure**

This command creates a project like:

```text
├── .dlt/
│   ├── config.toml
│   └── secrets.toml
├── github/
│   ├── __init__.py
│   ├── helpers.py
│   ├── queries.py
│   ├── README.md
│   ├── settings.py
├── github_pipeline.py
```

Follow the command output to install dependencies and add secrets.

**General process**

To initialize any verified source:

```sh
dlt init {source_name} {destination_name}
```

For example:

```sh
dlt init google_ads duckdb
dlt init mongodb bigquery
```

After running the command:

* The project directory and required files are created.
* You’ll be prompted to install dependencies.
* Add your credentials to `.dlt/secrets.toml`.

**Update or customize verified sources**

You can modify an existing verified source in place.

* If your changes are **generally useful**, consider contributing them back via PR.
* If they’re **specific to your use case**, make them modular so you can still pull upstream updates.

:::info
`dlt` includes several powerful, built-in sources for extracting data from different systems:
* [rest_api](../../dlt-ecosystem/verified-sources/rest_api) — extract data from any REST API using a declarative configuration for endpoints, pagination, and authentication.
* [sql_database](../../dlt-ecosystem/verified-sources/sql_database) — load data from 30+ SQL databases via SQLAlchemy, PyArrow, pandas, or ConnectorX. Supports automatic table reflection and all major SQL dialects.
* [filesystem](../../dlt-ecosystem/verified-sources/filesystem) — load files from local or cloud storage (S3, GCS, Azure Blob, Google Drive, SFTP). Natively supports CSV, Parquet, and JSONL formats.

Together, these sources cover the most common data ingestion scenarios — from APIs and databases to files.
:::

[Read more about verified sources](../../walkthroughs/add-a-verified-source)

## Step 2: Add credentials

Most pipelines require authentication or connection details such as API keys, passwords, or database credentials.
`dlt` retrieves these values automatically through **config providers**, which it checks in order when your pipeline runs.

**Provider priority:**

1. **Environment variables** – the highest priority.

   ```sh
   export SOURCES__GITHUB__API_SECRET_KEY="<github_personal_access_token>"
   export DESTINATION__DUCKDB__CREDENTIALS="duckdb:///_storage/github_data.duckdb"
   ```
2. **`.dlt/secrets.toml` and `.dlt/config.toml`** – created automatically when initializing a pipeline.

   * `secrets.toml` → for sensitive values (API tokens, passwords)
   * `config.toml` → for non-sensitive configuration
     Example:

   ```toml
   [sources.github]
   api_secret_key = "<github_personal_access_token>"

   [destination.duckdb]
   credentials = "duckdb:///_storage/github_data.duckdb"
   ```
3. **Vaults** – such as Google Secret Manager, Azure Key Vault, AWS Secrets Manager, or Airflow Variables.
4. **Custom providers** – added via `register_provider()` for your own configuration formats.
5. **Default values** – from your function signatures.


**Using credentials in code**

`dlt` automatically injects secrets into your functions when you call them.
For example:

```py
@dlt.source
def github_api_source(api_secret_key: str = dlt.secrets.value):
    return github_api_resource(api_secret_key=api_secret_key)
```

You don’t need to load secrets manually — `dlt` resolves them from any of the above providers.


[Read more about setting credentials](../../general-usage/credentials)

## Step 3: Run a pipeline

**Run your script**

Run the pipeline to verify that everything works correctly:

```sh
python {source_name}_pipeline.py
```

This executes your pipeline — fetching data from the source, normalizing it, and loading it into your chosen destination.


**What you should see**

A printed `load_info` summary similar to::

```text
Pipeline github_api_pipeline completed in 0.7 seconds
1 load package(s) were loaded to destination duckdb and into dataset github_data
Load package 1749667187.541553 is COMPLETED and contains no failed jobs
```


**Monitor progress (optional)**

```sh
pip install enlighten
PROGRESS=enlighten python {source_name}_pipeline.py
```

Alternatives: `tqdm`, `alive_progress`, or `PROGRESS=log`.

[See monitor loading progress](../../general-usage/pipeline#monitor-the-loading-progress)

**Inspect loads & trace**

```sh
dlt pipeline {pipeline_name} info            # overview
dlt pipeline {pipeline_name} load-package    # latest package
dlt pipeline -v {pipeline_name} load-package # with schema changes
dlt pipeline {pipeline_name} trace           # last run trace & errors
```

[Read more about running a pipeline](../../walkthroughs/run-a-pipeline)

## Next steps: Deploy and scale

Once your pipeline runs locally:
* [Monitor via the workspace dashboard](../../general-usage/dataset-access/data-quality-dashboard)
* Set up [Profiles](../core-concepts/profiles-dlthub.md) to manage separate dev, prod, and test environments
* [Deploy a pipeline](../../walkthroughs/deploy-a-pipeline/)
