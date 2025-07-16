---
title: LLM-native workflow
description: How to extract and explore data from REST API with Cursor
keywords: [cursor, llm, restapi, ai]
---

# LLM-native workflow

## Overview

This guide walks you through a collaborative AI-human workflow for extracting and exploring data from REST API sources using Cursor and dlt. It introduces the first workflow available in dltHub Workspace — an LLM-native data engineering platform.

You will learn:
1. How to use dltHub's [LLM-context database](https://dlthub.com/workspace) to set up Cursor scaffolding
2. How to build a REST API source in minutes with AI assistance
3. How to explore pipeline metadata using the dltHub dashboard

This guide focuses on automatic LLM-context setup. If your REST API isn't listed [here](https://dlthub.com/workspace) or you want to add a custom context, see [the manual Cursor setup](#alternative-manually-setting-up-cursor).

:::note
REST APIs are ideal for AI-assisted development because they're declarative and self-documenting, making them easy to troubleshoot and refine. However, this approach can also be adapted for other source types.
:::

## Prerequisites

- [Cursor IDE](https://cursor.com/) installed

## Concepts used in this guide

Before diving into the workflow, here’s a quick overview of key terms you’ll encounter:

1. **dltHub Workspace** - A platform designed to empower individual Python developers to do what traditionally required full data teams:
   - Deploy and run dlt pipelines, transformations, and notebooks with one command
   - Maintain pipelines with a Runtime Agent, customizable dashboards, and validation tests
   - Deliver live, production-ready reports without worrying about schema drift or silent failures

   It's not yet fully available, but you can start with the initial workflow: LLM-native pipeline development for 1,000+ REST APIs.

2. **[Cursor](https://cursor.com/)** - An AI-powered code editor that lets you express tasks in natural language for an LLM agent to implement. This LLM-native workflow isn’t exclusive to Cursor, but it’s the first AI code editor we’ve integrated with.

3. **LLM-context** - A curated collection of prompts, rules, docs, and examples provided to an LLM for specific tasks. A rich context leads to more accurate, bug-free code generation. dltHub provides tailored [LLM-context for 1,000+ REST API sources](https://dlthub.com/workspace), so you can go from idea to working pipeline in under 10 minutes.

## Setup

### Install dlt Workspace

```sh
pip install dlt[workspace]
```

### Initialize project

dltHub provides prepared contexts for 1000+ sources, available at [https://dlthub.com/workspace](https://dlthub.com/workspace). To get started, search for your API and follow the tailored instructions.

:::tip
If your API is not supported, you can find the API docs online and [add them to the Cursor context manually](#alternative-manually-setting-up-cursor), but this may result in less focused outputs, increased risk of overlooking critical API details, and require more troubleshooting.
:::

To initialize your project, execute this dltHub Workspace command:

```sh
dlt init dlthub:{source_name} duckdb
```

This command will initialize the dltHub Workspace with prepared:
- Documentation scaffold for the specific source
- Cursor rules tailored for dlt
- Pipeline template files

### Model selection

For best results, use Claude 3.7-sonnet or Gemini 2.5+. Weaker models struggle with context comprehension and workflow consistency.

## Build dlt pipeline

### Generate code

We recommend starting with our prepared prompts for each API. Visit [https://dlthub.com/workspace](https://dlthub.com/workspace) and copy the suggested prompt for your chosen source. Note that the prompt may vary depending on the API to ensure the best context and accuracy.

Here's a general prompt template:

```text
Please generate a REST API Source for {source} API, as specified in @{source}-docs.yaml
Start with endpoints {endpoints you want} and skip incremental loading for now.
Place the code in {source}_pipeline.py and name the pipeline {source}_pipeline.
If the file exists, use it as a starting point.
Do not add or modify any other files.
Use @dlt rest api as a tutorial.
After adding the endpoints, allow the user to run the pipeline with python {source}_pipeline.py and await further instructions.
```

### Add credentials

Prompt the LLM for credential setup instructions and add them to your project secrets file `.dlt/secrets.toml`.

### Test the pipeline

Run your pipeline:

```sh
python {source}_pipeline.py
```

Expected output:
```sh
Pipeline {source} load step completed in 0.26 seconds
1 load package(s) were loaded to destination duckdb and into dataset {source}_data
The duckdb destination used duckdb:/{source}.duckdb location to store data
Load package 1749667187.541553 is LOADED and contains no failed jobs
```

:::tip
If the pipeline fails, pass error messages to the LLM. Restart after 4-8 failed attempts.
:::

## Observe and validate

### Use the Pipeline Dashboard

Launch the dashboard to validate your pipeline:

```sh
dlt pipeline {source}_pipeline show --dashboard
```

The dashboard shows:
- Pipeline overview with state and metrics
- Data schema (tables, columns, types)
- Data itself - you can even write custom queries

The dashboard helps detect silent failures due to pagination errors, schema drift, or incremental load misconfigurations.

### Access the data

With the pipeline and data validated, you can continue with custom data explorations and reports. You can use your preferred environment, for example, [Jupyter Notebook](https://jupyter.org/), [Marimo Notebook](https://marimo.io/), or a plain Python file.

To access the data, you can use the `dataset()` method:

```py
import dlt

my_data = dlt.pipeline("{source}_pipeline").dataset()
# get any table as Pandas frame
# my_data.{table_name}.df().head()
```

For more, see [dataset access guide](../../general-usage/dataset-access).

## Next steps: production deployment

- [Prepare production deployment](../../walkthroughs/share-a-dataset.md)
- [Deploy a pipeline](../../walkthroughs/deploy-a-pipeline/)

## Alternative: manually setting up Cursor

If your source is not present in our [LLM-context database](https://dlthub.com/workspace), you can manually set up the context for Cursor.

1. Adding docs

    AI code editors let you upload documentation and code examples to provide additional context. [Here](https://docs.cursor.com/context/@-symbols/@-docs) you can learn how to do it.

    Under Cursor `Settings > Features > Docs`, you can see all the docs you have added. You can edit, delete, or add new docs here. We recommend adding documentation scoped for a specific task. For example, for developing a REST API source, consider adding:

    * [REST API Source](../verified-sources/rest_api/) documentation
    * Core dlt [concepts and usage](../../general-usage/resource)
    * Example pipeline rest_api_pipeline.py
    * YAML definitions, OpenAPI specs, or other legacy source references

    We've observed that Cursor is not able to ingest full dlt docs (there are bug reports in Cursor about their docs crawler). It also struggles with very large documentation sets, and excessive context can reduce code quality.

2. Cursorignore

    If you have local docs in a folder in your codebase, Cursor will automatically index them unless they are added to `.gitignore` or `.cursorignore` files.

    To improve accuracy, make sure any files or docs that could confound the search are ignored.

    One important note is to put your `.dlt` folder in `.cursorignore` as well to ensure any sensitive information is protected.

3. Model and context selection

    We've had the best results with Claude 3.7-sonnet (which requires the paid version of Cursor). Weaker models were not able to comprehend the required context fully and were not able to use tools and follow workflows consistently.