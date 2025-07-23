---
title: LLM-native workflow
description: How to extract and explore data from REST API with Cursor
keywords: [cursor, llm, restapi, ai]
---

# LLM-native workflow

## Overview

This guide walks you through a collaborative AI-human workflow for extracting and exploring data from REST API sources using Cursor and dlt. It introduces the first workflow available in dltHub Workspace — an LLM-native development environment for data engineering tasks.

You will learn:
1. How to use dltHub's [LLM-context database](https://dlthub.com/workspace) to init workspace for the source you need.
2. How to build a REST API source in minutes with AI assistance.
3. How to debug the pipeline and explore data using the pipeline dashboard.
4. How to start a new notebook and use the pipeline's dataset in it.

## Prerequisites

- [Cursor IDE](https://cursor.com/) installed

## Concepts used in this guide

Before diving into the workflow, here’s a quick overview of key terms you’ll encounter:

1. **dltHub Workspace** - An environment where all data engineering tasks, from writing code to maintenance in production, can be executed by single developer:
   - Develop and test data pipelines locally
   - Run dlt pipelines, transformations, and notebooks with one command
   - Deliver live, production-ready reports with streamlined access to the dataset

   We plan to support more functionality in the future, such as:
   - Deploy and run your data workflows in the cloud without any changes to code and schemas
   - Maintain pipelines with a Runtime Agent, customizable dashboards, and validation tests
   - Deploy live, reports without worrying about schema drift or silent failures

2. **[Cursor](https://cursor.com/)** - An AI-powered code editor that lets you express tasks in natural language for an LLM agent to implement. This LLM-native workflow isn’t exclusive to Cursor, but it’s the first AI code editor we’ve integrated with.

3. **LLM-context** - A curated collection of prompts, rules, docs, and examples provided to an LLM for specific tasks. A rich context leads to more accurate, bug-free code generation. dltHub provides tailored [LLM-context for 1,000+ REST API sources](https://dlthub.com/workspace), so you can go from idea to working pipeline in under 10 minutes.

## Setup

### Setup Cursor

1. Use the right model
For best results, use Claude 3.7-sonnet, Gemini 2.5+ or higher models. Weaker models struggle with context comprehension and workflow consistency.
We've observed the best results with Claude 3.7-sonnet (which requires the paid version of Cursor).

2. Add documentation
AI code editors let you upload documentation and code examples to provide additional context. [Here](https://docs.cursor.com/context/@-symbols/@-docs) you can learn how to do it with Cursor.
Go to `Cursor Settings > Indexing & Docs` to see all your added documentation. You can edit, delete, or add new docs here. We recommend adding documentation scoped for a specific task. Add the following documentation links:

    * [REST API Source](../verified-sources/rest_api/) as `@dlt rest api`
    * [Core dlt concepts & usage](../../general-usage/resource) as `@dlt docs`

### Install dlt Workspace

```sh
pip install dlt[workspace]
```

### Initialize workspace

dltHub provides prepared contexts for 1000+ sources, available at [https://dlthub.com/workspace](https://dlthub.com/workspace). To get started, search for your API and follow the tailored instructions.

<div style={{textAlign: 'center'}}>
![search for your source](https://storage.googleapis.com/dlt-blog-images/llm_workflows_search.png)
</div>

To initialize dltHub Workspace, execute the following:

```sh
dlt init dlthub:{source_name} duckdb
```

This command will initialize the dltHub Workspace with:
- Files and folder structure you know from [dlt init](../../walkthroughs/create-a-pipeline.md)
- Documentation scaffold for the specific source (typically a `yaml` file) optimized for LLMs
- Cursor rules tailored for `dlt`
- Pipeline script and REST API Source (`{source_name}_pipeline.py`) definition that you'll customize in next step

:::tip
If you can't find the source you need, start with a generic REST API Source template. Choose source name you need i.e.
```sh
dlt init dlthub:my_internal_fast_api duckdb
```
This will generate the full pipeline setup, including the script (`my_internal_fast_api_pipeline.py`) and all the files and folders you’d normally get with a standard [dlt init](../../walkthroughs/create-a-pipeline.md).
To make your source available to the LLM, be sure to [include the documentation](#addon-bring-your-own-llm-scaffold) in the context so the model can understand how to use it.
:::

## Create dlt pipeline

### Generate code

To get started quickly, we recommend using our pre-defined prompts tailored for each API. Visit [https://dlthub.com/workspace](https://dlthub.com/workspace) and copy the prompt for your selected source.
Prompts are adjusted per API to provide the most accurate and relevant context.

Here's a general prompt template you can adapt:

```text
Please generate a REST API Source for {source} API, as specified in @{source}-docs.yaml
Start with endpoints {endpoints you want} and skip incremental loading for now.
Place the code in {source}_pipeline.py and name the pipeline {source}_pipeline.
If the file exists, use it as a starting point.
Do not add or modify any other files.
Use @dlt rest api as a tutorial.
After adding the endpoints, allow the user to run the pipeline with python {source}_pipeline.py and await further instructions.
```

YIn this prompt, we use `@` references to link to source specifications and documentation. Make sure Cursor recognizes the referenced docs.
You can learn more about [referencing with @ in Cursor](https://docs.cursor.com/context/@-symbols/overview).

* `@{source}-docs.yaml` contains the source specification. Describes the source with endpoints, parameters, and other details.
* `@dlt rest api` contains the documentation for dlt's REST API source.

### Add credentials

Prompt the LLM for credential setup instructions and add them to your workspace secrets file `.dlt/secrets.toml`.

## Run the pipeline

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

### Validate with Pipeline Dashboard

Launch the dashboard to validate your pipeline:

```sh
dlt pipeline {source}_pipeline show --dashboard
```

The dashboard shows:
- Pipeline overview with state and metrics
- Data schema (tables, columns, types)
- Data itself, you can even write custom queries

The dashboard helps detect silent failures due to pagination errors, schema drift, or incremental load misconfigurations.

## Use the data in a Notebook

With the pipeline and data validated, you can continue with custom data explorations and reports. You can use your preferred environment, for example, [Jupyter Notebook](https://jupyter.org/), [Marimo Notebook](https://marimo.io/), or a plain Python file.

:::tip
For an optimized data exploration experience, we recommend using a Marimo notebook. Check out the [detailed guide on using dlt with Marimo](../../general-usage/dataset-access/marimo).
:::

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


## Addon: bring your own LLM Scaffold

LLMs can infer a REST API Source definition from various types of input, and in many cases, it’s easy to provide what’s needed.

Here are a few effective ways to scaffold your source:

1. **FastAPI (Internal APIs)**. If you're using FastAPI, simply add a file with the autogenerated OpenAPI spec to your workspace and reference it in your prompt.
2. **Legacy code in any programming language**. Add the relevant code files to your workspace and reference them directly in your prompt. LLM can extract useful structure even from older codebases.
3. **Human-readable documentation**. Well-written documentation works too. You can add it to your Cursor docs and reference it in your prompt for context.