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
2. How to build a REST API source in minutes with AI assistance
3. How to debug the pipeline and explore data using the pipeline dashboard
4. How to start a new notebook and use the pipeline's dataset in it

## Prerequisites

- [Cursor IDE](https://cursor.com/) installed

## Concepts used in this guide

Before diving into the workflow, here’s a quick overview of key terms you’ll encounter:

1. **dltHub Workspace** - An environment where all data engineering tasks, from writing code to maintenance in production, can be executed by single developer:
    > TODO: rewrite this
   - Develop and test locally with `dlt`, `duckdb` and `filesystem` then run in the cloud without any changes to code and schemas.
   - Deploy and run dlt pipelines, transformations, and notebooks with one command
   - Maintain pipelines with a Runtime Agent, customizable dashboards, and validation tests
   - Deliver live, production-ready reports without worrying about schema drift or silent failures

   It's not yet fully available, but you can start with the initial workflow: LLM-native pipeline development for 1,000+ REST APIs.

2. **[Cursor](https://cursor.com/)** - An AI-powered code editor that lets you express tasks in natural language for an LLM agent to implement. This LLM-native workflow isn’t exclusive to Cursor, but it’s the first AI code editor we’ve integrated with.

3. **LLM-context** - A curated collection of prompts, rules, docs, and examples provided to an LLM for specific tasks. A rich context leads to more accurate, bug-free code generation. dltHub provides tailored [LLM-context for 1,000+ REST API sources](https://dlthub.com/workspace), so you can go from idea to working pipeline in under 10 minutes.

## Setup

### Setup Cursor

> TODO: review and make this section smooth

1. Use the right model
For best results, use Claude 3.7-sonnet or Gemini 2.5+. Weaker models struggle with context comprehension and workflow consistency.
We've had the best results with Claude 3.7-sonnet (which requires the paid version of Cursor). Weaker models were not able to comprehend the required context fully and were not able to use tools and follow workflows consistently.

2. Add documentation

    AI code editors let you upload documentation and code examples to provide additional context. [Here](https://docs.cursor.com/context/@-symbols/@-docs) you can learn how to do it.

    Under Cursor `Settings > Features > Docs`, you can see all the docs you have added. You can edit, delete, or add new docs here. We recommend adding documentation scoped for a specific task. For example, for developing a REST API source, consider adding:

    * [REST API Source](../verified-sources/rest_api/) documentation


### Install dlt Workspace

```sh
pip install dlt[workspace]
```

### Initialize workspace

dltHub provides prepared contexts for 1000+ sources, available at [https://dlthub.com/workspace](https://dlthub.com/workspace). To get started, search for your API and follow the tailored instructions.


To initialize your workspace, execute this dltHub Workspace command:

```sh
dlt init dlthub:{source_name} duckdb
```

This command will initialize the dltHub Workspace with:
- files and folder structure you know from [dlt init](../../walkthroughs/create-a-pipeline.md)
- Documentation scaffold for the specific source (typically a `yaml` file)
- Cursor rules tailored for `dlt`
- Pipeline script and REST API Source (`{source_name}_pipeline.py`) definition that you'll customize in next step

:::tip
If you can't find the source you need, start with a generic REST API Source template. Choose source name you need ie.
```sh
dlt init dlthub:my_internal_fast_api duckdb
```
You'll still get full cursor setup and pipeline script (`my_internal_fast_api_pipeline.py`) plus all files and folder you get with regular [dlt init](../../walkthroughs/create-a-pipeline.md).

You'll need to [provide an useful REST API scaffold](#addon-bring-your-own-llm-scaffold) for your LLM model, though.

:::


## Create dlt pipeline

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

> TODO: the crucial part is to explain the context in the prompt. this is basically why we write this docs - to give a little background to the walkthrough on the website.
> in the prompt above: we link to the scaffold/spec, pipeline script, dlt rest api docs etc. this should be explained

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
- Data itself - you can even write custom queries

The dashboard helps detect silent failures due to pagination errors, schema drift, or incremental load misconfigurations.

## Use the data in a Notebook

With the pipeline and data validated, you can continue with custom data explorations and reports. You can use your preferred environment, for example, [Jupyter Notebook](https://jupyter.org/), [Marimo Notebook](https://marimo.io/), or a plain Python file.

> TODO: (1) maybe a short instruction how to bootstrap marimo notebook would help? (2) we have some instructions in our docs already https://dlthub.com/docs/general-usage/dataset-access/marimo 

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
LLMs can infer REST API Source definition from many kinds of specs and sometimes providing one is fairly easy.

1. If you use Fast API (ie. for internal API) - use autogenerated openAPI spec and refer to it in your prompt.
2. If you have legacy code in any Language, add it to the workspace and refer to it in your prompt.
3. A good human readable documentation also works! You can try to add it ot Cursor docs and refer to it in your prompt.
