---
title: Build pipelines and reports with LLMs
description: How to extract and explore data from REST API with AI editors/agents
keywords: [cursor, llm, restapi, ai]
---

# Build dlt pipelines and reports with LLMs

## Overview

This guide walks you through a collaborative AI-human workflow for extracting and exploring data from REST API sources using an AI editor/agent of your choice and dlt. It introduces the first workflow available in dltHub workspace — an LLM-native development environment for data engineering tasks.

You will learn:
1. How to initialize a dltHub workspace for your source using dltHub’s [LLM-context database](https://dlthub.com/workspace).
2. How to build a REST API source in minutes with AI assistance.
3. How to debug a pipeline and explore data using the workspace dashboard.
4. How to start a new notebook and work with the pipeline’s dataset in it.

## Prerequisites

Have one of the following AI editors/agents installed:
- [Cursor IDE](https://cursor.com/)
- [Continue](https://www.continue.dev/)
- [Cody](https://sourcegraph.com/cody)
- [Claude](https://docs.anthropic.com/en/docs/claude-code/ide-integrations)
- [Cline](https://cline.bot/)
- [Codex](https://openai.com/codex/)
- [Copilot](https://github.com/features/copilot)
- [Amp](https://ampcode.com/)
- [Windsurf](https://windsurf.com/)

## Concepts used in this guide

Before diving into the workflow, here’s a quick overview of key terms you’ll encounter:

1. **dlt workspace** - An environment where all data engineering tasks, from writing code to maintenance in production, can be executed by a single developer:
   - Develop and test data pipelines locally
   - Run dlt pipelines, transformations, and notebooks with one command
   - Deliver live, production-ready reports with streamlined access to the dataset

   We plan to support more functionality in the future, such as:
   - Deploy and run your data workflows in the cloud without any changes to code and schemas
   - Maintain pipelines with a Runtime Agent, customizable dashboards, and validation tests
   - Deploy live reports without worrying about schema drift or silent failures

2. **[Cursor](https://cursor.com/)** - An AI-powered code editor that lets you express tasks in natural language for an LLM agent to implement. Cursor is the first AI code editor we’ve integrated with, so the examples use Cursor, but the same workflow applies to Continue, Copilot, Cody, Windsurf, Cline, Claude, Amp, and Codex (only the UI/shortcuts differ).

3. **LLM-context** - A curated collection of prompts, rules, docs, and examples provided to an LLM for specific tasks. A rich context leads to more accurate, bug-free code generation. dltHub provides tailored [LLM-contexts for 1,000+ REST API sources](https://dlthub.com/workspace), so you can go from idea to working pipeline in under 10 minutes.

## Setup

### Setup your AI editor/agent

#### 1. Use the right model

For best results, use newer models. For example, in Cursor we’ve found that Claude-4-sonnet performs best (available in the paid version). Older or weaker models often struggle with context comprehension and workflows.

#### 2. Add documentation

AI code editors let you upload documentation and code examples to provide additional context. The exact steps vary by tool, but here are two examples:

1. Cursor ([guide](https://docs.cursor.com/context/@-symbols/@-docs)): Go to `Settings > Indexing & Docs` to add documentation.
2. Continue ([guide](https://docs.continue.dev/customize/context/documentation)): In chat, type `@Docs` and press `Enter`, then click `Add Docs`.

For any editor or agent, we recommend adding documentation scoped to a specific task.
At minimum, include:

* [REST API source](../verified-sources/rest_api/) as `@dlt_rest_api`
* [Core dlt concepts & usage](../../general-usage/) as `@dlt_docs`

### Install dlt workspace

```sh
pip install "dlt[workspace]"
```

### Initialize workspace

We provide LLM context from over 5,000 sources, available at [https://dlthub.com/workspace](https://dlthub.com/workspace). To get started, search for your API and follow the tailored instructions.

<div style={{textAlign: 'center'}}>
![search for your source](https://storage.googleapis.com/dlt-blog-images/llm_workflows_search.png)
</div>

To initialize a dltHub workspace, execute the following:

```sh
dlt init dlthub:{source_name} duckdb
```

This command will first prompt you to choose an AI editor/agent. If you pick the wrong one, no problem. After initializing the workspace, you can delete the incorrect editor rules and run `dlt ai setup` to select the editor again. This time it will only load the rules.

The dltHub workspace will be initialized with:
- Files and folder structure you know from [dlt init](../../walkthroughs/create-a-pipeline.md)
- Documentation scaffold for the specific source (typically a `yaml` file) optimized for LLMs
- Rules for `dlt`, configured for your selected AI editor/agent
- Pipeline script and REST API source (`{source_name}_pipeline.py`) definition that you'll customize in the next step

:::tip
If you can't find the source you need, start with a generic REST API source template. Choose a source name you need i.e.
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
Please generate a REST API source for {source} API, as specified in @{source}-docs.yaml
Start with endpoints {endpoints you want} and skip incremental loading for now.
Place the code in {source}_pipeline.py and name the pipeline {source}_pipeline.
If the file exists, use it as a starting point.
Do not add or modify any other files.
Use @dlt_rest_api as a tutorial.
After adding the endpoints, allow the user to run the pipeline with python {source}_pipeline.py and await further instructions.
```

In this prompt, we use `@` references to link source specifications and documentation. Make sure Cursor (or whichevert AI editor/agent you use) recognizes the referenced docs.
For example, see [Cursor’s guide](https://docs.cursor.com/context/@-symbols/overview) to @ references.

* `@{source}-docs.yaml` contains the source specification and describes the source with endpoints, parameters, and other details.
* `@dlt_rest_api` contains the documentation for dlt's REST API source.

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

### Validate with workspace dashboard

Launch the dashboard to validate your pipeline:

```sh
dlt pipeline {source}_pipeline show
```

The dashboard shows:
- Pipeline overview with state and metrics
- Data schema (tables, columns, types)
- Data itself, you can even write custom queries

The dashboard helps detect silent failures due to pagination errors, schema drift, or incremental load misconfigurations.

<div style={{textAlign: 'center'}}>
![dashboard](https://storage.googleapis.com/dlt-blog-images/llm-native-dashboard.png)
</div>

## Use the data in a notebook

With the pipeline and data validated, you can continue with custom data explorations and reports. You can use your preferred environment, for example, [Jupyter Notebook](https://jupyter.org/), [Marimo Notebook](https://marimo.io/), or a plain Python file.

:::tip
For an optimized data exploration experience, we recommend using a Marimo notebook. Check out the [detailed guide on using dlt with Marimo](../../general-usage/dataset-access/marimo).
:::

To access the data, you can use the `dataset()` method:

```py
import dlt

my_data = dlt.pipeline("{source}_pipeline").dataset()
# get any table as Pandas frame
my_data.table("table_name").df().head()
```

For more, see the [dataset access guide](../../general-usage/dataset-access).

## Next steps: production deployment

- [Prepare production deployment](../../walkthroughs/share-a-dataset.md)
- [Deploy a pipeline](../../walkthroughs/deploy-a-pipeline/)


## Addon: bring your own LLM scaffold

LLMs can infer a REST API source definition from various types of input, and in many cases, it’s easy to provide what’s needed.

Here are a few effective ways to scaffold your source:

1. **FastAPI (Internal APIs)**. If you're using FastAPI, simply add a file with the autogenerated OpenAPI spec to your workspace and reference it in your prompt.
2. **Legacy code in any programming language**. Add the relevant code files to your workspace and reference them directly in your prompt. LLMs can extract useful structure even from older codebases.
3. **Human-readable documentation**. Well-written documentation works too. You can add it to your AI editor docs and reference it in your prompt for context.