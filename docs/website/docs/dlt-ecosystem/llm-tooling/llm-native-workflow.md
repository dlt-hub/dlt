---
title: REST API source in 10min
description: Build a custom REST API connector in 10min
keywords: [cursor, llm, restapi, ai]
---

# REST API source in 10min

## Overview

The Python library `dlt` provides a powerful [REST API toolkit](../../dlt-ecosystem/verified-sources/rest_api/basic.md) to ingest data. Combined with our [LLM scaffolds](https://dlthub.com/workspace) and [MCP server](../../hub/features/mcp-server.md), you can build a custom connector for any of the 8k+ available sources in 10 minutes by following this guide.

Building with LLMs is an iterative process. We will follow this general workflow and give practical tips for each step.

```mermaid
stateDiagram-v2
    setup: setup workspace
    instructions: initial instructions
    codegen: generate code
    run: run pipeline
    context: manage context
    data: check data
    commit: commit working code

    [*] --> setup: start
    setup --> instructions: workspace is ready 
    instructions --> codegen: ask to ingest endpoint
    codegen --> run: code generated
    run --> context: fails
    run --> data: completes
    context --> codegen: context updated
    data --> context: is incorrect
    data --> commit: is correct
    commit --> instructions: add endpoint / refine config
    commit --> [*]
```

:::note
You will need an AI-enabled IDE or agent, such as Copilot, Claude Code, Cursor, Continue, etc.
:::


## Setup
Before starting to build our connector, we need to initialize our [dltHub workspace](../../hub/workspace/overview.md) and configure our IDE.


### Python dependencies

The following command will install the Python library `dlt` with the `workspace` extra. 

```sh
pip install "dlt[workspace]"
```

### Initialize workspace

To initialize your workspace, you will run a command of the following shape.

```sh
dlt init dlthub:{source} {destination}
```

For the destination, using `duckdb` during local development is recommended.
Once you have a working pipeline, you easily change the destination to your
data warehouse.

For the source, visit [https://dlthub.com/workspace](https://dlthub.com/workspace) to browse the 8k+ REST API sources. The page will contain the command to initialize the pipeline.

For example, this command setups ingestion from GitHub to local DuckDB.
```sh
dlt init dlthub:github duckdb
```

After running the `dlt init` command, you will be prompted to select the AI-enabled editor or agent that you want to use.

```shell
❯ dlt init dlthub:github duckdb
dlt will generate useful project rules tailored to your assistant/IDE.
Press Enter to accept the default (cursor), or type a name:
```

### Configure IDE

Your experience will greatly depend on the capabilities of the LLM you use. We suggest minimally using `GPT-4.1` from OpenAI or `Claude Sonnet 4` from Anthropic.

TODO MCP server instructions

### Verify setup
TODO
If you followed these steps, you should have:
- describe directory structure and content (`.dlt/`, `{name}_pipeline.py`)
- MCP server is active inside your IDE; link to MCP docs page
- LLM scaffold file; briefly explains its content and how it relates to REST API source


## Initial instructions

To get good result and make progress, it's best to implement one REST endpoint at a time.

The source's page on dlthub.com/workspace includes a prompt to get you started that looks
like this:

```
Generate a REST API Source for {source}, as specified in @{source}-docs.yaml
Start with endpoint {endpoint_name} and skip incremental loading for now. 
Place the code in {source}_pipeline.py and name the pipeline {source}_pipeline. 
If the file exists, use it as a starting point. 
Do not add or modify any other files. 
After adding the endpoint, allow the user to run the pipeline with 
`python {source}_pipeline.py`
and await further instructions.
```

:::tip
Reference `{source}-docs.yaml` and ask what the available endpoints are.
:::

## Generate code

The LLM can quickly produce a lot of code. When reviewing its proposed changes, your role is to nudge it in the right direction. 

### Anatomy of a REST API source
Before practical tips, let's look at a minimal REST API source:  

```python
import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig

# decorator indicates that this function produces a source
@dlt.source
def github_source(
    # the `access_token` will be retrieved from `.dlt/secrets.toml` by default
    access_token: str = dlt.secrets.value
):
    config: RESTAPIConfig = {
        # client section
        "client": {
            "base_url": "https://api.github.com/v3/",
            # access token will be passed via headers
            "auth": {"type": "bearer", "token": access_token},
        },
        # endpoint section
        "resources": [
            # refers to GET endpoint `/issues`
            "issues",
        ],
    }
    # returns a list of resources
    return rest_api_resources(config)
```

For now, it's best to delete all the code you don't understand (e.g., paginator, incremental, data selector). This keeps the LLM focused and reduces the surface for bugs. After generating a working pipeline and committing code, you can go back configure endpoints more precisely. 

:::tip
Reference `{source}-docs.yaml` and ask what the available endpoints parameters are.
:::

### Leveraging the IDE

In `dlt`, many features have built-in validation and completion suggestions. 

When the LLM produces invalid code, it will produce red error lines making code review easy.

![invalid rest api config](image.png)

Completion suggestions makes it easy to fix LLM errors or set configuration options. 

![rest api config completion suggestion](image-1.png)

## Run pipeline

Once the code seems decent, you can run it in a terminal using this command:

```sh
python github_pipeline.py
```

### Success
And a successful execution should print to the terminal:
```sh
Pipeline github_rest_api load step completed in 0.26 seconds
1 load package(s) were loaded to destination duckdb and into dataset github_rest_api_data
The duckdb destination used duckdb:/github_rest_api.duckdb location to store data
Load package 1749667187.541553 is LOADED and contains no failed jobs
```

### Source credentials error

Your first few iterations will probably trigger credentials errors. This can be easily fixed by filling the `.dlt/config.toml` and `.dlt/secrets.toml` or by using environment variables. [LINK] 

Unfortunately, getting your credentials or your API key from a source system can be tedious. For popular sources, the LLM can sometimes provide helpful step-by-step instructions to obtain credentials.

### Destination credentials error


## Manage context

If you're looking to ingest from specific endpoints or implement specific `dlt` features,
you can use TODO

- use `@` to refer to documentation
- use `@` to refer to the terminal output (if you ran a command and it produced an error)
- ask the LLM to use a specific MCP tool (e.g., check if data is loaded, what is the schema, how many rows)


## Check data

### Inspect dataset loads

You can use launch locally the interactive dlt dashboard [LINK] to view your pipeline execution.

```sh
dlt pipeline github_pipeline show
```

You can quickly view:
- Pipeline state and metrics
- Data schema (tables, columns, types)
- SQL data explorer

The dashboard helps detect silent failures due to pagination errors, schema drift, or incremental load misconfigurations.

<div style={{textAlign: 'center'}}>
![dashboard](https://storage.googleapis.com/dlt-blog-images/llm-native-dashboard.png)
</div>


### Data quality
- data validation
- schema contract
- data quality checks


### Ask the MCP server
If the dlt MCP server is connected [LINK], you can directly ask in the IDE chat window if the data was successfully loaded.

TODO: list a few example prompt / queries


### Interactive data exploration

Running a `dlt` pipeline creates a **dataset**. This provides a consistent interface to interact with loaded data and removes the destination-specific friction 

This snippets gives access to the GitHub data I loaded
```python
import dlt

# this refers to my previously ran pipeline
github_pipeline = dlt.pipeline("github_pipeline")
github_dataset = github_pipeline.dataset()

# call `.df()` to load the results as a pandas dataframe
github_dataset.table("pull_requests").df()
```

The dataset truly shines in interactive environments like marimo [LINK] or Jupyter [LINK]for data explorations, defining data quality checks, or writing data transformations. 

Learn more in our dataset guide [LINK]

## Next steps

- outcomes: you have a local pipeline and local dataset
- you can explore the dataset and build a data product
- you can configure the pipeline to write to remote destination


- [Prepare production deployment](../../walkthroughs/share-a-dataset.md)
- [Deploy a pipeline](../../walkthroughs/deploy-a-pipeline/)
