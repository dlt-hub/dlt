---
title: Build pipelines and reports with LLMs
description: How to extract and explore data from REST API with AI editors/agents
keywords: [cursor, llm, restapi, ai]
---

# REST API source in 10min with LLMs

<!--notes
"every page is page 1". We need to quickly introduce "dlt is Python code", "LLMs know
Python, but don't know data engineering or REST API", "we built the bridge"
-->

The Python library `dlt` provides a powerful toolkit to ingest data from any REST API source [LINK]. Combined with our LLM scaffolds [LINK] and MCP server [LINK], you can build a custom connector for more thank 8k sources in 10 minutes by following this guide!

<!--notes
Headers are my table of content. The previous "You will learn" was focused 
on "dlt features" not "problems the user wants to solve"
-->

:::note
You will need an AI-enabled IDE or agent, such as Copilot, Cursor, Continue, or more [LINK to our installation page] 
:::

<!--notes
pre-requisites are taking too much vertical space on the page 
-->

<!--notes
we don't need to introduce concepts here. Just link to the relevant page. This detracts
from the task the user wants to achieve. They should only be introduced to the minimal
slice of knowledge required. Here, we start talking about "we plan to"
-->

<!--notes
Instead of generic / abstract instructions with templated `{source_name}`, we should
pick a concrete source and use consistently throughout the guide. Then, we point out
that the concrete string changes based on source picked by user 

-->

## Setup
Before starting to build our connector, we need to initialize our
dltHub workspace [LINK] and configure our IDE.


<!--notes
Keep these sections action-focused and give links for people that want
to learn more; can also use collapsible HTML for details
-->


<!--notes
could be worth to mention how to setup virtual environments
-->
### Install `dlt` with the workspace extra

This will install the required Python dependencies.

```sh
pip install "dlt[workspace]"
```

### Initialize your dltHub workspace

To initialize your workspace, you will run a command of the following shape.

```sh
dlt init dlthub:{source} {destination}
```

For the source, you can pick from 8k+ REST API sources available at [https://dlthub.com/workspace](https://dlthub.com/workspace).


<!--note
Use a :::tip::: admonition to link to a separate page dedicated to this "bring
your own scaffold". This currently detracts from the current page. It's a topic
that deserves a dedicated page
(#addon-bring-your-own-llm-scaffold)
-->

For the destination, we strongly recommend using `duckdb` for local development.
Once you have a working pipeline, you'll be able to change the destination to your
data warehouse [LINK]

For example, this command gets you ready to ingest GitHub data into local DuckDB
```sh
dlt init dlthub:github duckdb
```

### Configuring your IDE
After running the `dlt init` command, you will be prompted to select the AI-enabled editor or agent that you want to use [LINK to supported].

<!--note
TODO add code snippet or screenshot of the user experience selecting the IDE
-->

:::warning
Your experience will greatly depend on the capabilities of the LLM you use. We suggest minimally using X from OpenAI, Y from Anthropic, Z from Mistral.
:::

### Verifying your setup
If you followed these steps, you should have TODO:
- describe directory structure and content (`.dlt/`, `{name}_pipeline.py`)
- MCP server is active inside your IDE; link to MCP docs page
- LLM scaffold file; briefly explains its content and how it relates to REST API source

<!--note
I don't know how good the "add documentation" feature inside Cursor, Continue, etc.
work. Ideally, we ship the right necessary features directly with the MCP
-->

## Craft your custom REST API source
<!--note
empowerement / encourage users to try it out; don't follow the prompts like it's
a perfect recipe or that it will give consistent results
Can be an occasion to link the community where people can discuss LLMs.
-->

This section gives practical advice to build a custom connector with LLMs. 
AI-tooling is evolving quickly and you'll figure out your own recipe
from hands-on experience. Feel free to share on Slack [LINK] 

The typical workflow is:

<!--TODO
add a mermaid chart

generate code -> run pipeline
run pipeline -> fails
fails -> manage context
manage context -> generate code
run pipeline -> success
success -> dlt dashboard
dlt dashboard -> notebook / app (final state; would a working dataset)
dlt dashboard -> generate code (add endpoints; refine pipeline)


This should be more of a "tips" section than a linear set of steps
because this is not linear at all
-->

### Start with a single endpoint

Your first goal should be to have a working pipeline that can ingest a single API endpoint. This means asking the LLM to load data from a single endpoint and remove code
that seems unnecessary

<!--note
code snippet of what a minimal REST API config looks like;
with "anatomy" comments 
-->

This achieves a few things:
- Ensure valid credentials for both the source and destination
- Gives a working code example for the LLM to build additional endpoints
- Allows you to inspected loaded data and gain insights about the REST API

To get started, you will find a copy-pasteable prompt on the page of the source you select
on [https://dlthub.com/workspace](https://dlthub.com/workspace)

For reference, I can run my GitHub pipeline via
```sh
python github_pipeline.py
```

And a successful execution should print to the terminal:
```sh
Pipeline github_rest_api load step completed in 0.26 seconds
1 load package(s) were loaded to destination duckdb and into dataset github_rest_api_data
The duckdb destination used duckdb:/github_rest_api.duckdb location to store data
Load package 1749667187.541553 is LOADED and contains no failed jobs
```

### Solving credentials issues

Your first few iterations will probably trigger credentials errors. This can be easily fixed by filling the `.dlt/config.toml` and `.dlt/secrets.toml` or by using environment variables. [LINK] 

Unfortunately, getting your credentials or your API key from a source system can be tedious. For popular sources, the LLM can sometimes provide helpful step-by-step instructions to obtain credentials.


### Context selection

If you're looking to ingest from specific endpoints or implement specific `dlt` features,
you can use TODO

- use `@` to refer to documentation
- use `@` to refer to the terminal output (if you ran a command and it produced an error)
- ask the LLM to use a specific MCP tool (e.g., check if data is loaded, what is the schema, how many rows)


### Ask the MCP server
If the dlt MCP server is connected [LINK], you can directly ask in the IDE chat window if the data was successfully loaded.

TODO: list a few example prompt / queries


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

### Access the data
<!--notes
"each page is the first page": introduce the dataset construct. The benefits can be 
better explained. The fact that "reading data from Snowflake or duckdb or bigquery"
is **huge**, especially for data scientists and analysts. Just dunk the credentials
in `config.toml` and `secrets.toml` and you don't even have to care where the data
is stored.
-->

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

<!--notes
rethink next step; realistically people are not going from vibe-coded to production
more realistic: build a report (marimo notebook), deploy on runtime, change from local
duckdb to  
-->
## Next steps

- [Prepare production deployment](../../walkthroughs/share-a-dataset.md)
- [Deploy a pipeline](../../walkthroughs/deploy-a-pipeline/)
