---
title: Deploy your first pipeline with dltHub
description: Scaffold a fresh dltHub workspace and let your coding agent deploy a pipeline to the managed platform in a few minutes
keywords: [dlthub, deploy, first pipeline, getting started, workspace, dlthub-start, onboarding]
---

# Deploy your first pipeline with dltHub

Go from nothing to a pipeline running on the managed [dltHub platform](https://app.dlthub.com/). 
`dlthub-start` scaffolds a workspace, signs you in, and installs the [AI Harness](https://github.com/dlt-hub/dlthub-ai-workbench) for your coding agent, **the agent then deploys and runs your first pipeline for you**.

Already have a `dlt` pipeline? See [Workspace setup](../pipeline-operations/workspace-setup.md) to convert an existing
project into a dltHub workspace.

## Before you start

- **Python 3.10–3.14** and [uv](https://docs.astral.sh/uv/) (recommended). See [installation](installation.md) for
  alternatives.                               
- **A coding agent** on your `PATH`: Claude Code, Cursor, or Codex.                                                 
- **A dltHub account.** `dlthub-start` signs you in via OAuth (GitHub, Google, or email) and creates your account on
   first login. Use is governed by the [dltHub License](../license.md). You automatically get a [Playground workspace](playground-workspace.md) to complete this guide with no setup required. 
- dltHub is commercial; use is governed by the ([license](../license.md)). On sign-up you get a [Playground workspace](playground-workspace.md) 
so you can complete this guide with no setup or cloud credentials.      

## 1. Scaffold, sign in, pick your agent

Run this in an empty directory — the folder name becomes your project package name:

```sh
uvx dlthub-start@latest
```

No arguments needed; the CLI is interactive and walks you through each step:

1. **Scaffolds a workspace** into the current directory (or a `./playground` folder if it isn't empty).
2. **Installs dependencies** with `uv sync` into `.venv`.
3. **Signs you in and connects a workspace**: opens an OAuth flow in your browser (creating your dltHub account on first
   use) and binds the project to your personal **Playground workspace**.
4. **Asks which coding agent you use** (Claude / Cursor / Codex) and adds that agent's workbench files — skills plus the
   dltHub MCP server.
5. **Offers to launch the agent** with a handoff prompt that tells it to continue onboarding using the
   `deploy-run-sample-pipeline` skill.


<!--  
```text
✓ Workspace ready — dependencies installed in .venv
Project package name: starter-test
|-- pyproject.toml
|-- pipeline.py
|-- __deployment__.py
|-- .dlt/
`-- README.md
✓ Logged in and connected to the playground workspace

Which coding agent do you want to use?
● claude
✓ Added claude workbench files

Next step: let claude deploy and run the sample pipeline on dltHub for you.
How do you want to continue?
● Launch claude now and hand it this prompt
```

Accept the launch and the agent picks up in the same terminal. If you decline, you can start the agent yourself later and
paste the prompt it printed — nothing is lost.

:::tip Explore first, then build your own
`dlthub-start` is the quickest way to explore dltHub end-to-end. It connects a personal
[Playground workspace](playground-workspace.md) and wires an **AI workbench** into your coding agent. Ask the agent to
build a pipeline for any source one-shot (for example, *"load the GitHub API into DuckDB"*) and it will write **and
deploy** it from scratch. Already have a `dlt` pipeline you maintain? See
[Workspace setup](../pipeline-operations/workspace-setup.md) to bring it onto the platform instead.
:::
 --> 

## 2. What you got

```text
starter-test/
├── pyproject.toml      # dlt[hub] + dependencies
├── pipeline.py         # the Sample Shop pipeline
├── __deployment__.py   # declares the deployable jobs
├── README.md           # what to do next
├── .dlt/               # config, secrets, and the .workspace marker
└── .claude/            # agent workbench: skills + MCP server (or .cursor/ / .codex/)
```

`pipeline.py` loads the Sample Shop dataset from a public sample REST API into the platform-managed [**Playground destination**](../getting-started/playground-workspace.md), with no warehouse, bucket, or credentials to configure. It's already a deployable job:

- decorated with `@run.pipeline`;
- listed in `__deployment__.py`.

See [Deployments](../pipeline-operations/deployments.md) for how the manifest works.

## 3. Let the agent deploy and run the sample

Hand over to the agent, which uses our [platform toolkit](https://github.com/dlt-hub/dlthub-ai-workbench/tree/master). It has the skills:
- `prepare-deployment`
- `setup-runtime`
- `deploy-workspace`
- `debug-deployment`

Tell the agent to "deploy to dltHub" and it will deploy the workspace, run the sample pipeline, and open the dataset browser. Every step is an ordinary CLI command, so you can follow along or run them yourself.

First, it deploys the workspace by uploading your files and registering every job in `__deployment__.py`:

```sh
uv run dlthub deploy
```

```text
Synced changed workspace files
Synced changed workspace configuration
3 job(s) found in __deployment__
```

The scaffold ships three jobs: `load_sample_shop` (the pipeline), `onboarding_success` (a notebook that browses the
loaded data), and the [observability `dashboard`](../ingestion/dashboard.md).

Then it runs the pipeline on the platform and follows the logs until it finishes:

```sh
uv run dlthub run load_sample_shop -f
```

The data lands in the `sample_shop` dataset on your [Playground destination](../ingestion/playground.md), a managed storage included with every workspace. When the run finishes, the agent prints a link to view it in the web UI:

```text
https://app.dlthub.com/w/<workspace-id>/runs/<run-id>
```

Finally, it opens the `onboarding_success` notebook so you can browse the loaded tables. At this point your first pipeline has run on the platform and the data is ready to query.

![alt text](https://storage.googleapis.com/dlt-blog-images/onboarding-dashboard.png)

## 4. Re-run and inspect

The `dlthub` CLI works in two modes:
- **`dlthub …`** - operates on the connected cloud workspace
- **`dlthub local …`** - runs on your machine using the local `dev` [profile](../pipeline-operations/profiles)

```sh
uv run dlthub run load_sample_shop -f      # run on the platform, follow logs until it finishes
uv run dlthub show                         # open the workspace overview in the browser
```

## 5. Point production at a cloud destination

The Playground destination is great for testing but isn't meant for production. For real data, you'll want to load into a destination you own.

The recommended approach: use an alias like `warehouse` in your code, then configure what it actually points to in each [profile](../pipeline-operations/profiles.md). This way the same code runs on local DuckDB during development and on your cloud warehouse in production.

Set your pipeline's destination to the alias:

```py
import dlt
from dlt.hub import run


@run.pipeline("sample_shop_pipeline")
def load_sample_shop():
    pipeline = dlt.pipeline(
        pipeline_name="sample_shop_pipeline",
        destination="warehouse",   # alias, resolved per profile
        dataset_name="sample_shop",
    )
    ...
```

Then configure the alias differently for each profile in `.dlt/` (secrets files are gitignored):

```toml
# dev.config.toml: local development
[destination.warehouse]
destination_type = "duckdb"
```

```toml
# prod.config.toml: production runs on the platform
[destination.warehouse]
destination_type = "motherduck"
```

```toml
# prod.secrets.toml: production credentials
[destination.warehouse.credentials]
database = "your_database"
password = "your-service-token"
```

Supported destinations include
[MotherDuck](../../dlt-ecosystem/destinations/motherduck.md),
[BigQuery](../../dlt-ecosystem/destinations/bigquery.md),
[Snowflake](../../dlt-ecosystem/destinations/snowflake.md),
[S3](../../dlt-ecosystem/destinations/filesystem.md), and
[many more](../../dlt-ecosystem/destinations).

For the full setup (synced vs. local profiles, and read-only `access` credentials for interactive jobs) see
[Workspace setup](../pipeline-operations/workspace-setup.md) and [Profiles](../pipeline-operations/profiles.md).

## 6. Schedule the pipeline

Scheduling is declarative: add a trigger to the job and redeploy.

```py
from dlt.hub import run
from dlt.hub.run import trigger


@run.pipeline("sample_shop_pipeline", trigger=trigger.schedule("*/10 * * * *"))
def load_sample_shop():
    ...
```

```sh
uv run dlthub deploy
```

See [Triggers and scheduling](../pipeline-operations/triggers.md) for cron, intervals, and follow-ups.

## 7. Build your own pipeline with your coding agent

With the sample running, the same agent session is ready to build a pipeline for **your own** source. Ask it:

```text
Help me get started building and running a data pipeline on dltHub
```

Describe the source in plain language and the agent does the rest — it has the dltHub skills and MCP server wired in from
step 1:

- **Writes the pipeline** and registers it as a job in `__deployment__.py`.
- **Deploys and runs it** on the platform, iterating on failures.
- **Opens the dashboard** so you can explore the loaded data right away.

Note that the sample loads into the Playground destination. When you build your own pipeline you pick a real destination
of your own — see [step 5](#5-point-production-at-a-cloud-destination).

For the production-grade path (auth, incremental loading, more endpoints) see the
[dltHub AI Workbench](../ingestion/rest-api-source.md).

## Next steps

- [Convert an existing dlt pipeline](../pipeline-operations/workspace-setup.md) into a workspace and bring it onto the platform.
- Ask your coding agent to [build a pipeline](../ingestion/rest-api-source.md) for your source.
- Reshape your data with [Transformations](../transformations/index.md).
- Add [data quality checks](../data-quality/index.md) to catch issues early.
- Work through the [advanced example](platform-tutorial.md): a full starter pack with pipelines, transformations, and a dashboard.
