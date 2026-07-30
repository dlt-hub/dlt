---
title: Deploy your first pipeline with dltHub
description: Scaffold a fresh dltHub workspace and let your coding agent deploy a pipeline to the managed platform in a few minutes
keywords: [dlthub, deploy, first pipeline, getting started, workspace, dlthub-start, onboarding]
---

# Deploy your first pipeline with dltHub

Go from nothing to a pipeline running on the managed dltHub platform. `dlthub-start` scaffolds a workspace, signs you in,
and installs the AI workbench for your coding agent — **the agent then deploys and runs your first pipeline for you**.

Already have a `dlt` pipeline? See [Workspace setup](../pipeline-operations/workspace-setup.md) to convert an existing
project into a dltHub workspace.

## Before you start

- **A supported Python version** (3.10–3.14) and [uv](https://docs.astral.sh/uv/) (recommended). `dlthub-start`
  provisions the Python version and virtual environment for the workspace itself. See
  [installation](installation.md) for alternative install paths.
- **A coding agent** installed and available on your `PATH`: Claude Code, Cursor, or Codex. The workbench files and MCP
  server are wired into whichever one you pick.
- **A dltHub account.** You don't need to sign up first. `dlthub-start` signs you in through a browser-based OAuth flow
  (GitHub, Google, or email) and creates your account on first login. You can explore everything afterwards in the web UI
  at [app.dlthub.com](https://app.dlthub.com).
- dltHub is a commercial platform; use is governed by the [dltHub License](../license.md). On sign-up you automatically get
  a personal [Playground workspace](playground-workspace.md), so you can complete this guide end-to-end with nothing to set
  up and no cloud credentials of your own.

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

`pipeline.py` loads the **Sample Shop** dataset from a public sample REST API into the platform-managed
**Playground destination**, with no warehouse, bucket, or credentials to configure. It's already a deployable job:

- decorated with `@run.pipeline`;
- listed in `__deployment__.py`.

See [Deployments](../pipeline-operations/deployments.md) for how the manifest works.

## 3. Let the agent deploy and run the sample

The handed-off prompt points the agent at the `deploy-run-sample-pipeline` skill, which walks a short checklist:
deploy the workspace, run the sample pipeline on the platform, then open the dataset browser. It runs ordinary CLI
commands, so you can watch — or run — every step yourself.

First it deploys the workspace, which uploads your files and registers every job in `__deployment__.py`:

```sh
uv run dlthub deploy
```

```text
Synced changed workspace files
Synced changed workspace configuration
3 job(s) found in __deployment__
```

The scaffold ships three jobs: `load_sample_shop` (the pipeline), `onboarding_success` (a notebook that browses the
loaded data), and `dashboard`.

Then it runs the pipeline on the platform and follows the logs until it finishes:

```sh
uv run dlthub run load_sample_shop -f
```

One load package lands in the `sample_shop` dataset on the
[Playground destination](../ingestion/playground.md) — zero-config storage the platform provisions for you. The agent
prints a link to the run in the web UI:

```text
https://app.dlthub.com/w/<workspace-id>/runs/<run-id>
```

Finally it opens the `onboarding_success` notebook so you can browse the loaded tables right away. At that point
onboarding is complete: a pipeline has run on the platform and its data is queryable.

## 4. Re-run and inspect

The `dlthub` CLI has two scopes: unqualified **`dlthub …`** operates on the connected cloud workspace, and
**`dlthub local …`** runs on your machine using the local `dev` profile.

```sh
uv run dlthub run load_sample_shop -f      # run on the platform, follow logs until it finishes
uv run dlthub show                         # open the workspace overview in the browser
```

## 5. Point production at a cloud destination

The Playground destination is ideal for testing but isn't meant for production data. To load real data, point your
pipeline at a destination you own. The recommended pattern is a **named destination** resolved per
[profile](../pipeline-operations/profiles.md), so the *same* pipeline code runs on local DuckDB during development
(`dev`) and on your cloud warehouse in production (`prod`).

Set your pipeline's destination to a neutral alias such as `warehouse`:

```py
import dlt
from dlt.hub import run


@run.pipeline("sample_shop_pipeline")
def load_sample_shop():
    pipeline = dlt.pipeline(
        pipeline_name="sample_shop_pipeline",
        destination="warehouse",   # named alias, resolved per profile
        dataset_name="sample_shop",
    )
    ...
```

Then configure the alias per profile in `.dlt/` (secrets files are gitignored):

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
