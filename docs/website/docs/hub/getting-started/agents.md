---
title: Getting started with agents
description: Set up a new or existing dltHub workspace for background agents and declare the job inspector agent with your coding agent
keywords: [dlthub, background agents, job inspector, ai harness, dlthub-platform, design partners, getting started]
---
# Getting started with agents

:::warning
Background agents are in public preview.
:::

Background agents are dltHub jobs that run an AI agent loop. They run unattended, for example after another job fails, and report a structured result next to the job run they acted on.

The agents ship with the [AI Harness](../ai-harness/introduction.md). The [`dlthub-platform`](../ai-harness/toolkits.md#dlthub-platform) toolkit includes `job-inspector`, a verified agent that diagnoses failed job runs.

## Prerequisites

- [uv](https://docs.astral.sh/uv/) and Python 3.12 to 3.14.
- A coding agent: Claude Code, Cursor, or Codex.
- The model name, endpoint URL, and API key for your agents.

## Set up with your coding agent

Open your coding agent in your dltHub workspace, or in an empty directory to start from scratch, and paste this prompt:

```text
Set up this directory for dltHub background agents. Use `uv run` and pass
`--non-interactive` to `dlthub` commands.

1. If there is no `.dlt/.workspace` file, run `uvx dlthub-init@latest`.
2. Add these dependencies to pyproject.toml and run `uv sync`:
   "dlt[hub]==1.30.1a0", "dlthub[mcp]", "dlthub-client>=0.28.5",
   "pydantic-ai-slim[anthropic,openai,google,mcp,spec]>=2.35.0", "aiohttp>=3.14.3"
3. Run `uv run dlthub ai toolkit install dlthub-platform --overwrite`, then
   `uv run dlthub ai status`, and fix any warnings.
4. Declare the `job-inspector` agent in `__deployment__.py` with an explicit
   trigger on the jobs I choose, never the default `job.fail:*`. Ask me which
   jobs to watch, or which pipeline to build first if there are none.

I'll configure the model key and endpoint myself. Never ask for them, put them
in a command, or write them to a file.
```

## Set the model credentials

The key is yours. dltHub operates no model endpoint and supplies no key, so every agent run bills your provider account. Agents are tested on Anthropic and Azure OpenAI.

Add the model and the endpoint URL to `.dlt/config.toml`. Every agent job in the workspace uses them:

```toml
[agent]
model = "azure:gpt-5.6-sol"
api_url = "<endpoint URL>"
api_version = "2024-12-01-preview"  # Azure endpoints only
```

A model set here overrides `model=` on an individual agent job. To give one job a different model, set it under that job's section instead. See [Which model a run uses](../agents/index.md#which-model-a-run-uses).

Set the API key yourself, in your own terminal, so your coding agent never sees it. Export it for local runs, and store it as a workspace secret for deployed runs, because the platform runner can't read your shell:

```sh
export AGENT__API_KEY=<your API key>
uv run dlthub login
uv run dlthub variable set AGENT__API_KEY --value "$AGENT__API_KEY" --secret --workspace
```

If your workspace is not connected to dltHub yet, run `uv run dlthub workspace connect` before `variable set`.

## Deploy and run

`job-inspector` starts only when a job it watches fails. When the coding agent asks which jobs to watch, pick a job that can fail, for example one that calls an external API or reads credentials.

Deploy the workspace:

```sh
uv run dlthub deploy
```

Then run the watched job, from the CLI:

```sh
uv run dlthub run <job-name>
```

Or from the Web UI: open the **Jobs** page at [app.dlthub.com](https://app.dlthub.com), select the job, and start a run.

If the run fails, `job-inspector` starts on its own. To read its diagnosis, open the inspector run:

- From the **failed run's page** in the Web UI, which links to the inspector run.
- From the **Agents** tab, which lists all agents in your workspace and their runs.

## Customize the job inspector

Override the inspector's defaults in `__deployment__.py`, for example its model, turn limit, or extra instructions:

```py notype
job_inspector = run.agent(
    "dlthub-platform:job-inspector",
    trigger=[my_job.fail],
    limits={"max_turns": 20},
    instructions="focus on the loader step",
)
```

## Build your own agent

Your coding agent writes the definition for you. The `create-background-agent` skill ships with every dltHub workspace, so you describe the agent and it produces the `AGENT.md` and the `run.agent(...)` declaration:

> Write a background agent that reports the latest status of every job in the workspace.

To grade an agent that already runs, the `evaluate-background-agent` skill writes a second agent that checks each run against the first agent's own instructions. See [Job inspector evaluator](../agents/job-inspector-eval.md).

An agent is an `AGENT.md` file. The YAML frontmatter declares the tools, access, and output, and the Markdown body is the system prompt.

The examples below define `workspace_report`, an agent that reads the status of every job in the workspace and writes a report:

```md
---
description: Reports the latest status of every job in the workspace. Read-only.
tools: [jobs]
access:
  context: [read]
output:
  type: object
  properties:
    jobs_total: { type: integer }
    failed_jobs: { type: array, items: { type: string } }
---
List every job in the workspace and read the status of its latest run. Report
how many jobs there are and which ones failed. In `summary`, write a Markdown
table with one row per job: name, last run status, and when it ran.
```

Save it as `agents/workspace_report/AGENT.md` and declare it in `__deployment__.py` by its folder:

```py notype
from dlt.hub import run
from dlt.hub.run import trigger

__all__ = [
    # ...,
    "workspace_report",
]

workspace_report = run.agent("agents/workspace_report", name="workspace_report")
```

Reach for the Python form when the schemas should come from Python types, or when code has to run around the loop. The docstring is the system prompt, the parameters are the inputs, and the return type is the output schema. See [Agent definition as a Python function](../agents/agent-definitions.md#agent-definition-as-a-python-function-advanced). The same agent, written this way:

```py notype
from typing import List

from dlt.hub import run
from dlt.hub.run import trigger


class WorkspaceReport(run.TAgentOutput):
    jobs_total: int
    failed_jobs: List[str]


@run.agent(
    tools=["jobs"],
    access={"context": ["read"]},
)
async def workspace_report(run_context: run.TJobRunContext = None) -> WorkspaceReport:
    """List every job in the workspace and read the status of its latest run.

    Report how many jobs there are and which ones failed. In `summary`, write a
    Markdown table with one row per job: name, last run status, and when it ran.
    """
    return await run_context["ai_loop"].run()
```
