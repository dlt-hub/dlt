---
title: Background agents
description: Run background agents as jobs on the dltHub platform, for example to diagnose failed job runs
keywords: [dlthub platform, agents, background agents, agent job, AGENT.md, run.agent, job inspector, pydantic-ai, claude-agent-sdk, toolkits]
---

# Background agents

:::warning
This feature is in private preview
:::

An agent job is a dltHub job that runs an AI agent loop. It runs unattended on a schedule, after another job fails, or when you start it from the CLI or the Web UI. It can read the workspace, query runs and logs through the dltHub Model Context Protocol (MCP) server, and it returns a structured result shown next to the job run it acted on.

Agent jobs are declared, run, and deployed like every other job: in `__deployment__.py`, with `dlthub local run` locally and `dlthub deploy` on the platform. What's described in [Deployments](../pipeline-operations/deployments.md), [Triggers and scheduling](../pipeline-operations/triggers.md), and [Job configuration](../pipeline-operations/job-configuration.md) applies to agent jobs as well.

This page covers declaring an agent as a job and running it locally and on the platform. [Agent definitions](agent-definitions.md) covers the definition itself. The examples use `job-inspector`, the agent the [`dlthub-platform`](../ai-harness/toolkits.md#dlthub-platform) toolkit ships. See [Job inspector agent](job-inspector.md) for what it does and how to declare it.

## Terms

| Term | Definition | Where it lives |
|------|------------|----------------|
| Agent definition | System prompt plus a declaration of the agent's inputs, output, tools, skills, rules, and access | `AGENT.md` file, or a decorated Python function |
| Agent loop | Framework that runs the model turn by turn: `pydantic-ai` (default) or `claude-agent-sdk` | Selected with `loop=` on `run.agent` or `agent.loop` in configuration |
| Agent job | Definition plus the settings for your workspace: model, limits, trigger, instructions, loop | `run.agent(...)` in `__deployment__.py` |
| Agent run | Execution of the agent job. It receives inputs and returns an output and a trace | Started by a trigger, `dlthub local run`, `dlthub run`, or the Web UI |
| Access axis | Area of the workspace that `access` covers: `local` for the files and the shell, `data` for the data in your destinations, `context` for runs, logs, job definitions, and telemetry | Key of `access` in the agent definition |
| Verb | What the agent may do on an axis: `read`, `write`, `execute`, `network` | Listed under the axis in `access` |

The [dltHub AI harness](../ai-harness/introduction.md) ships verified agent definitions in its toolkits. Installing a toolkit copies the `AGENT.md` into your workspace, where you can adapt it. Your `__deployment__.py` declares the agent jobs built on these definitions, and `dlthub deploy` ships the definitions with the rest of the workspace.

## Prerequisites

1. A dltHub workspace with `dlt[hub]` installed and connected to the platform. See [Workspace setup](../pipeline-operations/workspace-setup.md).
2. An agent loop installed locally. dltHub ships two agent loops, `pydantic-ai` (default) and `claude-agent-sdk`:

   ```sh
   uv add "pydantic-ai-slim[anthropic,openai,google,mcp,spec]"   # pydantic-ai loop (default)
   uv add claude-agent-sdk                                        # claude-agent-sdk loop
   ```

   You install a loop for local runs. On deploy, each agent job declares the dependency group of its
   loop, and the platform runner installs that group before the run starts. A job that uses
   `claude-agent-sdk` runs on the platform even when your machine has only `pydantic-ai`. See
   [Agent loops](#agent-loops).
3. Credentials for a model provider. Locally, the provider's default environment variables work (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, and so on). See [Model and credentials](#model-and-credentials) for the configuration keys.

## Declare the agent job

`run.agent(...)` declares a job from a definition. The first argument is the agent: a `<toolkit>:<agent>` reference to an installed one, a workspace-relative path to a folder holding an `AGENT.md`, or a `TAgentSpec` dict declared inline.

```py notype
from dlt.hub import run

inspector = run.agent(
    "dlthub-platform:job-inspector",
    trigger="job.fail:tag:ingest",
    model="opus",
    limits={"max_turns": 20},
    instructions="focus on the loader step",
)
```

The job is named after the agent definition (`job-inspector` becomes `job_inspector`) in the declaring module's section. Every argument overrides the matching entry of the definition's `defaults`.

| Argument | Meaning |
|----------|---------|
| `instructions` | First user message of each run. Use it for the task at hand. The system prompt describes the agent |
| `model` | `provider:model` id such as `anthropic:claude-sonnet-5`, or an alias. See [Model and credentials](#model-and-credentials) |
| `limits` | `max_turns` and `max_tokens` per run. The loop ends the run when either is exhausted |
| `loop` | `"pydantic-ai"` (default) or `"claude-agent-sdk"`. See [Agent loops](#agent-loops) |
| `loop_run_args` | Arguments passed to the framework, merged over the definition's defaults. `retries` sets how many times pydantic-ai allows the model to correct a failing tool call |
| `verbosity` | How much of the run is printed: `0` the outcome and tool names, `1` (default) adds the agent's thoughts and tool arguments, `2` adds the rendered system prompt |
| `inputs_validator` | Called with the resolved inputs before the run. Its return value is merged into them. Use it to derive an input, for example a run id from a job ref |
| `outputs_validator` | Called with the agent's output after the run. Its return value replaces the output |
| `name`, `section` | Job name and configuration section, as on every job |
| `trigger`, `execute`, `expose`, `require`, `spec` | Standard job options. See [Triggers and scheduling](../pipeline-operations/triggers.md) and [Job configuration](../pipeline-operations/job-configuration.md) |

### Triggers for agents

Agent jobs take every trigger other jobs take. Two string triggers exist for reacting to job outcomes across the workspace. They accept a job ref or any selector `dlthub job trigger` accepts:

```
job.fail:tag:ingest          every job tagged `ingest`
job.fail:batch:              every batch job
job.fail:jobs.mod.*          every job in section `mod`
job.fail:*                   every job
job.success:jobs.mod.load    one job, on success
```

A selector expands at deploy time to a follow-up trigger per matching job. The declaring job itself and interactive jobs are excluded. A run started manually arrives with a `manual:` trigger and only the inputs it was given, so the body must say what to do with empty input.

## Run an agent job

Each trigger of the agent job produces an agent run. You can also start a run manually, the way you run any job locally or on the platform:

```sh
dlthub local run job_inspector -c failed_run_id=<run-id>     # locally
dlthub run job_inspector -c failed_run_id=<run-id> -f        # on the platform
```

You can override settings for a single run. Inputs and agent settings are ordinary job configuration under the job's section, so the same keys work on the command line, in `config.toml`, in the environment, and in the Web UI's run dialog:

| What | Key | Example |
|------|-----|---------|
| Declared inputs | `jobs.<section>.<job>.<input>` | `-c failed_run_id=...` |
| Instructions, model, limits, loop, verbosity | `jobs.<section>.<job>.agent.*` | `-c agent.instructions="explain, do not fix"`, `-c agent.max_turns=10`, `-c agent.verbosity=2` |

```toml
# .dlt/config.toml
[jobs.__deployment__.job_inspector]
failed_job_ref = "jobs.github_pipeline.load_commits"

[jobs.__deployment__.job_inspector.agent]
model = "opus"
max_turns = 20
verbosity = 0
```

Each source overrides the ones before it: the loop default, the definition's `defaults`, the `run.agent` argument, the run's configuration.

### Model and credentials

`model` is a `provider:model` id in the naming pydantic-ai uses, or one of these aliases:

| Alias | Model |
|-------|-------|
| `sonnet` (default) | `anthropic:claude-sonnet-5` |
| `opus` | `anthropic:claude-opus-5` |
| `haiku` | `anthropic:claude-haiku-4-5` |
| `fable` | `anthropic:claude-fable-5` |
| `gpt`, `gpt-mini`, `gpt-nano` | `openai:gpt-5.5`, `openai:gpt-5.4-mini`, `openai:gpt-5.4-nano` |
| `gemini`, `gemini-pro` | `google:gemini-3.5-flash`, `google:gemini-3.1-pro-preview` |

The `claude-agent-sdk` loop runs Anthropic models only.

Credentials for the provider go under the job's `agent` section, in `secrets.toml` or the environment. Without them, the provider's default environment variables are used (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, and so on):

```toml
# .dlt/secrets.toml
[jobs.__deployment__.job_inspector.agent]
api_key = "sk-ant-..."
# api_url = "https://my-proxy.example.com"   # a proxy or a private deployment
# api_version = "2024-10-21"                 # Azure only
```

On the platform the runtime can supply a model endpoint of its own. If you set any of `model`, `api_key`, `api_url`, or `api_version`, the run takes all four from your configuration and ignores the runtime's endpoint. The run logs which endpoint it used.

### Deploy the agent job

`dlthub deploy` ships the job graph and the agent definitions with the rest of the workspace:

```sh
dlthub deploy
```

A selector trigger expands at deploy time, so a `job.fail:` agent job starts watching the jobs it matches as soon as the deployment lands. See [Deployments](../pipeline-operations/deployments.md).

## Read the agent run result

The agent run prints its transcript as it goes: the model's reasoning, its messages, the tool calls it makes, what each tool returned, and a closing line listing the tools, skills, and MCP tools used. `agent.verbosity` controls how much of it you see. `NO_COLOR` turns colors off. When the run ends, the launcher prints and delivers the job result:

```json
{
  "type": "job.background_agent.dlthub-platform:job-inspector",
  "job_ref": "jobs.__deployment__.job_inspector",
  "status": "succeeded",
  "summary": "## Root cause: platform runner token conflict ...",
  "result": {
    "status": "succeeded",
    "summary": "...",
    "failed_run_id": "<run-id>",
    "failed_job_ref": "jobs.github_pipeline.load_commits",
    "classification": "config",
    "confidence": "high",
    "evidence": [
      { "source": "dlthub job runs logs <run-id> line 38", "excerpt": "..." }
    ],
    "proposed_fix": "...",
    "requires_human": true
  },
  "object": [
    { "type": "job-run", "id": "job-run/<run-id>" },
    { "type": "job", "id": "job/jobs.github_pipeline.load_commits" }
  ],
  "trace": {
    "loop_type": "pydantic-ai",
    "model": "anthropic:claude-sonnet-5",
    "turn_count": 3,
    "total_tokens": 13215
  }
}
```

- `status` and `summary` are copied from the agent output to the top level. `succeeded` and `failed` mean what the prompt defines. `aborted` means the task couldn't be done at all: the result is delivered, then the run fails with an exception carrying `summary`.
- `result` is the agent output as the definition's `output` schema declares it.
- `object` lists the entities the run acted on. On the platform, the result appears on each of their pages.
- `trace` records the model, limits, resolved inputs, the tools that were wired, skills and MCP tools used, turn and token counts, and per-turn tool calls.

On the platform, inspect agent runs like any other run with `dlthub job runs list` and `dlthub job runs info`. See [Monitoring and debugging](../pipeline-operations/monitoring.md).

## Agent loops

A loop is the framework that runs the agent. dltHub ships two agent loops and adds the matching dependency group to the job, so the runner installs it. A third-party loop can register through the `plug_agent_loop` plugin hook.

| | `pydantic-ai` (default) | `claude-agent-sdk` |
|--|-------------------------|--------------------|
| Models | Any provider pydantic-ai supports | Anthropic models, through a bundled Claude Code CLI |
| Local tools | dlt's own file, search, shell, and web tools | Claude Code's tools, under the same names |
| Skills | Inlined into the system prompt | Listed by name and loaded on demand, as in Claude Code |
| Install locally | `uv add "pydantic-ai-slim[anthropic,openai,google,mcp,spec]"` | `uv add claude-agent-sdk` |

Both loops read the same declarations. `access` selects the local tools and `tools` selects the MCP server features. The rendered body becomes the system prompt, `instructions` becomes the user turn, and `output` becomes the structured output schema. dlt counts `limits.max_tokens` after each turn, so the limit means the same on both loops.

Select the loop on the job with `loop="claude-agent-sdk"`, or for a single run with `-c agent.loop=claude-agent-sdk`. On `claude-agent-sdk` the workspace's `CLAUDE.md` loads as in any Claude Code session. The project's `.claude/rules` and `.mcp.json` aren't loaded. The agent receives the rules and the MCP server it declares.

## Guardrails

- Tools follow the access profile. An agent without `access` receives no file tools and no shell. MCP tools declare the access they require, and a tool the grant doesn't cover isn't offered to the model.
- Credential files are never readable by a file tool: `*secrets.toml`, `.env`, `.env.*`, on both loops, whatever `local` grants.
- SQL through the MCP server is limited to a single `SELECT` statement per call.
- `execute` runs in the job's own process. A shell runs in the same process tree and virtual environment as the job, with the job's credentials on the runner. Grant it only to agents that need it, and give an agent with `execute` and data access an explicit rule never to write data.

## Next steps

- [Agent definitions](agent-definitions.md) covers the `AGENT.md` and Python function forms
- [Job inspector agent](job-inspector.md) is the verified agent that diagnoses failed job runs
- [Toolkits](../ai-harness/toolkits.md) shows the `dlthub-platform` toolkit that ships `job-inspector`
- [Triggers and scheduling](../pipeline-operations/triggers.md) covers the schedule, interval, and follow-up triggers available to all jobs
- [Job configuration](../pipeline-operations/job-configuration.md) covers `execute`, `require`, `expose`, and TOML sections
- [Monitoring and debugging](../pipeline-operations/monitoring.md) shows how to list runs and read their results
