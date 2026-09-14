---
title: Agents
description: Run AI agents as jobs on the dltHub platform, for example to diagnose failed job runs
keywords: [dlthub platform, agents, background agents, agent job, AGENT.md, run.agent, job inspector, pydantic-ai, claude-agent-sdk, toolkits]
---

# Agents

:::warning
This feature is in public preview
:::

An **agent job** is a dltHub job whose body is an AI agent loop. It runs unattended on a schedule, after another job fails, or when you start it from the CLI or the Web UI. It can read the workspace, query runs and logs through the dltHub MCP server, and it returns a structured result shown next to the job run it acted on.

Agent jobs are declared, run, and deployed like every other job: in `__deployment__.py`, with `dlthub local run` locally and `dlthub deploy` on the platform. [Deployments](deployments.md), [Triggers and scheduling](triggers.md), and [Job configuration](job-configuration.md) apply to them as to any job.

The worked example on this page is `job-inspector`, an agent shipped with the [`dlthub-platform`](../ai-harness/toolkits.md#dlthub-platform) toolkit. It starts when a job fails, reads the run and its logs, and reports a diagnosis. It does not change code or data.

## Three terms

| Term | What it is | Where it lives |
|------|------------|----------------|
| **Agent definition** | System prompt plus a declaration of the agent's inputs, output, tools, skills, rules, and access | `AGENT.md` file, or a decorated Python function |
| **Agent job** | Definition plus the settings for your workspace: model, limits, trigger, instructions, loop | `run.agent(...)` in `__deployment__.py` |
| **Agent run** | An execution of the job. It receives inputs and returns an output and a trace | Started by a trigger, `dlthub local run`, `dlthub run`, or the Web UI |

Toolkits ship agent definitions, and your `__deployment__.py` declares the agent jobs built on them.

## Prerequisites

1. A dltHub workspace with `dlt[hub]` installed and connected to the platform. See [Workspace setup](workspace-setup.md).
2. An **agent loop** installed locally. dlt ships two; the default is `pydantic-ai`:

   ```sh
   uv add "pydantic-ai-slim[anthropic,openai,google,mcp,spec]"   # pydantic-ai loop (default)
   uv add claude-agent-sdk                                        # claude-agent-sdk loop
   ```

   On the platform the runner installs the loop a job needs by itself. See [Loops](#loops).
3. Credentials for a model provider. Locally, the provider's usual environment variable works (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, ...). See [Model and credentials](#model-and-credentials) for the configuration keys.
4. For the quick start below, the `dlthub-platform` toolkit:

   ```sh
   uv run dlthub ai toolkit install dlthub-platform
   ```

## Quick start: inspect failed jobs

Declare the `job-inspector` agent as a job in `__deployment__.py` and point its trigger at the jobs you want watched:

```py notype
"""GitHub ingest workspace with a failure inspector."""
from dlt.hub import run
from github_pipeline import load_commits

inspector = run.agent(
    "dlthub-platform:job-inspector",
    trigger="job.fail:tag:ingest",
)

__all__ = ["load_commits", "inspector"]
```

The job is named after the agent definition, `job_inspector`. Run it locally against a run that already failed, then deploy:

```sh
# one agent run, by hand, on a failed run id from `dlthub job runs list`
dlthub local run job_inspector -c failed_run_id=89826ee6-ed5d-478e-8ad1-415251f225e0

# push the job graph; every failed job tagged "ingest" now starts an inspector run
dlthub deploy
```

The transcript streams to your terminal. When the run ends you get a job result with a `status`, a markdown `summary`, and the inspector's own fields (`classification`, `confidence`, `evidence`). On the platform the result appears on the failed run's page, because the agent reported that run as the entity it acted on.

## Write an agent definition

An agent definition is a system prompt plus a declaration of what the agent takes, returns, and may touch. Write it as an `AGENT.md` or as a decorated Python function. Both make the same agent job.

### As an `AGENT.md`

A toolkit installs its agents to `.claude/dlthub/agents/<name>/AGENT.md` (`.cursor/dlthub/agents/` or `.agents/dlthub/agents/` for the other hosts). Coding agents do not scan this folder for their own subagents. You can also keep an `AGENT.md` in any folder of the workspace and refer to it by its path.

The YAML frontmatter holds the declarations and the markdown body is the system prompt. Only the body is required. A file with no frontmatter is a working agent named after its folder.

```md
---
name: job-inspector
description: Inspects a failed job run and reports a diagnosis. Read-only.
tools: [telemetry]                          # dlthub MCP feature groups
skills: [dlthub-platform:debug-deployment]  # loaded natively or inlined
rules:  [dlthub-platform:job-resources]     # always inlined into the prompt

access:                                     # what the job may touch
  local:   [read]                           # read | write | execute | network
  data:    [read]                           # read | write, governed by the profile
  context: [read]                           # telemetry, runs, job definitions

inputs:
  type: object
  properties:
    failed_run_id:
      type: string
      description: run id of the job that failed
      entity_type: job-run                  # this input names a workspace entity
    failed_job_ref:
      type: string
      description: job ref of the failed job
      entity_type: job
  required: {}

output:
  type: object
  properties:
    classification: { enum: [config, credentials, code, upstream_data, unknown] }
    confidence:     { enum: [high, medium, low] }
    evidence:       { type: array, items: { type: string } }
  required: [classification, confidence]

defaults:                                   # the job and the run may override all of these
  trigger: [job.fail:*]
  model: sonnet
  limits: { max_turns: 30, max_tokens: 1000000 }
  loop_run_args: { retries: 1 }
---
You are a job inspector for a dltHub Platform workspace. You run unattended, seconds
after a job failed. Explain the failure; do not repair it.

Investigate job_ref '{{ failed_job_ref }}' from trigger `{{ run_context.trigger }}`
with failed run id '{{ failed_run_id }}'. Any of the three may be empty.

1. With a run id, read the run and its logs.
2. Without one, resolve it from the job ref or the trigger.
3. With neither, you have nothing to inspect: return `status: aborted` naming what was
   missing.
```

| Field | Meaning |
|-------|---------|
| `name` | Folder name. Optional |
| `description` | What the agent does and when to run it. Shown in the Web UI |
| `tools` | Feature groups of the dltHub MCP server the agent gets |
| `skills`, `rules` | `<toolkit>:<name>` references to components the agent uses |
| `access` | What the agent may touch: `local`, `data`, `context` |
| `inputs` | JSON Schema of the inputs. Each input is a job configuration key |
| `output` | JSON Schema of the output. `status` and `summary` are part of it on every agent |
| `defaults` | Settings the agent job and the run may override: `trigger`, `model`, `limits`, `loop_run_args` |
| body | System prompt, a template over `inputs` |

#### Inputs

`inputs` is a JSON Schema. Every property becomes a configuration key of the job, so you can set it with `-c failed_run_id=...` on the command line, under `[jobs.<section>.<job>]` in `config.toml` or the environment, or through a run argument the trigger carries. Values arrive typed: `-c depth=3` is an `int` when the schema says so.

The body refers to inputs as `{{ name }}`. It can also refer to the run itself: `{{ run_context.trigger }}`, `{{ run_context.run_id }}`, `{{ run_context.refresh }}`, and on a job with an interval `{{ run_context.interval_start }}` and `{{ run_context.interval_end }}`. Placeholders are rendered before the first turn.

- A required input with no value fails the run like any missing job argument.
- An optional input nobody supplied renders as empty text. The body must say what to do then, and when to abort.
- An input the body never mentions produces a warning when the manifest is generated.
- `inputs.prompt` is refused. Put the task in the body.

#### Entities

An input that names a workspace object carries `entity_type`: `job-run`, `job`, `pipeline`, `dataset`, or `workspace`. The agent receives the bare id (a run id, a job ref, a pipeline name). Declaring the type does two things:

1. The run reports the entity in its result, so the run shows up on that entity's page in the Web UI.
2. The first entity-typed input is how the Web UI offers the agent from an entity. On a failed run's row, every agent job with a `job-run` input is offered, and choosing one starts a run with that run id filled in.

Declare the same name with `entity_type` on an **output** property when the agent may end up acting on a different entity than it was given. An output overwrites the input of the same name, so an inspector that resolved a run from a job ref reports the run it actually inspected.

#### Output

`output` is a JSON Schema of what the agent returns. Two properties are part of every agent's output and dlt adds them when the definition leaves them out:

| Property | Meaning |
|----------|---------|
| `status` | `succeeded` or `failed`, as your prompt defines them, or `aborted` when the task could not be done at all |
| `summary` | Markdown. What the agent accomplished. For `aborted` it becomes the text of the exception that fails the run |

Add the agent's own fields next to them. The model sees the whole schema, descriptions and enums included, so describe every field whose name does not say it all. Structured output guarantees the shape of the answer, so the body must define what each value means and when to pick it. Anthropic's structured output rejects `minimum`, `maximum`, and `minLength`, so put numeric bounds in the description.

#### Access

`access` says what the agent may touch, per axis, as a verb or a list of verbs. Without `access` the agent gets no file tools or shell, and its MCP server serves only the toolkit catalogue.

| Axis | Verbs | What it buys |
|------|-------|--------------|
| `local` | `read` | `Read`, `Glob`, `Grep` on the workspace files |
| | `write` | `Write`, `Edit` |
| | `execute` | `Bash` (`PowerShell` on Windows) and `RunPython`, in the workspace, in the job's own process |
| | `network` | `WebFetch`, `WebSearch` |
| `data` | `read`, `write` | Workspace data through the MCP server, enforced by the [profile](profiles.md) the run uses (`read` maps to `access`, `write` to `prod`) |
| `context` | `read` | Runs, logs, job definitions, and telemetry through the MCP server |

`all` is shorthand for every verb on an axis. `local` maps to the same tool set on both loops, under the names Claude Code uses. Two limits hold whatever you declare: credential files (`*secrets.toml`, `.env`) are never readable by a file tool, and SQL queries through the MCP server are read-only.

If a loop has no tool for a granted verb, the run proceeds with the tools it has. The trace of each run lists the tools that were wired.

#### Tools, skills, and rules

`tools` lists feature groups of the dltHub MCP server: `workspace`, `pipeline`, `toolkit`, `secrets`, `context`, and on the platform `jobs`, `logs`, `telemetry`. The agent gets exactly the groups listed, and within a group only the tools its `access` covers. Without `tools` no server is started.

`skills` and `rules` reference components of an installed toolkit as `<toolkit>:<name>`, or a workspace-relative path such as `.claude/skills/my-skill/SKILL.md`. Rules are inlined into the system prompt on both loops. On `claude-agent-sdk` skills are listed by name and loaded when the agent invokes one, as in Claude Code; on `pydantic-ai` their text is inlined. The agent gets only the listed components; other skills and rules installed in the workspace, including the `.claude/rules` folder, are not loaded. A reference that does not resolve is skipped with a warning.

#### Defaults

`defaults` holds the values that apply when the agent job and the run's configuration do not override them.

```yaml
defaults:
  trigger: [job.fail:*]                 # trigger strings, selectors allowed
  model: sonnet                         # alias, or provider:model
  limits: {max_turns: 30, max_tokens: 1000000}
  loop_run_args: {retries: 1}           # handed to the framework
```

#### Writing the body

The body is the system prompt. Write it as you would a skill, for a reader with the tools but without the context. Platform knowledge belongs in the referenced rules and skills. Keep the body under about two hundred lines and cover these points:

1. **State the role in two sentences**, including that the agent runs unattended.
2. **Define `succeeded`, `failed`, and `aborted` for this agent.** The schema does not. Under structured output the model tends to report `succeeded` when unsure, so say what counts as a failure.
3. **Say what to do with each input, and with its absence.** Name the fallbacks in order and the point at which the answer is `aborted`.
4. **Give the first steps concretely.** Which tool to call first, what to read, what to look for.
5. **Write constraints as rules**, for example "Never edit code, never deploy, never re-run a job".
6. **Define every enum the output declares.** Say what `unknown` or `low` means and that reporting it is a legitimate outcome.

The model also receives the rules, the skills, the output schema, the workspace and temp folder paths, and the tools, so the body does not need to repeat them. The user turn of each run is the job's `instructions`, or "Go ahead" when none are set.

### As a Python function

A decorated function does not need an `AGENT.md` or a toolkit. Its docstring is the system prompt, its parameters are the inputs, and its return type is the output. The decorator arguments are the agent job's settings, and the body drives the loop it finds in `run_context["ai_loop"]`.

```py notype
from typing import Annotated, List, Literal

import dlt
from dlt.common.typing import NotRequired
from dlt.hub import run


class CrashReport(run.TAgentOutput):
    """`status` and `summary` come from the base."""
    classification: Annotated[
        Literal["config", "credentials", "code", "upstream_data", "unknown"],
        run.Doc("What kind of failure it was"),
    ]
    confidence: Literal["high", "medium", "low"]
    evidence: NotRequired[List[str]]


@run.agent(
    access={"local": ["read"], "data": ["read"], "context": ["read"]},
    tools=["telemetry"],
    skills=["dlthub-platform:debug-deployment"],
    trigger="job.fail:*",
    limits={"max_turns": 30},
)
async def crash_inspector(
    failed_run_id: Annotated[str, run.Entity("job-run")] = dlt.config.value,
    depth: int = 2,
    run_context: run.TJobRunContext = None,
) -> CrashReport:
    """Inspects a failed job run and reports a diagnosis.

    You run unattended, seconds after a job failed. Explain the failure; do not repair it.
    Investigate run '{{ failed_run_id }}' from `{{ run_context.trigger }}`, going
    {{ depth }} runs back. Return `aborted` when the inputs give you nothing to inspect.
    """
    loop = run_context["ai_loop"]
    report = await loop.run(inputs={"failed_run_id": failed_run_id, "depth": depth})
    if loop.trace["total_tokens"] > 800_000:
        report["summary"] += "\n\n_Investigation was expensive; consider narrowing the trigger._"
    return report
```

| In Python | In the agent definition |
|-----------|-------------------------|
| Function name | `name` of the agent job |
| Docstring | System prompt, placeholders included. Its first line is the `description` |
| Parameters | `inputs`, and so the job's configuration: `-c failed_run_id=...` fills them, typed |
| `Annotated[str, run.Entity("job-run")]` | Entity-typed input |
| `dlt.config.value` default | Required input |
| `run_context` parameter | Passed by the launcher, not declared as an input |
| Return type deriving from `run.TAgentOutput` | `output`. `run.Doc(...)` on a field is its description |
| `access=`, `tools=`, `skills=`, `rules=` | Matching `AGENT.md` fields |
| `model=`, `limits=`, `loop_run_args=`, `instructions=`, `trigger=`, `loop=` | Agent job settings, `defaults` in an `AGENT.md` |

The schemas come from pydantic, so `Optional`, `Literal`, `List`, nested models, and `NotRequired` behave as they do everywhere else. The function may be `def` or `async def`. Most functions return the loop's output as is. The body above shows the function can also inspect `loop.trace`, run the loop twice, or skip it.

A function can also drive an installed agent definition. Pass it as `agent=`; the decorator arguments and the function override its fields:

```py notype
@run.agent(agent="dlthub-platform:job-inspector", loop="claude-agent-sdk")
async def inspect(run_context: run.TJobRunContext = None) -> run.TAgentOutput:
    return await run_context["ai_loop"].run()
```

## Declare the agent job

`run.agent(...)` turns a definition into a job. The first argument is the agent: a `<toolkit>:<agent>` reference to an installed one, a workspace-relative path to a folder holding an `AGENT.md`, or a `TAgentSpec` dict declared inline.

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
| `instructions` | First user message of each run. Use it for the task at hand; the system prompt describes the agent |
| `model` | `provider:model` id such as `anthropic:claude-sonnet-5`, or an alias. See [Model and credentials](#model-and-credentials) |
| `limits` | `max_turns` and `max_tokens` per run. The loop ends the run when either is spent |
| `loop` | `"pydantic-ai"` (default) or `"claude-agent-sdk"`. See [Loops](#loops) |
| `loop_run_args` | Arguments handed to the framework, merged over the definition's defaults. `retries` is how often pydantic-ai lets the model correct a failing tool call |
| `verbosity` | How much of the run is printed: `0` the outcome and tool names, `1` (default) adds the agent's thoughts and tool arguments, `2` adds the rendered system prompt |
| `inputs_validator` | Called with the resolved inputs before the run. Its return value is merged into them. Use it to derive an input, for example a run id from a job ref |
| `outputs_validator` | Called with the agent's output after the run. Its return value replaces the output |
| `name`, `section` | Job name and configuration section, as on every job |
| `trigger`, `execute`, `expose`, `require`, `spec` | The usual job options. See [Triggers and scheduling](triggers.md) and [Job configuration](job-configuration.md) |

### Triggers for agents

Agent jobs take every trigger other jobs take. Two string triggers exist for reacting to job outcomes across the workspace. They accept a job ref or any selector `dlthub job trigger` accepts:

```
job.fail:tag:ingest          every job tagged `ingest`
job.fail:batch:              every batch job
job.fail:jobs.mod.*          every job in section `mod`
job.fail:*                   every job
job.success:jobs.mod.load    one job, on success
```

A selector expands at deploy time to a follow-up trigger per matching job. The declaring job itself and interactive jobs are excluded. A run started by hand arrives with a `manual:` trigger and only the inputs it was given, so the body must say what to do with empty input.

## Run an agent

Each trigger of the agent job produces an agent run. You start one by hand the way you run any job, locally or on the platform:

```sh
dlthub local run job_inspector -c failed_run_id=89826ee6-...     # locally
dlthub run job_inspector -c failed_run_id=89826ee6-... -f        # on the platform
```

Settings can be overridden for that run alone. Inputs and agent settings are ordinary job configuration under the job's section, so the same keys work on the command line, in `config.toml`, in the environment, and in the Web UI's run dialog:

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

Precedence, lowest first: loop default, the definition's `defaults`, the `run.agent` argument, configuration.

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

Credentials for the provider go under the job's `agent` section, in `secrets.toml` or the environment. Without them, the provider's own environment variable is used (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, ...):

```toml
# .dlt/secrets.toml
[jobs.__deployment__.job_inspector.agent]
api_key = "sk-ant-..."
# api_url = "https://my-proxy.example.com"   # a proxy or a private deployment
# api_version = "2024-10-21"                 # Azure only
```

On the platform the runtime can supply a model endpoint of its own. If you set any of `model`, `api_key`, `api_url`, or `api_version`, the run takes all four from your configuration and ignores the runtime's endpoint. The run logs which endpoint it used.

## Read the result

The agent run prints its transcript as it goes: what the model thinks, says, and calls, what each tool handed back, and a finish line with the tools, skills, and MCP tools it used. `agent.verbosity` controls how much of it you see; `NO_COLOR` turns colors off. When the run ends, the launcher prints and delivers the job result:

```json
{
  "type": "job.background_agent.dlthub-platform:job-inspector",
  "job_ref": "jobs.__deployment__.job_inspector",
  "status": "succeeded",
  "summary": "## Root cause: platform runner token conflict ...",
  "result": {
    "status": "succeeded",
    "summary": "...",
    "classification": "config",
    "confidence": "high",
    "evidence": ["..."]
  },
  "object": [
    { "type": "job-run", "id": "job-run/89826ee6-ed5d-478e-8ad1-415251f225e0" },
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

- **`status`** and **`summary`** are copied from the agent output to the top level. `succeeded` and `failed` mean what the prompt defines. `aborted` means the task could not be done at all: the result is delivered, then the run fails with an exception carrying `summary`.
- **`result`** is the agent output as the definition's `output` schema declares it.
- **`object`** lists the entities the run acted on. On the platform, the result appears on each of their pages.
- **`trace`** records the model, limits, resolved inputs, the tools that were wired, skills and MCP tools used, turn and token counts, and per-turn tool calls.

On the platform, inspect agent runs like any other run with `dlthub job runs list` and `dlthub job runs info`. See [Monitoring and debugging](monitoring.md).

## Loops

A loop is the framework that runs the agent. dlt ships two and adds the matching dependency group to the job, so the runner installs it. A third-party loop can register through the `plug_agent_loop` plugin hook.

| | `pydantic-ai` (default) | `claude-agent-sdk` |
|--|-------------------------|--------------------|
| Models | Any provider pydantic-ai supports | Anthropic models, through a bundled Claude Code CLI |
| Local tools | dlt's own file, search, shell, and web tools | Claude Code's tools, under the same names |
| Skills | Inlined into the system prompt | Listed by name and loaded on demand, as in Claude Code |
| Install locally | `uv add "pydantic-ai-slim[anthropic,openai,google,mcp,spec]"` | `uv add claude-agent-sdk` |

On both loops `access` selects the local tools, `tools` the MCP server features, the rendered body is the system prompt, `instructions` the user turn, and `output` the structured output schema. dlt counts `limits.max_tokens` after each turn, so the limit means the same on both loops.

Pick the loop on the job with `loop="claude-agent-sdk"`, or for a single run with `-c agent.loop=claude-agent-sdk`. On `claude-agent-sdk` the workspace's `CLAUDE.md` loads as in any Claude Code session. The project's `.claude/rules` and `.mcp.json` are not loaded; the agent gets the rules and the MCP server it declares.

## What an agent can and cannot do

- **Tools follow `access`.** An agent without `access` gets no file tools and no shell. MCP tools declare the access they require, and a tool the grant does not cover is not offered to the model.
- **Credential files are never readable** by a file tool: `*secrets.toml`, `.env`, `.env.*`, on both loops, whatever `local` grants.
- **SQL through the MCP server is read-only**, a single statement per call.
- **Data access follows the profile.** `data: read` runs on the `access` profile, `data: write` on `prod`. See [Profiles](profiles.md).
- **`execute` is not a sandbox.** A shell runs in the same process tree and virtual environment as the job, with the job's permissions on the runner. Grant it only to agents that need it.

## Next steps

- [Toolkits](../ai-harness/toolkits.md) shows the `dlthub-platform` toolkit that ships `job-inspector`
- [Triggers and scheduling](triggers.md) covers the schedule, interval, and follow-up triggers available to all jobs
- [Job configuration](job-configuration.md) covers `execute`, `require`, `expose`, and TOML sections
- [Monitoring and debugging](monitoring.md) shows how to list runs and read their results
