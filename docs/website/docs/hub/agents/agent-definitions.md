---
title: Agent definitions
description: Write a dltHub agent definition as an AGENT.md file or as a decorated Python function
keywords: [dlthub platform, agents, AGENT.md, agent definition, inputs, output, access, tools, skills, rules, run.agent]
---
# Agent definitions

:::warning
This feature is in private preview
:::

Installed toolkits ship verified agent definitions ready to declare as jobs, such as the [job inspector agent](job-inspector.md) in the `dlthub-platform` toolkit. This page describes how to write your own, or how to adapt an installed one for your workspace.

An agent definition consists of a system prompt and a declaration of the agent's inputs, output, tools, and access. It can be written as an `AGENT.md` file or as a decorated Python function. Both forms produce the same agent job.

An `AGENT.md` suits an agent made of a prompt plus its declarations, an agent a toolkit ships, and an agent maintained by people who don't write Python. A Python function suits the cases where the schemas should come from Python types, or where code has to run around the loop to derive an input, inspect the trace, run the loop twice, or skip it. A function can also drive an installed `AGENT.md` through `agent=`, so a toolkit definition keeps the prompt while your code handles the rest.

## Agent definition in an `AGENT.md` file

`dlthub ai toolkit install` copies a toolkit's agent definitions to `.claude/dlthub/agents/<name>/AGENT.md` (`.cursor/dlthub/agents/` or `.agents/dlthub/agents/` for the other hosts). Coding agents don't scan this folder for their own subagents. You can also keep an `AGENT.md` in any folder of the workspace and refer to it by its path.

The YAML frontmatter holds the declarations and the Markdown body is the system prompt. Only the body is required. A file with no frontmatter is a working agent named after its folder. The example below is a shortened version of the `job-inspector` definition:

```md
---
name: job-inspector
description: Inspects a failed job run and reports a diagnosis with a proposed fix. Read-only.
tools: [jobs, logs, telemetry]              # feature groups of the dltHub MCP server
skills: [dlthub-platform:debug-deployment]  # loaded on demand or inlined
rules:  [dlthub-platform:job-resources]     # always inlined into the prompt

access:                                     # what the agent may read, write, or run
  local:   [read]                           # read | write | execute | network
  context: [read]                           # runs, logs, job definitions, telemetry
                                            # no `data`: the diagnosis reads metadata and source, never rows

inputs:
  type: object
  properties:
    failed_run_id:
      type: string
      description: run id of the failed job run to inspect
      entity_type: job-run                  # this input names a workspace entity
    failed_job_ref:
      type: string
      description: job ref of the failed job; its latest failed run is inspected when no run id is given
      entity_type: job
  required: {}

output:
  type: object
  properties:
    status:
      enum: [succeeded, failed, aborted]
      description: Outcome of your task, as defined in your system prompt
    summary:
      type: string
      description: Markdown. What you accomplished, or what blocked you when `status` is `aborted`
    failed_run_id:                          # the run actually inspected, reported as an entity
      type: string
      entity_type: job-run
    classification:
      enum: [config, credentials, upstream_data, code, resources, transient, unknown]
      description: The kind of failure, as defined in the "Classification" section of your system prompt
    confidence:
      enum: [high, medium, low]
      description: How well the evidence supports the classification. `low` whenever the classification is `unknown`
    evidence:
      type: array
      items:
        type: object
        properties:
          source: { type: string }          # with the line the excerpt sits on
          excerpt: { type: string }
          provenance:
            enum: [run_log, run_record, trace, job_definition, workspace_file, inference]
    proposed_fix:
      type: string
      description: What a human should do next, naming the target and the change. You never apply it
    requires_human:
      type: boolean
  required: [status, summary, classification, confidence, evidence, requires_human]

defaults:                                   # the job and the run may override all of these
  trigger: [job.fail:*]
  limits: { max_turns: 30, max_tokens: 1000000 }
  loop_run_args: { retries: 2 }
---
You are a job inspector for a dltHub Platform workspace. You run unattended, seconds
after a job failed. Explain the failure. Do not repair it.

You were given run id '{{ failed_run_id }}' and job ref '{{ failed_job_ref }}', from
trigger `{{ run_context.trigger }}`. Any of the three may be empty. Resolve them in this
order and stop at the first that works:

1. A run id: inspect that run.
2. A job ref: take its latest failed run.
3. A `job.fail:<job ref>` trigger: take the latest failed run of that job.
4. Nothing: return `status: aborted` with a `summary` naming which inputs were empty.
```

| Field             | Meaning                                                                                        |
| ----------------- | ---------------------------------------------------------------------------------------------- |
| `name`            | Folder name. Optional                                                                          |
| `description`     | What the agent does and when to run it. Shown in the Web UI                                    |
| `tools`           | Feature groups of the dltHub MCP server the agent receives                                     |
| `skills`, `rules` | `<toolkit>:<name>` references to components the agent uses                                     |
| `access`          | What the agent may read, write, run, or reach, per axis: `local`, `data`, `context`            |
| `inputs`          | JSON Schema of the inputs. Each input is a job configuration key                               |
| `output`          | JSON Schema of the output. `status` and `summary` are part of it on every agent                |
| `defaults`        | Settings the agent job and the run may override: `trigger`, `model`, `limits`, `loop_run_args` |
| body              | System prompt, a template over `inputs`                                                        |

### Input schema

`inputs` is a JSON Schema. Every property becomes a configuration key of the job, so you can set it with `-c failed_run_id=...` on the command line, under `[jobs.<section>.<job>]` in `config.toml` or the environment, or through a run argument the trigger carries. Values are typed: `-c depth=3` resolves to an `int` when the schema declares one.

The body refers to inputs as `{{ name }}`. It can also refer to the run itself: `{{ run_context.trigger }}`, `{{ run_context.run_id }}`, `{{ run_context.refresh }}`, and on a job with an interval `{{ run_context.interval_start }}` and `{{ run_context.interval_end }}`. Placeholders are rendered before the first turn.

- A required input with no value fails the run like any missing job argument.
- An optional input with no value renders as empty text. The body must say what to do then, and when to abort.
- An input the body never mentions produces a warning when the manifest is generated.
- `inputs.prompt` is refused. Put the task in the body.

### Entity-typed inputs and outputs

An input that names a workspace object carries `entity_type`: `job-run`, `job`, `pipeline`, `dataset`, or `workspace`. The agent receives the bare id (a run id, a job ref, a pipeline name). Declaring the type does two things:

1. The run reports the entity in its result, so the run shows up on that entity's page in the Web UI.
2. The first entity-typed input becomes `expose.object_input` in the deployment manifest. The Web UI reads it to offer the agent job from an entity. On a failed run's row it lists every agent job with a `job-run` input, and picking one starts a run with that run id filled in.

Declare the same name with `entity_type` on an output property when the agent may end up acting on a different entity than it was given. An output overwrites the input of the same name, so an inspector that resolved a run from a job ref reports the run it actually inspected.

### Output schema

`output` is a JSON Schema of what the agent returns. Two properties are part of every agent's output and dltHub adds them when the definition leaves them out:

| Property  | Meaning                                                                                                      |
| --------- | ------------------------------------------------------------------------------------------------------------ |
| `status`  | `succeeded` or `failed`, as your prompt defines them, or `aborted` when the task couldn't be done at all     |
| `summary` | Markdown. What the agent accomplished. For `aborted` it becomes the text of the exception that fails the run |

Declare both in the file so it shows the whole contract, and leave them as they stand. dltHub replaces any declaration that differs from the standard one. Adding a `status` value or typing `summary` as something other than a string has no effect: the standard `status` and `summary` are used instead. Put a domain outcome in a field of its own, so a data-quality agent returns a `verdict` and `status` keeps its meaning.

Declare the agent's own fields alongside `status` and `summary`. The model receives the whole output schema, every description and enum included, so give a description to each field whose name doesn't explain it. The schema describes the shape of the answer. The body says what each value means and when to pick it.

Keep the schema small. A large one can stop a platform run before the container launches, with no error, no logs, and no start time on the run record. A schema of about 18,000 characters of JSON never started; the same agent with the schema flattened to under 8,000 ran normally. Flatten nested models and drop the fields the `summary` already covers.

The schema reaches the model as declared, with two exceptions:

- `entity_type` moves into `$comment`.
- Anthropic's structured output rejects `minimum`, `maximum`, and `minLength`. Put numeric bounds in the field description instead.

This data-quality agent reports a verdict on the run it checked:

```yaml
output:
  type: object
  properties:
    verdict:
      enum: [pass, warn, fail]
      description: pass when every check passed, warn when only soft checks failed, fail otherwise
    checked_run_id:
      type: string
      entity_type: job-run                # travels to the model in `$comment`
    rows_checked:
      type: integer
      description: How many rows you read, between 1 and 1000000   # a bound belongs here, `minimum` is rejected
  required: [status, summary, verdict]
```

`status` and `summary` stay out of the declaration here, so dltHub adds them. `verdict` carries the domain outcome, and its description tells the model when to pick each value.

### Access declaration

`access` is declared per axis. An axis is an area of the workspace that `access` covers: `local` for the files and the shell, `data` for the data in your destinations, `context` for runs, logs, job definitions, and telemetry. Each axis takes one verb or a list of verbs, and an axis you leave out grants nothing. With no `access` at all the agent receives no file tools or shell, and its MCP server serves only the toolkit catalog.

| Axis      | Verbs           | Grants                                                                                                                                                       |
| --------- | --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `local`   | `read`          | `Read`, `Glob`, `Grep` on the workspace files                                                                                                                |
|           | `write`         | `Write`, `Edit`                                                                                                                                              |
|           | `execute`       | `Bash` (`PowerShell` on Windows) and `RunPython`, in the workspace, in the job's own process                                                                 |
|           | `network`       | `WebFetch`, `WebSearch`                                                                                                                                      |
| `data`    | `read`, `write` | Workspace data through the MCP server's data tools. `read` serves the read tools only. The SQL tool runs a single read-only statement whatever `data` grants |
| `context` | `read`          | Runs, logs, job definitions, and telemetry through the MCP server. `write`, `execute`, and `deploy` are refused when the manifest is generated               |

`all` is shorthand for every verb on an axis. `local` maps to the same toolset on both loops, under the names Claude Code uses. Credential files (`*secrets.toml`, `.env`) are never readable by a file tool, whatever `local` grants. The job runner carries no `curl`, so an agent with `execute` makes an HTTP request through `RunPython` and `urllib`.

`access` doesn't select the profile the job runs on. An agent job is a batch job and takes `prod` unless the job declares `require={"profile": "access"}`, so a `data` grant on an unpinned job reads production data with production credentials. See [Profile of an agent job](index.md#profile-of-an-agent-job).

The declaration is a request that the runtime grants as far as it can. If a loop has no tool for a granted verb, the run proceeds with the tools it has. The trace of each run lists the tools that were wired.

Write the policy into the body as well. "You are read-only" in the prompt helps the model understand its role, and the `access` block enforces it for the MCP tools. `local: execute` is the exception: the shell runs under the job's credentials and nothing restricts what it does with them, so an agent with `execute` and data access needs an explicit rule in the body never to write data.

### Tools, skills, and rules

`tools` lists feature groups of the dltHub MCP server: `workspace`, `pipeline`, `toolkit`, `secrets`, `context`, on the platform `jobs`, `logs`, `telemetry`, plus groups other plugins contribute. The agent receives exactly the groups listed, and within a group only the tools its `access` covers. Without `tools` no server is started.

`skills` and `rules` reference components of an installed toolkit as `<toolkit>:<name>`, or a workspace-relative path such as `.claude/skills/my-skill/SKILL.md`. Rules are inlined into the system prompt on both loops. On `claude-agent-sdk` skills are listed by name and loaded when the agent invokes one, as in Claude Code. On `pydantic-ai` their text is inlined. The agent receives only the listed components. Other skills and rules installed in the workspace, including the `.claude/rules` folder, aren't loaded. A reference that doesn't resolve is skipped with a warning.

### Defaults for the agent job

`defaults` holds the values that apply when the agent job and the run's configuration don't override them. State a requirement in the body, since any default can be overridden.

```yaml
defaults:
  trigger: [job.fail:*]                 # trigger strings, selectors allowed
  model: sonnet                         # alias, or provider:model
  limits: {max_turns: 30, max_tokens: 1000000}
  loop_run_args: {retries: 2}           # passed to the framework
```

`access`, `tools`, `skills`, and `rules` are declarations, not defaults. A job that references the definition keeps them as declared. A decorated function that drives the definition replaces each list it passes an argument for, every axis included.

A definition a toolkit ships leaves `model` out, so an installer isn't handed a provider. The run then takes the `sonnet` default or the model the job sets.

### System prompt body

The body is the system prompt. Write it as you would a skill, for a reader who has the tools and needs the context. Platform knowledge belongs in the referenced rules and skills. Keep the body under about two hundred lines and cover these points:

1. State the role in two sentences, including that the agent runs unattended.
2. Define `succeeded`, `failed`, and `aborted` for this agent. The schema doesn't. Under structured output the model tends to report `succeeded` when unsure, so say what counts as a failure.
3. Say what to do with each input, and with its absence. Name the fallbacks in order and the point at which the answer is `aborted`.
4. Give the first steps concretely. Which tool to call first, what to read, what to look for.
5. Write constraints as rules, for example "Never edit code, never deploy, never rerun a job".
6. Define every enum the output declares. Say what `unknown` or `low` means and that reporting it is a legitimate outcome.

The model also receives the rules, the skills, the output schema, the workspace and temp folder paths, and the tools, so the body doesn't need to repeat them. The user turn of each run is the job's `instructions`, or "Go ahead" when none are set.

## Agent definition as a Python function

A decorated function doesn't need an `AGENT.md` or a toolkit. Its docstring is the system prompt, its parameters are the inputs, and its return type is the output. The decorator arguments are the agent job's settings, and the body drives the loop it finds in `run_context["ai_loop"]`.

Write the docstring as a prompt. It goes to the model on every run with its placeholders resolved, the same way an `AGENT.md` body does, and the [system prompt body](#system-prompt-body) points apply to it. The function body decides how the loop is driven, and the model reads none of it, so a rewrite of the function that trims the docstring to a description changes the agent.

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
    access={"local": ["read"], "context": ["read"]},
    tools=["telemetry"],
    skills=["dlthub-platform:debug-deployment"],
    trigger="job.fail:*",
    require={"profile": "access"},
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

| In Python                                                                   | In the agent definition                                                            |
| --------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| Function name                                                               | `name` of the agent job                                                            |
| Docstring                                                                   | System prompt, placeholders included. Its first line is the `description`          |
| Parameters                                                                  | `inputs`, and so the job's configuration: `-c failed_run_id=...` fills them, typed |
| `Annotated[str, run.Entity("job-run")]`                                     | Entity-typed input                                                                 |
| `dlt.config.value` default                                                  | Required input                                                                     |
| `run_context` parameter                                                     | Passed by the launcher, not declared as an input                                   |
| Return type deriving from `run.TAgentOutput`                                | `output`. `run.Doc(...)` on a field is its description                             |
| `access=`, `tools=`, `skills=`, `rules=`                                    | Matching `AGENT.md` fields                                                         |
| `model=`, `limits=`, `loop_run_args=`, `instructions=`, `trigger=`, `loop=` | Agent job settings, `defaults` in an `AGENT.md`                                    |

The schemas come from pydantic, so `Optional`, `Literal`, `List`, nested models, and `NotRequired` behave as they do everywhere else. The function may be `def` or `async def`. Most functions return the loop's output as is. The example body shows the function can also inspect `loop.trace`, run the loop twice, or skip it.

A function can also drive an installed agent definition. Pass it as `agent=`. The decorator arguments and the function override its fields:

```py notype
@run.agent(
    agent="dlthub-platform:job-inspector",
    loop="claude-agent-sdk",
    require={"profile": "access"},
)
async def inspect(run_context: run.TJobRunContext = None) -> run.TAgentOutput:
    return await run_context["ai_loop"].run()
```

The function leaves `access`, `tools`, `skills`, and `rules` out here, so the definition's own lists stand. Passing one of them replaces the definition's list rather than adding to it, so an `access` argument has to name every axis the agent needs.

## Next steps

- [Background agents](index.md) covers declaring the definition as a job, running it, and deploying it
- [Job inspector agent](job-inspector.md) is the verified agent that diagnoses failed job runs
