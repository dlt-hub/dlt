---
title: Agent definitions
description: Write a dltHub agent definition as an AGENT.md file or as a decorated Python function
keywords: [dlthub platform, agents, AGENT.md, agent definition, inputs, output, access, tools, skills, rules, run.agent]
---
# Agent definitions

:::warning
This feature is in public preview
:::

Installed toolkits ship verified agent definitions ready to declare as agent jobs, such as the [job inspector agent](job-inspector.md) in the `dlthub-platform` toolkit. This page describes how to write your own, or how to adapt an installed one for your workspace.

An agent definition consists of a system prompt and a declaration of the agent's inputs, output, tools, and access. You can write it as an `AGENT.md` file or as a decorated Python function. Both forms produce the same agent job.

An `AGENT.md` is better for three cases: an agent definition that is only a system prompt plus declarations, a definition that a toolkit ships, and a definition that people who don't write Python maintain. A Python function is better when the schemas come from Python types. It is also better when code runs around the loop: to derive an input, inspect the agent trace, run the loop twice, or not run it. A function can also drive an installed `AGENT.md` through `agent=`, so a toolkit definition keeps the system prompt while your code handles the rest.

## Write it with your coding agent

The `create-background-agent` skill ships with the base `init` toolkit, so every dltHub workspace has it without an install. Describe the agent you want:

> Write a background agent that checks each morning which of my jobs failed overnight and reports what they have in common.

The skill covers the frontmatter fields, the `access` decision, the output contract, and the system prompt body. It shows you the deployment plan before it writes the `run.agent(...)` call. It routes you elsewhere in three cases: the work is deterministic and belongs in a plain job, a single failure needs diagnosing right now, or what you want is a subagent of your coding agent in `.claude/agents/`, which runs inside a conversation.

The rest of this page is the reference behind that skill. Read it to review what the skill wrote, or to write a definition by hand.

## Agent definition in an `AGENT.md` file

`dlthub ai toolkit install` copies a toolkit's agent definitions to `.claude/dlthub/agents/<name>/AGENT.md` (`.cursor/dlthub/agents/` or `.agents/dlthub/agents/` for the other hosts). Coding agents don't scan this folder for their own subagents. You can also keep an `AGENT.md` in any folder of the workspace and refer to it by its path.

The YAML frontmatter holds the declarations and the Markdown body is the system prompt. Only the body is required. A file with no frontmatter is a working agent definition, named after its folder. The example below is a shortened version of the `job-inspector` definition:

```md
---
name: job-inspector
description: Inspects a failed job run and reports a diagnosis with a proposed fix. Read-only.
# feature groups of the dltHub MCP server, not the same names as the access axes
tools:  [jobs, logs, telemetry, workspace, secrets, config]
skills: [dlthub-platform:debug-deployment]  # loaded on demand or inlined
rules:  [init:dlthub-workspace, dlthub-platform:job-resources, dlthub-platform:profiles]

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
      entity_type: job-runs                 # this input names a workspace entity
    failed_job_ref:
      type: string
      description: job ref of the failed job; its latest failed run is inspected when no run id is given
      entity_type: job
  required: []

output:
  type: object
  properties:
    status:
      type: string
      enum: [succeeded, failed, aborted]
      description: Outcome of your task, as defined in your system prompt
    summary:
      type: string
      description: Markdown. What you accomplished, or what blocked you when `status` is `aborted`
    failed_run_id:                          # the run actually inspected, reported as an entity
      type: string
      entity_type: job-runs
    classification:
      type: string
      enum: [config, credentials, upstream_data, code, resources, transient, unknown]
      description: The kind of failure, as defined in the "Classification" section of your system prompt
    confidence:
      type: string
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
            type: string
            enum: [run_log, run_record, trace, job_definition, workspace_file, secrets_redacted, inference]
    proposed_fix:
      type: string
      description: What a human should do next, naming the target and the change. You never apply it
    requires_human:
      type: boolean
  required: [status, summary, classification, confidence, evidence, requires_human]

defaults:                                   # the job and the run may override all of these
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
| `access`          | What the agent asks to read, write, run, or reach, per access axis: `local`, `data`, `context` |
| `inputs`          | JSON Schema of the inputs. Each input is a job configuration key                               |
| `output`          | JSON Schema of the output. `status` and `summary` are part of it in every agent definition     |
| `defaults`        | Settings that the agent job and the run can override: `model`, `limits`, `loop_run_args`       |
| body              | System prompt, a template over `inputs`                                                        |

### Input schema

`inputs` is a JSON Schema. Every property becomes a configuration key of the job. You can set it in three ways: with `-c failed_run_id=...` on the command line, under `[jobs.<section>.<job>]` in `config.toml` or the environment, or with a run argument that the trigger carries. Values are typed: `-c depth=3` resolves to an `int` when the schema declares one.

The body refers to inputs as `{{ name }}`. It can also refer to the run itself: `{{ run_context.trigger }}`, `{{ run_context.run_id }}`, `{{ run_context.refresh }}`, and on a job with an interval `{{ run_context.interval_start }}` and `{{ run_context.interval_end }}`. The loop renders the placeholders before the first turn.

- A required input with no value fails the run like any missing job argument.
- An optional input with no value renders as empty text. The body must say what to do then, and when to abort.
- An input the body never mentions produces a warning when dlt generates the deployment manifest.
- `inputs.prompt` is refused. Put the task in the body.

### Entity-typed inputs and outputs

An input that names a workspace object carries `entity_type`: `job-runs`, `job`, `pipeline`, `dataset`, or `workspace`. The agent receives the bare id (a run id, a [job ref](index.md#job-refs), a pipeline name). dlt still accepts `job-run`, the old spelling of `job-runs`, and passes it on unchanged. Write `job-runs`: the old spelling is going away. Declaring the type does two things:

1. The job run reports the entity in its job result, so the run shows up on that entity's page in the Web UI.
2. The first entity-typed input becomes `expose.object_input` in the deployment manifest. The Web UI reads it to offer the agent job from an entity. On the row of a failed job run, the Web UI lists every agent job with a `job-runs` input. A click on one starts an agent run with that run id.

If the agent can act on a different entity than the one it received, declare the same name with `entity_type` on an output property. The output value overwrites the input of the same name. As a result, an inspector that found a run from a job ref reports the run that it inspected.

### Output schema

`output` is a JSON Schema of what the agent returns. Two properties are part of every agent's output and dltHub adds them when the definition leaves them out:

| Property  | Meaning                                                                                                           |
| --------- | ----------------------------------------------------------------------------------------------------------------- |
| `status`  | `succeeded` or `failed`, as your system prompt defines them, or `aborted` when the agent can't do the task at all |
| `summary` | Markdown. What the agent accomplished. For `aborted` it becomes the text of the exception that fails the run      |

dltHub replaces any declaration of `status` or `summary` that differs from the standard one. Adding a `status` value or typing `summary` as something other than a string has no effect: dltHub uses the standard `status` and `summary` instead. The model receives the whole output schema, every description and enum included. The schema describes the shape of the answer. The body says what each value means and when to pick it.

- Declare `status` and `summary` in the file so it shows the whole contract, and leave them as they stand.
- Put a domain outcome in a field of its own, so a data-quality agent returns a `verdict` and `status` keeps its meaning.
- Declare the agent's own fields alongside `status` and `summary`. Give a description to each field whose name doesn't explain it.

:::warning
Keep the schema small. A large one can stop a platform run before the container launches, with no error, no logs, and no start time on the run record. Stay under about 8,000 characters of JSON: flatten nested models and drop the fields the `summary` already covers.
:::

The schema reaches the model as declared, with these exceptions:

- `entity_type` moves into `$comment`.
- Anthropic's structured output rejects `minimum`, `maximum`, and `minLength`. Put numeric bounds in the field description instead.
- Anthropic also rejects a property that carries an `enum` and no `type`. Declare `type: string` next to every `enum`. dlt fills the type on `status` alone.
- A bare `type: object` means "any object", which OpenAI's structured output refuses. Name the `properties` of every object, including the ones your Python code fills after the run.

This data-quality agent reports a verdict on the run it checked:

```yaml
output:
  type: object
  properties:
    verdict:
      type: string
      enum: [pass, warn, fail]
      description: pass when every check passed, warn when only soft checks failed, fail otherwise
    checked_run_id:
      type: string
      entity_type: job-runs               # travels to the model in `$comment`
    rows_checked:
      type: integer
      description: How many rows you read, between 1 and 1000000   # a bound belongs here, `minimum` is rejected
  required: [status, summary, verdict]
```

`status` and `summary` stay out of the declaration here, so dltHub adds them. `verdict` carries the domain outcome, and its description tells the model when to pick each value.

### Summary structure

`summary` is Markdown, rendered on the run page. By default it carries three headings, each over short bullets:

| Heading             | What the bullets answer                                                                      |
| ------------------- | -------------------------------------------------------------------------------------------- |
| `## Diagnosis`      | The root cause: what failed, where, and why. One bullet quotes the evidence line carrying it |
| `## Recommendation` | The next action: the target and the change, written as the instruction itself                |
| `## Confidence`     | The limits of the diagnosis: every entry of `open_points`, and why this confidence           |

If your agent reports something else, declare your own headings in the body.

### Access declaration

`access` is declared per axis. An axis is an area of the workspace that `access` covers: `local` for the files and the shell, `data` for the data in your destinations, `context` for runs, logs, job definitions, and telemetry. Each axis takes one verb or a list of verbs, and an axis that you leave out declares no access. With no `access` at all, the agent gets no file tools and no shell. If `tools` starts an MCP server, the server serves only the tools that need no access, such as the toolkit catalog.

The `context` axis and the `context` feature group of `tools` are different declarations that share a name. The axis says the agent may read platform records. The group names a set of MCP tools, listed under [Tools, skills, and rules](#tools-skills-and-rules). A tool is offered only when both cover it: the group puts it on the server, the axis permits it.

| Axis      | Verbs           | Grants                                                                                                                                                                              |
| --------- | --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `local`   | `read`          | `Read`, `Glob`, `Grep` on the workspace files                                                                                                                                       |
|           | `write`         | `Write`, `Edit`                                                                                                                                                                     |
|           | `execute`       | `Bash` (`PowerShell` on Windows) and `RunPython`, in the workspace, in the job's own process                                                                                        |
|           | `network`       | `WebFetch`, `WebSearch`. On `pydantic-ai` with an Anthropic model the provider serves these, see below                                                                              |
| `data`    | `read`, `write` | Workspace data through the data tools of the MCP server. `read` offers the read tools only and holds the SQL tool to a single `SELECT`. Mapping `write` to a dlt profile is planned |
| `context` | `read`          | Runs, logs, job definitions, and telemetry through the MCP server. `read` is the only verb a runtime serves today, so dlt refuses `write`, `execute`, and `deploy` at manifest time |

`all` is shorthand for every verb on an axis. `local` maps to the same toolset on both loops, under the names Claude Code uses. Credential files (`*secrets.toml`, `.env`, `.env.*`) are never readable by a file tool, whatever verbs `local` declares. The job runner carries no `curl`, so an agent with `execute` makes an HTTP request through `RunPython` and `urllib`.

`access` doesn't select the profile the job runs on. An agent job takes the read-only `access` profile unless it declares otherwise, so an agent with `data: read` reads through read-only credentials. Keep it that way: an unattended agent must not hold the production profile. See [Profile of an agent job](index.md#profile-of-an-agent-job).

The declaration is a request that the runtime grants as far as it can. If a loop has no tool for a granted verb, the run proceeds with the tools it has. The agent trace of each agent run lists the tools that the loop wired.

Model providers can give a model tools that run on their own servers, on any loop and with any model. `access` doesn't control these tools. For example, `pydantic-ai` gives recent Anthropic models the web search and fetch tools of Anthropic for `network`. These tools filter their results with Python that the model writes, so Anthropic also gives the model a code execution sandbox. The sandbox runs on the servers of Anthropic. It has no access to the workspace, its files, or its credentials, and no network access of its own. The model gets it whether or not the agent has `execute`.

Write the policy into the body as well. "You are read-only" in the system prompt helps the model understand its role, and the `access` block enforces it for the MCP tools. `local: execute` is the exception. The shell runs with the credentials of the job, and nothing limits what it does with them. An agent with `execute` and data access needs a rule in the body: never write data.

### Tools, skills, and rules

`tools` lists feature groups of the dltHub MCP server: `workspace`, `pipeline`, `toolkit`, `secrets`, `config`, `context`, on the platform `jobs`, `logs`, `telemetry`, plus groups other plugins contribute. `secrets` is how an agent sees credentials: it serves the names and a redacted view, and the tool that writes a fragment needs `local: write` and is dropped without it. `config` serves the workspace variables. The agent receives exactly the groups listed, and within a group only the tools its `access` covers. Without `tools` no server is started.

`skills` and `rules` reference components of an installed toolkit as `<toolkit>:<name>`, or a workspace-relative path such as `.claude/skills/my-skill/SKILL.md`. Both loops inline the rules into the system prompt. `claude-agent-sdk` lists the skills by name and loads a skill when the agent calls it, as Claude Code does. `pydantic-ai` inlines the text of the skills. The agent receives only the listed components. Other skills and rules installed in the workspace, including the `.claude/rules` folder, aren't loaded. If a reference doesn't resolve, dlt logs a warning and skips it.

### Defaults for the agent job

`defaults` holds the values that apply when the agent job and the run's configuration don't override them. State a requirement in the body, since any default can be overridden.

```yaml
defaults:
  model: sonnet                         # alias, or provider:model
  limits: {max_turns: 30, max_tokens: 1000000}
  loop_run_args: {retries: 2}           # passed to the framework
```

`access`, `tools`, `skills`, and `rules` are declarations, not defaults. A job that references the definition keeps them as declared. A decorated function that drives the definition replaces each list it passes an argument for, every axis included.

Leave `model` out of a definition you ship in a toolkit. The user who installs it can be on Anthropic, OpenAI, Azure, or Google. That user can't always reach the model that you name. The job or the workspace picks it. Write in the body which models you wrote the system prompt for, for example "at least as capable as Claude Sonnet 5".

### System prompt body

The body is the system prompt. Write it as you would a skill, for a reader who has the tools and needs the context. Platform knowledge belongs in the referenced rules and skills. Keep the body under about two hundred lines and cover these points:

1. State the role in two sentences, including that the agent runs unattended.
2. Define `succeeded`, `failed`, and `aborted` for this agent. The output schema lists the three values but says nothing about when each applies, so the model decides for itself unless the body tells it. Under structured output it leans towards `succeeded` when unsure, so spell out what counts as a failure.
3. Say what to do with each input, and with its absence. Name the fallbacks in order and the point at which the answer is `aborted`.
4. Give the first steps concretely. Which tool to call first, what to read, what to look for.
5. Write constraints as rules, for example "Never edit code, never deploy, never rerun a job".
6. Define every enum the output declares. Say what `unknown` or `low` means and that reporting it is a legitimate outcome.
7. State the headings `summary` must carry and the shape of its bullets. See [Summary structure](#summary-structure).

The model also receives the rules, the skills, the output schema, the tools, and the paths of the workspace and the temp folder. The body doesn't need to repeat them. The user turn of each run is the job's `instructions`, or "Go ahead" when none are set.

### Agent code in `agent.py`

An agent folder can have an `agent.py` next to its `AGENT.md`. dlt imports it when a job that references the agent definition runs, and calls two functions from it when they are defined:

| Function                  | Called with                                                                                                                                                                                                   | Its return value                                          |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| `validate_input(inputs)`  | the inputs of the run as a dict, before the loop starts: each declared input that has a value, the run arguments and the call arguments, and `run_context` with `run_id`, `trigger`, `refresh` and `run_args` | replaces the inputs. Return `None` to keep them unchanged |
| `validate_output(output)` | the agent output as a dict, after the loop ends: `status`, `summary` and the declared output fields                                                                                                           | replaces the output. Return `None` to keep it unchanged   |

Return the whole dict, not only the keys that you change. The dict that you return replaces the inputs, and `None` keeps them.

```py notype nolint
# .claude/dlthub/agents/job-inspector/agent.py
from dlt.hub.run import JobAbortedException


def validate_input(inputs):
    run_id = inputs.get("failed_run_id")
    if not run_id:
        # ends the run without calling the model
        raise JobAbortedException("no failed run to inspect", {"summary": "nothing to inspect"})
    return {**inputs, "failed_run_id": run_id.strip()}


def validate_output(output):
    if output["status"] == "succeeded" and not output.get("evidence"):
        return {**output, "status": "failed", "summary": "the diagnosis cites no evidence"}
    return None
```

- An exception from either function fails the job run. To end a run on purpose without calling the model, raise `JobAbortedException` from `validate_input` with the agent output to deliver. The run is reported as `aborted`, and `validate_output` isn't called.
- A job that also passes `inputs_validator` or `outputs_validator` gets both: the functions in `agent.py` run first, then the job's own on their result.
- dlt runs `agent.py` only for a job that references the agent definition, as `run.agent("<toolkit>:<agent>")` or by path. A decorated function drives the loop itself. dlt never imports `agent.py` when it generates the deployment manifest.

#### Helper modules and module state

The agent folder is imported as a package, so `agent.py` can import the files next to it relatively. Two agents can each ship a `checks.py` without a conflict. `agent.py` is imported afresh for every run, so a module variable can carry what `validate_input` found to `validate_output` within one run:

```py notype nolint
# .claude/dlthub/agents/<name>/agent.py
from .checks import prepare, finalize

_prep = None


def validate_input(inputs):
    global _prep
    _prep = prepare(inputs["run_context"])
    return {**inputs, **_prep.judge_inputs}


def validate_output(output):
    return finalize(output, _prep)
```

## Agent definition as a Python function

A decorated function doesn't need an `AGENT.md` or a toolkit. Its docstring is the system prompt. Its parameters with a default or with `dlt.config.value` are the inputs. Its return type is the output. The decorator arguments are the agent job's settings, and the body drives the loop it finds in `run_context["ai_loop"]`.

Write the docstring as a system prompt. The model gets it on every agent run with its placeholders rendered, as it gets an `AGENT.md` body. The points in [System prompt body](#system-prompt-body) apply to it. The function body controls the loop, and the model doesn't read it. If a rewrite cuts the docstring down to a description, the agent changes.

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
    trigger="job.fail:tag:ingest",
    require={"profile": "access"},
    limits={"max_turns": 30},
)
async def crash_inspector(
    failed_run_id: Annotated[str, run.Entity("job-runs")] = dlt.config.value,
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

| In Python                                       | In the agent definition                                                                                                           |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| Function name                                   | `name` of the agent job                                                                                                           |
| Docstring                                       | System prompt, placeholders included. Its first line is the `description`                                                         |
| Parameters with a default or `dlt.config.value` | `inputs`, and so the job's configuration: `-c failed_run_id=...` fills them, typed. A parameter without a default is not an input |
| `Annotated[str, run.Entity("job-runs")]`        | Entity-typed input                                                                                                                |
| `dlt.config.value` default                      | Required input                                                                                                                    |
| `run_context` parameter                         | Passed by the launcher, not declared as an input                                                                                  |
| Return type deriving from `run.TAgentOutput`    | `output`. `run.Doc(...)` on a field is its description                                                                            |
| `access=`, `tools=`, `skills=`, `rules=`        | Matching `AGENT.md` fields                                                                                                        |
| `model=`, `limits=`, `loop_run_args=`           | `defaults` in an `AGENT.md`                                                                                                       |
| `instructions=`, `trigger=`, `loop=`            | Agent job settings. An `AGENT.md` has no field for them                                                                           |

The schemas come from pydantic, so `Optional`, `Literal`, `List`, nested models, and `NotRequired` behave as they do everywhere else. The function can be `def` or `async def`. Most functions return the loop's output as is. The example reads `loop.trace` after the run. A function can also run the loop twice, or not run it.

A function can also drive an installed agent definition. Pass it as `agent=`. The decorator arguments override the fields of the definition. The function overrides them in turn:

- A docstring replaces the body.
- The parameters of the function decide which inputs exist. For each parameter, what the signature says wins, and the definition fills in what the signature leaves out: the description, `entity_type`, other attributes, and the type when the parameter has no annotation. Inputs of the definition that the function doesn't take are dropped. The inputs of the agent job in the deployment manifest, `expose.object_input` included, come from the same merge.
- A return type that derives from `run.TAgentOutput` replaces `output`. Without a return type, the definition's `output` stays.

```py notype
@run.agent(
    agent="dlthub-platform:job-inspector",
    loop="claude-agent-sdk",
    require={"profile": "access"},
)
async def inspect(
    failed_run_id=dlt.config.value,
    failed_job_ref: str = None,
    run_context: run.TJobRunContext = None,
):
    return await run_context["ai_loop"].run(
        inputs={"failed_run_id": failed_run_id, "failed_job_ref": failed_job_ref}
    )
```

The function has no docstring and no return type, so the system prompt and the output of the definition stay. `failed_run_id` has no annotation, so it takes its type, its description, and `entity_type: job-runs` from the definition. `failed_job_ref` declares its type and takes its description and `entity_type: job` from the definition.

The function leaves `access`, `tools`, `skills`, and `rules` out here, so the definition's own lists stand. Passing one of them replaces the definition's list rather than adding to it, so an `access` argument has to name every axis the agent needs.

## Next steps

- [Background agents](index.md) covers declaring the definition as a job, running it, and deploying it
- [Job inspector agent](job-inspector.md) is the verified agent definition that diagnoses failed job runs
