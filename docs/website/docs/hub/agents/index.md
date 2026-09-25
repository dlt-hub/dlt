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

| Term             | Definition                                                                                                                                                                          | Where it lives                                                        |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| Agent definition | System prompt plus a declaration of the agent's inputs, output, tools, skills, rules, and access                                                                                    | `AGENT.md` file, or a decorated Python function                       |
| Agent loop       | Framework that runs the model turn by turn: `pydantic-ai` (default) or `claude-agent-sdk`                                                                                           | Selected with `loop=` on `run.agent` or `agent.loop` in configuration |
| Agent job        | Definition plus the settings for your workspace: model, limits, trigger, instructions, loop                                                                                         | `run.agent(...)` in `__deployment__.py`                               |
| Agent run        | Execution of the agent job. It receives inputs and returns an output and a trace                                                                                                    | Started by a trigger, `dlthub local run`, `dlthub run`, or the Web UI |
| Access axis      | Area of the workspace that `access` covers: `local` for the files and the shell, `data` for the data in your destinations, `context` for runs, logs, job definitions, and telemetry | Key of `access` in the agent definition                               |
| Verb             | What the agent may do on an axis: `read`, `write`, `execute`, `network`                                                                                                             | Listed under the axis in `access`                                     |

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
    require={"profile": "access"},
    model="sonnet",
    limits={"max_turns": 20},
    instructions="focus on the loader step",
)
```

The job is named after the agent definition (`job-inspector` becomes `job_inspector`) in the declaring module's section. Every argument overrides the matching entry of the definition's `defaults`.

`access`, `tools`, `skills`, and `rules` aren't `defaults`. A referenced agent keeps the lists its definition declares and `run.agent` drops the arguments for them. A decorated function driving a referenced agent is the other way around: its argument replaces the definition's list, so `access={"local": ["read"]}` on such a function removes `context: read`. Pass every axis the agent needs, or leave the block to the definition.

We strongly advise never to assign the production profile to an unattended agent. An agent job takes the read-only `access` profile by default, and the example pins it. See [Profile of an agent job](#profile-of-an-agent-job).

| Argument                                          | Meaning                                                                                                                                                             |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `instructions`                                    | First user message of each run. Use it for the task at hand. The system prompt describes the agent                                                                  |
| `model`                                           | `provider:model` id such as `anthropic:claude-sonnet-5`, or an alias. See [Model and credentials](#model-and-credentials)                                           |
| `limits`                                          | `max_turns` and `max_tokens` per run. The loop ends the run when either is exhausted                                                                                |
| `loop`                                            | `"pydantic-ai"` (default) or `"claude-agent-sdk"`. See [Agent loops](#agent-loops)                                                                                  |
| `loop_run_args`                                   | Arguments passed to the framework, merged over the definition's defaults. `retries` sets how many times pydantic-ai allows the model to correct a failing tool call |
| `verbosity`                                       | How much of the run is printed: `0` the outcome and tool names, `1` (default) adds the agent's thoughts and tool arguments, `2` adds the rendered system prompt     |
| `inputs_validator`                                | Called with the resolved inputs before the run. Its return value is merged into them, so use it to derive an input such as a run id from a job ref                  |
| `outputs_validator`                               | Called with the agent's output after the run. Its return value replaces the output                                                                                  |
| `name`, `section`                                 | Job name and configuration section, as on every job                                                                                                                 |
| `trigger`, `execute`, `expose`, `require`, `spec` | Standard job options. See [Triggers and scheduling](../pipeline-operations/triggers.md) and [Job configuration](../pipeline-operations/job-configuration.md)        |

Both validators are accepted only when the agent is passed by reference. A decorated function drives the loop itself and passes the inputs to `loop.run()`.

### Triggers for agents

Agent jobs take every trigger other jobs take. Two string triggers react to the outcome of other jobs in the workspace: `job.fail:` and `job.success:`. After the colon you write which jobs to watch, as a job ref or as a selector.

#### Job refs

A job ref is the name the platform gives one job. It is built from where the job is declared, so you can read it off the source. This file declares two jobs:

```py notype
# github_pipeline.py
from dlt.hub import run

@run.pipeline("github", expose={"tags": ["ingest"]})
def load_commits():
    ...

@run.job()
def check_commits():
    ...
```

Deployed, they are `jobs.github_pipeline.load_commits` and `jobs.github_pipeline.check_commits`:

| Part              | Where it comes from                                             |
| ----------------- | --------------------------------------------------------------- |
| `jobs`            | Fixed prefix on every job ref                                   |
| `github_pipeline` | The section: the file the job is declared in, without the `.py` |
| `load_commits`    | The decorated function's name                                   |

The section is the filename. The pipeline name, `"github"` here, never appears in the ref. Pass `section="ingest"` to the decorator to set it yourself, which is what you do for jobs written inline in `__deployment__.py`, where the section would otherwise be `__deployment__`. `dlthub job list` prints the refs of a deployment.

#### Selectors

A selector matches a set of jobs at once. It takes the same forms `dlthub job trigger` takes:

| Selector                            | Matches                                            |
| ----------------------------------- | -------------------------------------------------- |
| `jobs.github_pipeline.load_commits` | That one job                                       |
| `jobs.github_pipeline.*`            | Every job declared in `github_pipeline.py`         |
| `tag:ingest`                        | Every job tagged `ingest`, wherever it is declared |
| `batch:`                            | Every batch job                                    |
| `*`                                 | Every job in the workspace                         |

So `trigger="job.fail:tag:ingest"` starts the agent whenever a job tagged `ingest` fails, and `trigger="job.success:jobs.github_pipeline.load_commits"` starts it when `load_commits` succeeds.

A selector expands at deploy time to a follow-up trigger per matching job. The declaring job itself and interactive jobs are excluded. A run started manually arrives with a `manual:` trigger and only the inputs it was given, so the body must say what to do with empty input.

An agent that only ever runs when you start it takes no `trigger=` at all. The runner adds the `manual:` trigger itself, so `trigger.manual()` is not something you pass: it raises `InvalidTrigger: manual: triggers are added automatically`.

### Profile of an agent job

A [profile](../pipeline-operations/profiles.md) names the set of credentials a job runs with. It's a separate thing from the `access` declaration: `access` decides which tools the model is offered, the profile decides which credentials the job process holds. An agent granted `data: write` on a job running the `access` profile still can't write, because the credentials it holds can't.

The profile covers profile-scoped configuration: `prod.secrets.toml`, `prod.config.toml`, and a variable set with `dlthub variable set --profile prod`. A variable set with `--workspace` carries no profile and reaches the job whatever it runs on, so a secret that must stay away from an agent belongs in a profile scope. `dlthub variable list` prints the scope of each one.

Which profile an agent job runs on is decided for you unless you say otherwise. An agent job that declares none runs on the read-only `access` profile, so the production credentials stay out of its environment. Other batch jobs still default to `prod`; the agent job is the exception.

Every workspace has an `access` profile, so the default applies wherever you deploy. Pin it on the job anyway, to state the intent in the code:

```py notype
inspector = run.agent(
    "dlthub-platform:job-inspector",
    trigger="job.fail:tag:ingest",
    require={"profile": "access"},
)
```

A declared profile always wins, `prod` included, so nothing stops `require={"profile": "prod"}` on an agent job. Don't give an agent the production profile. An agent job runs unattended, with a model deciding what to do, and the production profile hands that decision the credentials to change your data. Work that needs production write credentials belongs in a pipeline or a plain job a person wrote and reviewed.

:::warning
The profile is a name for a set of credentials, and dltHub doesn't check what those credentials can do. Before you run an unattended agent, make sure the destination credentials in your `access` profile are read-only at the destination itself: a read-only database role, a storage key without write permission. An `access` profile holding a writable credential gives the agent write access under a read-only name. [Define profiles](../pipeline-operations/profiles.md#define-profiles) covers where each profile's credentials live.
:::

Manifest validation refuses a local-only profile (`dev`, `tests`) and takes any other name as given, so a typo surfaces as missing credentials at run time. On `dlthub local run` the declaration is a warning rather than a switch: the run uses the active profile and reports the mismatch.

## Run an agent job

Each trigger of the agent job produces an agent run. You can also start a run manually, the way you run any job locally or on the platform:

```sh
dlthub local run job_inspector -c failed_run_id=<run-id>     # locally
dlthub run job_inspector -f                                  # on the platform
```

`-c` is a local flag. `dlthub run` does not take it, and a workspace variable holding an input key does not reach a remote run either. A remote run takes its inputs from the trigger that started it, or from the job's `config.toml` as deployed. To point a remote agent run at one specific run id today, put the value in `config.toml` and deploy, or run it locally.

You can override settings for a single local run. Inputs and agent settings are ordinary job configuration under the job's section, so the same keys work on the command line, in `config.toml`, and in the environment:

| What                                         | Key                            | Example                                                                                        |
| -------------------------------------------- | ------------------------------ | ---------------------------------------------------------------------------------------------- |
| Declared inputs                              | `jobs.<section>.<job>.<input>` | `-c failed_run_id=...`                                                                         |
| Instructions, model, limits, loop, verbosity | `jobs.<section>.<job>.agent.*` | `-c agent.instructions="explain, do not fix"`, `-c agent.max_turns=10`, `-c agent.verbosity=2` |

```toml
# .dlt/config.toml
[jobs.__deployment__.job_inspector]
failed_job_ref = "jobs.github_pipeline.load_commits"

[jobs.__deployment__.job_inspector.agent]
model = "sonnet"
max_turns = 20
verbosity = 0
```

Each source overrides the ones before it: the loop default, the definition's `defaults`, the `run.agent` argument, the run's configuration.

### Model and credentials

`model` is a `provider:model` id in the naming pydantic-ai uses, or an alias for one:

| Provider     | `model`                                                                            | Alias                              |
| ------------ | ---------------------------------------------------------------------------------- | ---------------------------------- |
| Azure OpenAI | `azure:<deployment name>`                                                          | none                               |
| Anthropic    | `anthropic:claude-sonnet-5`, `claude-opus-5`, `claude-haiku-4-5`, `claude-fable-5` | `sonnet`, `opus`, `haiku`, `fable` |
| OpenAI       | `openai:gpt-5.5`, `openai:gpt-5.4-mini`, `openai:gpt-5.4-nano`                     | `gpt`, `gpt-mini`, `gpt-nano`      |
| Google       | `google:gemini-3.5-flash`, `google:gemini-3.1-pro-preview`                         | `gemini`, `gemini-pro`             |

There's no default model. The workspace deploying the agent sets one, and a definition a toolkit ships names none, so the same definition works whatever provider you have.

Azure OpenAI addresses a deployment on your own endpoint rather than a shared model, so it has no alias and needs `api_url` and `api_version` alongside the model and the key. The `claude-agent-sdk` loop runs Anthropic models only, so naming it in a workspace whose key is Azure or Google breaks the run.

Credentials for the provider go under the job's `agent` section, in `secrets.toml` or the environment. Without them, the provider's default environment variables are used (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, and so on):

```toml
# .dlt/secrets.toml
[jobs.__deployment__.job_inspector.agent]
model = "anthropic:claude-sonnet-5"
api_key = "sk-ant-..."
```

Azure OpenAI takes all four:

```toml
# .dlt/secrets.toml
[jobs.__deployment__.job_inspector.agent]
model = "azure:my-gpt-deployment"
api_key = "..."
api_url = "https://my-resource.openai.azure.com"
api_version = "2024-10-21"                   # the api-version your deployment serves
```

Azure is the only provider pydantic-ai gives `api_version`. Elsewhere it's ignored with a warning, so leave it unset.

On the platform the runtime can supply a model endpoint of its own. `model`, `api_key`, `api_url`, and `api_version` are one set: if you set any of them, the run takes all four from your configuration and ignores the runtime's endpoint. Setting `api_key` alone leaves `model` unset, so the run sends the wrong model name to your endpoint and fails with `401 API key is invalid`. The run logs which endpoint it used.

On the platform, set the four as workspace variables rather than in `secrets.toml`. They arrive on the runner as environment and override the file:

```sh
printf '%s' '<key>' | dlthub variable set AGENT__API_KEY --secret --workspace
```

### Deploy the agent job

`dlthub deploy` ships the job graph and the agent definitions with the rest of the workspace:

```sh
dlthub deploy
```

A selector trigger expands at deploy time, so a `job.fail:` agent job starts watching the jobs it matches as soon as the deployment lands. See [Deployments](../pipeline-operations/deployments.md).

:::warning
Check what a wide selector matched before you deploy a second agent job. `job.fail:*` and `job.fail:batch:` match every batch job in the workspace, agent jobs included. The declaring job is excluded, so an agent never triggers on its own failures, but two agents both watching `job.fail:*` do trigger each other: a failed run of A starts B, a failed run of B starts A, and the pair keeps going until you archive one. Scope each agent with a tag or a section selector, such as `job.fail:tag:ingest`. `dlthub deploy --show-manifest` prints the concrete triggers a selector expanded to. Excluding agent jobs from wide selectors is planned.
:::

## Read the agent run result

An agent run leaves three things behind, and they answer different questions:

| What        | Where                                                                                  | Holds                                                                                                                                                                             |
| ----------- | -------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Run log     | Your terminal on a local run, the run's **Logs** in the Web UI, `dlthub job runs logs` | Everything the run printed as it went: the model's reasoning, its messages, each tool call and what it returned, and a closing line listing the tools, skills, and MCP tools used |
| Agent trace | The run's **Trace** in the Web UI, and `trace` in the result below                     | The structured record of the same run: model, limits, resolved inputs, the tools that were wired, turn and token counts, per-turn tool calls                                      |
| Job result  | The run's **Summary** in the Web UI, `dlthub job runs info`                            | What the agent returned: `status`, `summary`, and the fields its `output` schema declares                                                                                         |

The log is what the run printed, so you read it to follow what the agent did and why. The trace is queryable, so you read it to count turns and tokens or to check which tools a run was actually given.

`agent.verbosity` controls how much reaches the log: `0` the outcome and tool names, `1` adds the agent's thoughts and tool arguments, `2` adds the rendered system prompt. The log is colored when a terminal is attached. Set `DLT_ECHO_FORCE_COLOR` to keep the colors without a terminal, or `DLT_ECHO_NO_COLOR` to drop them.

When the run ends, the launcher prints and delivers the job result:

```json
{
  "type": "job.background_agent.dlthub-platform:job-inspector",
  "engine_version": 1,
  "job_ref": "jobs.__deployment__.job_inspector",
  "status": "succeeded",
  "summary": "## Diagnosis\n\n- The `load_commits` job failed in the extract step ...",
  "result": {
    "status": "succeeded",
    "summary": "...",
    "failed_run_id": "<run-id>",
    "failed_job_ref": "jobs.github_pipeline.load_commits",
    "classification": "config",
    "confidence": "high",
    "evidence": [
      {
        "source": "dlthub job runs logs <run-id> line 38",
        "excerpt": "...",
        "provenance": "run_log"
      }
    ],
    "proposed_fix": "...",
    "fix_target": "pipelines/github.py",
    "fix_change": "cursor_path=\"updated_at\"",
    "open_points": [],
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

|                 | `pydantic-ai` (default)                                                                                        | `claude-agent-sdk`                                     |
| --------------- | -------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| Models          | Any provider pydantic-ai supports                                                                              | Anthropic models, through a bundled Claude Code CLI    |
| Local tools     | dlt's own file, search, and shell tools. Web access comes from the model provider's own search and fetch tools | Claude Code's tools, under the same names              |
| Skills          | Inlined into the system prompt                                                                                 | Listed by name and loaded on demand, as in Claude Code |
| Install locally | `uv add "pydantic-ai-slim[anthropic,openai,google,mcp,spec]"`                                                  | `uv add claude-agent-sdk`                              |

Both loops read the same declarations. `access` selects the local tools and `tools` selects the MCP server features. The rendered body becomes the system prompt, `instructions` becomes the user turn, and `output` becomes the structured output schema. dlt counts `limits.max_tokens` after each turn, so the limit means the same on both loops.

Select the loop on the job with `loop="claude-agent-sdk"`, or for a single run with `-c agent.loop=claude-agent-sdk`. On `claude-agent-sdk` the workspace's `CLAUDE.md` loads as in any Claude Code session. The project's `.claude/rules` and `.mcp.json` aren't loaded. The agent receives the rules and the MCP server it declares.

## Guardrails

- Tools follow the `access` declaration. An agent without `access` receives no file tools and no shell. MCP tools declare the access they require, and a tool the grant doesn't cover isn't offered to the model.
- An agent runs no code unless its definition grants `local: execute`. Without that verb it has no `Bash` and no `RunPython`, so it can read and reason but can't run anything on the runner. The agents the harness ships don't grant it.
- Credential files are never readable by a file tool: `*secrets.toml`, `.env`, `.env.*`, on both loops, whatever `local` grants.
- SQL through the MCP server is limited to a single `SELECT` statement per call.
- `execute` runs in the job's own process. A shell runs in the same process tree and virtual environment as the job, with the job's credentials on the runner. It also reaches around the file tools' credential rules. Grant it only to agents that need it, and give an agent with `execute` and data access an explicit rule never to write data.
- `data` is a grant you make deliberately: it opens the workspace data to a model-driven process. The agents the harness ships declare `local: read` and `context: read` and no `data`, and build a diagnosis from run records, logs, job definitions, telemetry, and workspace source.
- An agent job declaring no profile runs on the read-only `access` profile, so the production credentials stay out of its environment. Pin `require={"profile": "access"}` to state it in the code. See [Profile of an agent job](#profile-of-an-agent-job).

## Next steps

- [Agent definitions](agent-definitions.md) covers the `AGENT.md` and Python function forms
- [Job inspector agent](job-inspector.md) is the verified agent that diagnoses failed job runs
- [Toolkits](../ai-harness/toolkits.md) shows the `dlthub-platform` toolkit that ships `job-inspector`
- [Triggers and scheduling](../pipeline-operations/triggers.md) covers the schedule, interval, and follow-up triggers available to all jobs
- [Job configuration](../pipeline-operations/job-configuration.md) covers `execute`, `require`, `expose`, and TOML sections
- [Monitoring and debugging](../pipeline-operations/monitoring.md) shows how to list runs and read their results
