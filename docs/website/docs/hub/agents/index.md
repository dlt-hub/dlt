---
title: Background agents
description: Run background agents as jobs on the dltHub platform, for example to diagnose failed job runs
keywords: [dlthub platform, agents, background agents, agent job, AGENT.md, run.agent, job inspector, pydantic-ai, claude-agent-sdk, toolkits]
---
# Background agents

:::info
This feature is in public preview
:::

An agent job is a dltHub job that runs an AI agent loop. It runs unattended on a schedule, after another job fails, or when you start it from the CLI or the Web UI. It can read the workspace. It can query runs and logs through the dltHub Model Context Protocol (MCP) server. It returns a job result, which shows on the page of each entity that the agent run acted on.

Agent jobs are declared, run, and deployed like every other job: in `__deployment__.py`, with `dlthub local run` locally and `dlthub deploy` on the platform. [Deployments](../pipeline-operations/deployments.md), [Triggers and scheduling](../pipeline-operations/triggers.md), and [Job configuration](../pipeline-operations/job-configuration.md) apply to agent jobs too.

This page covers how to declare an agent job and how to run it locally and on the platform. [Agent definitions](agent-definitions.md) covers the agent definition itself. The examples use `job-inspector`, the agent definition that the [`dlthub-platform`](../ai-harness/toolkits.md#dlthub-platform) toolkit ships. See [Job inspector agent](job-inspector.md) for what it does and how to declare it.

## Terms

| Term             | Definition                                                                                                                                                                          | Where it lives                                                        |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| Agent definition | System prompt plus a declaration of the inputs, the output, the MCP feature groups, the skills, the rules, and the access of the agent                                              | `AGENT.md` file, or a decorated Python function                       |
| Agent loop       | Framework that runs the model turn by turn: `pydantic-ai` (default) or `claude-agent-sdk`                                                                                           | Selected with `loop=` on `run.agent` or `agent.loop` in configuration |
| Agent job        | Definition plus the settings for your workspace: model, limits, trigger, instructions, loop                                                                                         | `run.agent(...)` in `__deployment__.py`                               |
| Agent run        | Execution of the agent job. It receives inputs and returns an agent output and an agent trace                                                                                       | Started by a trigger, `dlthub local run`, `dlthub run`, or the Web UI |
| Access axis      | Area of the workspace that `access` covers: `local` for the files and the shell, `data` for the data in your destinations, `context` for runs, logs, job definitions, and telemetry | Key of `access` in the agent definition                               |
| Verb             | What the agent can do on an axis: `local` takes `read`, `write`, `execute`, `network`; `data` takes `read`, `write`; `context` takes `read`. `all` grants every verb of its axis    | Listed under the axis in `access`                                     |

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

`run.agent(...)` declares a job from an agent definition. The first argument is the agent definition, in one of three forms:

- a `<toolkit>:<agent>` reference to an installed agent definition
- a workspace-relative path to a folder that holds an `AGENT.md`
- a `TAgentSpec` dict, declared inline

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

The job is named after the agent definition (`job-inspector` becomes `job_inspector`) in the declaring module's section. Every argument overrides the matching entry of the definition's `defaults`. The job takes no trigger from the agent definition: an agent job without `trigger=` runs only when you start it.

You can call the agent job in process, for example in a script or a test. An agent job without a function returns a coroutine, so await it: `await inspector(failed_run_id="...")`. It takes its inputs as keyword arguments only. An `async def` decorated function also returns a coroutine, and a sync decorated function returns its value directly. The call returns the agent output, not the job result, and sends nothing to the platform. See [Test agent jobs](#test-agent-jobs).

`access`, `tools`, `skills`, and `rules` aren't `defaults`. An agent job without a function keeps the lists that its agent definition declares, and `run.agent` refuses the arguments for them with a `TypeError`. A decorated function that drives an agent definition (`agent=`) works the other way. Its argument replaces the list of the agent definition, so `access={"local": ["read"]}` on such a function removes `context: read`. Pass every access axis the agent needs, or leave the block to the agent definition.

The inputs of such a function merge with the agent definition instead. The parameters of the function decide which inputs exist. For each parameter, what the signature says wins, and the agent definition fills what it leaves out: the description, `entity_type`, other attributes, and the type when the parameter has no annotation. The inputs that the function doesn't take are dropped.

Don't give the production profile to an agent job. On the platform, an agent job takes the read-only `access` profile by default, and the example pins it. See [Profile of an agent job](#profile-of-an-agent-job).

| Argument                               | Meaning                                                                                                                                                                                                                    |
| -------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `instructions`                         | First user message of each run. Use it for the task at hand. The system prompt describes the agent                                                                                                                         |
| `model`                                | `provider:model` id such as `anthropic:claude-sonnet-5`, or an alias. See [Model and credentials](#model-and-credentials)                                                                                                  |
| `limits`                               | `max_turns` and `max_tokens` per run. The loop ends the run when either is exhausted. Without a limit, `pydantic-ai` stops at 50 turns and `claude-agent-sdk` at 30, with no token limit                                   |
| `loop`                                 | `"pydantic-ai"` (default) or `"claude-agent-sdk"`. See [Agent loops](#agent-loops)                                                                                                                                         |
| `loop_run_args`                        | Arguments passed to the framework, merged over the definition's defaults. On `pydantic-ai`, `retries` sets how often the model retries a failing tool call, 0 by default. After that, the call fails and the run continues |
| `verbosity`                            | How much of the run is printed, `0` to `2`, `1` by default. See [Read the agent run result](#read-the-agent-run-result)                                                                                                    |
| `emojis`                               | Marks tool calls, results and the outcome in the printed run with emojis. `true` by default, `false` prints labels and arrows                                                                                              |
| `inputs_validator`                     | Called with the resolved inputs, `run_context` included, before the run. Its return value replaces the inputs and `None` keeps them. Use it to derive an input such as a run id from a job ref                             |
| `outputs_validator`                    | Called with the agent output after the run. Its return value replaces the output and `None` keeps it                                                                                                                       |
| `name`, `section`                      | Job name and configuration section, as on every job                                                                                                                                                                        |
| `execute`                              | `timeout` and `concurrency`. Keys you leave out come from `defaults.execute` of the agent definition. An agent job runs up to 5 runs at once (other jobs 1). `concurrency: None` removes the limit                         |
| `trigger`, `expose`, `require`, `spec` | Standard job options. See [Triggers and scheduling](../pipeline-operations/triggers.md) and [Job configuration](../pipeline-operations/job-configuration.md)                                                               |

Only an agent job without a function takes the two validators. On a decorated function, `run.agent` raises `TypeError`. A decorated function runs the loop itself and gives the inputs to `loop.run()`. When the agent definition has its own `agent.py`, its functions run first and the job's validators run on their result. See [Agent code in `agent.py`](agent-definitions.md#agent-code-in-agentpy).

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

The section is the filename. The pipeline name, `"github"` here, never appears in the ref. You can set the section with `section="ingest"` on the decorator. Without it, a job written inline in `__deployment__.py` gets the section `__deployment__`. `dlthub job list` prints the refs of a deployment.

#### Selectors

A selector matches a set of jobs at once. It takes the same forms `dlthub job trigger` takes:

| Selector                            | Matches                                               |
| ----------------------------------- | ----------------------------------------------------- |
| `jobs.github_pipeline.load_commits` | That one job                                          |
| `jobs.github_pipeline.*`            | Every job declared in `github_pipeline.py`            |
| `tag:ingest`                        | Every job tagged `ingest`, wherever it is declared    |
| `pipeline_name:*`                   | Every job declared with `@run.pipeline`               |
| `pipeline_name:github`              | Every `@run.pipeline` job that runs pipeline `github` |
| `batch:`                            | Every batch job                                       |
| `*`                                 | Every job in the workspace                            |

So `trigger="job.fail:tag:ingest"` starts the agent job whenever a job tagged `ingest` fails, `trigger="job.fail:pipeline_name:*"` starts it whenever any pipeline job fails, and `trigger="job.success:jobs.github_pipeline.load_commits"` starts it when `load_commits` succeeds.

`pipeline_name:*` is a safe way to watch every pipeline in the workspace: unlike `*` it never matches an agent job, because only `@run.pipeline` declares a pipeline.

A selector expands at deploy time to a follow-up trigger per matching job. The declaring job itself and interactive jobs are excluded. A manual run arrives with a `manual:` trigger and only the inputs that it was given. As a result, the system prompt must say what to do when an input is empty.

An agent job that only runs when you start it takes no `trigger=` at all. dlt adds the `manual:` trigger itself, so `trigger.manual()` is not something you pass: it raises `InvalidTrigger: manual: triggers are added automatically`.

### Profile of an agent job

A [profile](../pipeline-operations/profiles.md) names the set of credentials a job runs with. The profile isn't the `access` declaration. `access` selects the tools that the model gets. The profile selects the credentials that the job process holds. An agent job that declares `data: write` and runs on the `access` profile can't write, because its credentials can't.

The profile covers profile-scoped configuration: `prod.secrets.toml`, `prod.config.toml`, and a variable set with `dlthub variable set --profile prod`. A variable set with `--workspace` has no profile, and it reaches the job on every profile. A secret that an agent must not get belongs in a profile scope. `dlthub variable list` prints the scope of each one.

On the platform, an agent job that declares no profile runs on the read-only `access` profile, so the production credentials stay out of its environment. Other batch jobs use `prod` by default. The agent job is the exception. dlt itself doesn't set the profile: `dlthub local run` uses the active profile, and warns when the workspace has no `access` profile.

Pin the profile on the job, to state the intent in the code:

```py notype
inspector = run.agent(
    "dlthub-platform:job-inspector",
    trigger="job.fail:tag:ingest",
    require={"profile": "access"},
)
```

A declared profile always wins, `prod` included, so nothing stops `require={"profile": "prod"}` on an agent job. Don't give an agent job the production profile. An agent job runs unattended, and a model decides what to do. The production profile gives that model the credentials to change your data. Work that needs production write credentials belongs in a pipeline or a plain job that a person wrote and reviewed.

:::warning
Before you run an agent job, make sure that the destination credentials in your `access` profile are read-only at the destination itself. Use a read-only database role, or a storage key without write permission. dltHub doesn't check what the credentials of a profile can do. An `access` profile that holds a writable credential gives the agent write access under a read-only name. [Define profiles](../pipeline-operations/profiles.md#define-profiles) covers where each profile's credentials live.
:::

Manifest validation refuses a local-only profile (`dev`, `tests`) and takes any other name as given, so a typo surfaces as missing credentials at run time. On `dlthub local run` the declaration is a warning rather than a switch: the run uses the active profile and reports the mismatch.

## Run an agent job

Each trigger of the agent job produces an agent run. You can also start a run manually, the way you run any job locally or on the platform:

```sh
dlthub local run job_inspector -c failed_run_id=<run-id>     # locally
dlthub run job_inspector -f                                  # on the platform
```

`-c` is a local flag. `dlthub run` does not take it, and a workspace variable holding an input key does not reach a remote run either. A remote run takes its inputs from the trigger that started it, or from the job's `config.toml` as deployed. To point a remote agent run at one specific run id today, put the value in `config.toml` and deploy, or run it locally.

You can override settings for a single local run. Inputs and agent settings are job configuration in the section of the job. The same keys work on the command line, in `config.toml`, and in the environment:

| What                                                 | Key                            | Example                                                                                                                 |
| ---------------------------------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------------------- |
| Declared inputs                                      | `jobs.<section>.<job>.<input>` | `-c failed_run_id=...`                                                                                                  |
| Instructions, model, limits, loop, verbosity, emojis | `jobs.<section>.<job>.agent.*` | `-c agent.instructions="explain, do not fix"`, `-c agent.max_turns=10`, `-c agent.verbosity=2`, `-c agent.emojis=false` |

```toml
# .dlt/config.toml
[jobs.__deployment__.job_inspector]
failed_job_ref = "jobs.github_pipeline.load_commits"

[jobs.__deployment__.job_inspector.agent]
model = "sonnet"
max_turns = 20
verbosity = 0
```

Each source overrides the ones before it: the loop default, the definition's `defaults`, the `run.agent` argument, the run's configuration. `limits` and `loop_run_args` merge key by key, so setting `max_turns` keeps the `max_tokens` of the source below.

`dlthub -v local run <job>` sets `agent.verbosity` to `2` for that run, unless `-c agent.verbosity=...` sets it.

Ctrl-C on a local run, or a stop on the platform, ends the run at the next turn.

### Model and credentials

`model` is a `provider:model` id in the naming pydantic-ai uses, or an alias for one:

| Provider     | `model`                                                                            | Alias                              |
| ------------ | ---------------------------------------------------------------------------------- | ---------------------------------- |
| Azure OpenAI | `azure:<deployment name>`                                                          | none                               |
| Anthropic    | `anthropic:claude-sonnet-5`, `claude-opus-5`, `claude-haiku-4-5`, `claude-fable-5` | `sonnet`, `opus`, `haiku`, `fable` |
| OpenAI       | `openai:gpt-5.5`, `openai:gpt-5.4-mini`, `openai:gpt-5.4-nano`                     | `gpt`, `gpt-mini`, `gpt-nano`      |
| Google       | `google:gemini-3.5-flash`, `google:gemini-3.1-pro-preview`                         | `gemini`, `gemini-pro`             |

If the job, the agent definition, and the configuration name no model, the loop uses `sonnet`. The agent definitions that a toolkit ships name no model, so the workspace that deploys the agent job selects the model for its provider.

Azure OpenAI addresses a deployment on your own endpoint, not a shared model. It has no alias, and it needs `api_url` and `api_version` together with the model and the key. The `claude-agent-sdk` loop runs Anthropic models only, so naming it in a workspace whose key is Azure or Google breaks the run.

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

Azure is the only provider pydantic-ai gives `api_version`. Other providers ignore it and log a warning.

On the platform the runtime can supply a model endpoint of its own. `model`, `api_key`, `api_url`, and `api_version` are one set. If you set one of them, the run takes all four from your configuration and ignores the endpoint of the runtime. If you set only `api_key`, the run uses the model of the job, of the agent definition, or the loop default, not the model of the runtime. It sends the request to the public endpoint of the provider, so a key issued for the runtime's endpoint fails with `401 API key is invalid`. The run logs which endpoint it used.

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
Check what a wide selector matched before you deploy a second agent job. `job.fail:*` and `job.fail:batch:` match every batch job in the workspace, agent jobs included. Don't let two agent jobs watch `job.fail:*`. A failed run of one starts the other, and the two jobs start each other until you archive one. The declaring job is excluded, so an agent job doesn't start on its own failure. Scope each agent job with a tag, a section or a pipeline selector, such as `job.fail:tag:ingest` or `job.fail:pipeline_name:*`. `dlthub deploy --show-manifest` prints the concrete triggers a selector expanded to. Excluding agent jobs from wide selectors is planned.
:::

## Read the agent run result

An agent run leaves three things behind, and they answer different questions:

| What        | Where                                                                                  | Holds                                                                                                                                                                                                              |
| ----------- | -------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Run log     | Your terminal on a local run, the run's **Logs** in the Web UI, `dlthub job runs logs` | Everything the run printed as it went: the model's reasoning, its messages, each tool call and what it returned, and a closing line listing the tools, skills, and MCP tools used. `agent.verbosity` sets how much |
| Agent trace | The run's **Trace** in the Web UI, and `trace` in the result below                     | The structured record of the same run: model, limits, resolved inputs, the tools that were wired, turn and token counts, per-turn tool calls                                                                       |
| Job result  | The run's **Summary** in the Web UI, `dlthub job runs info`                            | What the agent returned: `status`, `summary`, and the fields its `output` schema declares                                                                                                                          |

The log is what the run printed, so you read it to follow what the agent did and why. The agent trace is queryable, so you read it to count turns and tokens or to see which tools a run got.

`agent.verbosity` controls how much reaches the log:

- `0`: the prompt, the agent's messages, the tool names, tool results cut to 80 characters, and the outcome
- `1` (default): adds the agent's thoughts and the tool arguments, each on one line and cut to 200 characters, and tool results cut to 200 characters
- `2`: prints thoughts, arguments, and results in full, and adds the rendered system prompt

The log is plain text. Emojis mark its parts: 📝 the prompt, 📜 the system prompt, 🔧 a tool call, 🌐 an MCP tool call, ✅ and ❌ a tool result and the outcome, ❗ an abort, 🏁 a finish without a status, 💭 the agent's thoughts, 💬 the agent speaking, 🎁 the job result. Set `agent.emojis` to `false` to print labels and arrows instead.

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
    { "type": "job-runs", "id": "job-runs/<run-id>" },
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
- `result` is the agent output as the `output` schema of the agent definition declares it.
- `object` lists the entities the run acted on. On the platform, the job result appears on the page of each entity.
- `trace` is the agent trace. It records the model, the limits, the resolved inputs, and the tools that the loop wired. It also records the skills and MCP tools used, the turn and token counts, and the tool calls of each turn.

On the platform, inspect agent runs like any other run with `dlthub job runs list` and `dlthub job runs info`. See [Monitoring and debugging](../pipeline-operations/monitoring.md).

## Agent loops

A loop is the framework that runs the agent. dltHub ships two agent loops and adds the matching dependency group to the job, so the runner installs it. A third-party loop can register through the `plug_agent_loop` plugin hook.

|                 | `pydantic-ai` (default)                                                                                        | `claude-agent-sdk`                                     |
| --------------- | -------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| Models          | Any provider pydantic-ai supports                                                                              | Anthropic models, through a bundled Claude Code CLI    |
| Local tools     | dlt's own file, search, and shell tools. Web access comes from the model provider's own search and fetch tools | Claude Code's tools, under the same names              |
| Skills          | Inlined into the system prompt                                                                                 | Listed by name and loaded on demand, as in Claude Code |
| Install locally | `uv add "pydantic-ai-slim[anthropic,openai,google,mcp,spec]"`                                                  | `uv add claude-agent-sdk`                              |

Both loops read the same declarations. `access` selects the local tools and `tools` selects the MCP feature groups. The rendered body becomes the system prompt, `instructions` becomes the user turn, and `output` becomes the structured output schema. dlt counts `limits.max_tokens` after each turn, so the limit means the same on both loops. A turn is one model response, and its input tokens include the tokens read from and written to the prompt cache. The run stops after the turn that passes the limit.

Select the loop on the job with `loop="claude-agent-sdk"`, or for a single local run with `-c agent.loop=claude-agent-sdk`. The dependency group the job declares follows `loop=`, so on the platform switch the loop with `loop=` and deploy. On `claude-agent-sdk` the workspace's `CLAUDE.md` loads as in any Claude Code session. The project's `.claude/rules` and `.mcp.json` aren't loaded. The agent receives the rules and the MCP server it declares.

## Guardrails

- Tools follow the `access` declaration. An agent without `access` receives no file tools and no shell. MCP tools declare the access they require, and the model isn't offered a tool that the declared access doesn't cover.
- An agent runs no code on the runner unless its agent definition declares `local: execute`. Without that verb, the agent has no `Bash` and no `RunPython`. The agent definitions that the dltHub AI Harness ships don't declare it. Model providers can give a model server-side capabilities on any loop or model, and `access` doesn't control them. For example, pydantic-ai with recent Anthropic models and `network` gets web tools that run code in Anthropic's sandbox. See [Access declaration](agent-definitions.md#access-declaration).
- Credential files are never readable by a file tool: `*secrets.toml`, `.env`, `.env.*`, on both loops, whatever `local` grants.
- SQL through the MCP server is limited to a single `SELECT` statement per call.
- `execute` runs in the job's own process. A shell runs in the same process tree and virtual environment as the job, with the job's credentials on the runner. It also reaches around the file tools' credential rules. Declare it only in agent definitions that need it. If an agent definition declares `execute` and `data`, give it a rule that forbids writing data.
- `data` is an access axis that you declare deliberately: it opens the workspace data to a model-driven process. The agent definitions that the dltHub AI Harness ships declare `local: read` and `context: read` and no `data`. They build a diagnosis from run records, logs, job definitions, telemetry, and workspace source.
- On the platform, an agent job declaring no profile runs on the read-only `access` profile, so the production credentials stay out of its environment. Pin `require={"profile": "access"}` to state it in the code. See [Profile of an agent job](#profile-of-an-agent-job).

## Test agent jobs

You test an agent job like other Python code. To a developer, an agent job is a function: it takes input arguments and returns a value. That's an advantage over a skill, which you can only test in a chat. A decorated function and an agent job without a function are tested the same way. You await an agent job without a function and an `async def` decorated function. You call a sync decorated function without `await`.

```py notype nolint
import pytest

from my_agents import crash_inspector  # an agent job without a function, or an async def decorated function


@pytest.mark.asyncio
async def test_crash_inspector() -> None:
    run_context = {"run_id": "r-test", "trigger": "job.fail:jobs.ingest", "refresh": False}

    # inputs as keyword arguments, the run context as you want the agent to see it
    report = await crash_inspector(failed_run_id="r-failed-42", run_context=run_context)

    # the agent output, as the agent returned it
    assert report["status"] == "succeeded"
    assert report["classification"] == "upstream_data"

    # the job result the launcher would deliver, with the agent trace and the entities
    job_result = crash_inspector.last_job_result
    assert {"type": "job-runs", "id": "job-runs/r-failed-42"} in job_result["object"]
    trace = job_result["trace"]
    assert "Bash" not in trace["tools_used"]
    assert trace["total_tokens"] < 200_000
```

In the test above:

1. Pass the inputs as keyword arguments, and a run context with the `run_id` and the `trigger` that you want to test. If you leave out `run_context`, a local run context with the trigger `manual:` is used.
2. The call returns the agent output, and you test it as any return value.
3. `last_job_result` on the agent job holds the job result of the call: `type`, `job_ref`, `status`, `summary`, `result`, the agent trace with the tools and tokens used, and `object`. dlt sends nothing to the platform. When you run several calls of the same agent job at once, for example with `asyncio.gather`, each call keeps its own job result while it runs, and `last_job_result` holds the one of the call that finished last.
4. When your agent code calls a REST API, in `agent.py` or in the decorated function, mock it as in any other Python test.
5. Unstructured fields such as `summary` can still be scored, with `jev` or a similar cheap scoring model.

## Next steps

- [Agent definitions](agent-definitions.md) covers the `AGENT.md` and Python function forms
- [Job inspector agent](job-inspector.md) is the verified agent that diagnoses failed job runs
- [Toolkits](../ai-harness/toolkits.md) shows the `dlthub-platform` toolkit that ships `job-inspector`
- [Triggers and scheduling](../pipeline-operations/triggers.md) covers the schedule, interval, and follow-up triggers available to all jobs
- [Job configuration](../pipeline-operations/job-configuration.md) covers `execute`, `require`, `expose`, and TOML sections
- [Monitoring and debugging](../pipeline-operations/monitoring.md) shows how to list runs and read their results
