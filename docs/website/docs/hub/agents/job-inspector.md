---
title: Job inspector agent
description: Verified dltHub agent that diagnoses a failed job run and reports a classification with evidence and a proposed fix
keywords: [dlthub platform, agents, job inspector, failed job run, diagnosis, dlthub-platform toolkit, job.fail]
---
# Job inspector agent

:::warning
This feature is in public preview
:::

`job-inspector` is an agent definition that the [`dlthub-platform`](../ai-harness/toolkits.md#dlthub-platform) toolkit ships. An agent job made from it diagnoses failed job runs. The agent reads the run record, the logs, and the job definition. It follows the traceback into the workspace source. It follows a missing input back to the job that produces it. It reports a classification of the failure, the evidence, and a fix that names the target and the change. The agent inspects any batch job, and it doesn't change code or data.

Set the trigger of the agent job to the jobs that it must watch. A `job.fail:` trigger selects one job, the jobs in a section, the jobs that have a tag, the pipeline jobs, or all jobs. One agent job inspects every failed job run that its trigger selects. Without `trigger=`, the agent job runs only when you start it. See [Triggers for agents](index.md#triggers-for-agents).

You declare an agent job for it, as for any other agent definition. [Background agents](index.md) covers the mechanics this page builds on.

## Install the toolkit

Make sure that you meet the [prerequisites](index.md#prerequisites) for agent jobs. Then install the toolkit:

```sh
dlthub ai toolkit install dlthub-platform
```

## Quick start: inspect failed jobs

Declare an agent job for the `job-inspector` agent definition in `__deployment__.py`. Set its trigger to the jobs that it must watch:

```py notype
"""GitHub ingest workspace with a failure inspector."""
from dlt.hub import run
from github_pipeline import load_commits

inspector = run.agent(
    "dlthub-platform:job-inspector",
    trigger="job.fail:tag:ingest",
    require={"profile": "access"},
)

__all__ = ["load_commits", "inspector"]
```

The agent job is named after the agent definition: `job_inspector`. On the platform, `require={"profile": "access"}` keeps the production credentials out of the job environment. `dlthub local run` uses the active profile, or the profile you pass with `--profile`. See [Profile of an agent job](index.md#profile-of-an-agent-job). Run the agent job locally on a job run that failed. Then deploy:

```sh
# a single manual run, on a failed run id from `dlthub job runs list`
dlthub local run job_inspector -c failed_run_id=<run-id>

# push the job graph; every failed job tagged "ingest" now starts an inspector run
dlthub deploy
```

The run log streams to your terminal as the agent works: its reasoning, each tool call, and what each tool returned. When the agent run ends, the launcher delivers a job result. `status` and a Markdown `summary` are at the top level. The full agent output is in `result`, with the fields of the inspector: `classification`, `confidence`, `evidence`, `proposed_fix`, `fix_target`, `fix_change`, `open_points`, and `requires_human`.

## Agent inputs

| Input            | Meaning                                                                                    |
| ---------------- | ------------------------------------------------------------------------------------------ |
| `failed_run_id`  | Run id of the failed job run to inspect                                                    |
| `failed_job_ref` | Job ref of the failed job. If no run id is given, the agent inspects its latest failed run |

Both inputs are optional. The agent resolves them in this order:

1. If `failed_run_id` is set, the agent inspects that job run.
2. If `failed_job_ref` is set, the agent inspects the latest failed run of that job.
3. If the trigger is `job.fail:<job ref>`, the agent inspects the latest failed run of that job.
4. If no input names a job run, the agent returns `status: aborted`, and its `summary` names the empty inputs. The launcher delivers the job result, and then the job run fails.

An agent run that a `job.fail:` trigger starts gets both inputs empty. The trigger string names the failed job, and the agent takes the job ref from `{{ run_context.trigger }}` in step 3. An agent run that you start manually gets the inputs from the command line or from configuration.

Only `job.fail:` resolves in step 3. An agent run that a `job.success:` trigger starts reaches step 4 and ends `aborted`, because the trigger names a job but no failed run.

### Inspect a run that succeeded

If you give the agent job a run id, the agent reads that job run, whatever its status. To inspect a job run that succeeded:

```sh
dlthub local run job_inspector -c failed_run_id=<run-id>
```

If a job run succeeded but loaded no rows, the agent looks for the cause as it does for a failure: it follows the empty input back to the job that produces it. If a filter or a date range in the source limited the load, the `Diagnosis` names the setting, its value, and the load. The agent can't run the source, so it can't prove that the filter is the cause.

To find a load with too few rows, add a [data quality](../data-quality/index.md) check on the loaded row count. Make the job that runs the check fail when the check fails. Then a `job.fail:` trigger on that job starts the inspector.

## What it reports

| Field            | Meaning                                                                                                                                    |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `status`         | `succeeded`, `failed`, or `aborted`                                                                                                        |
| `summary`        | Markdown, in three sections: `Diagnosis`, `Recommendation`, `Confidence`. See [Summary format](#summary-format)                            |
| `failed_run_id`  | The run it inspected, reported as an entity                                                                                                |
| `failed_job_ref` | The job whose run it inspected, reported as an entity                                                                                      |
| `classification` | `config`, `credentials`, `upstream_data`, `code`, `resources`, `transient`, or `unknown`                                                   |
| `confidence`     | `high`, `medium`, or `low`. It's `low` whenever the classification is `unknown`                                                            |
| `evidence`       | A `source`, an `excerpt`, and a `provenance` per item. The source carries the line the excerpt sits on                                     |
| `proposed_fix`   | What a person should do next, naming the target and the change. The agent never applies it                                                 |
| `fix_target`     | The one thing the fix changes: a file path, a config key, a table or resource name, a secret name, or a job ref                            |
| `fix_change`     | The exact value or code change to apply to `fix_target`, such as `cursor_path="ordered_at"`. Empty when the evidence does not establish it |
| `open_points`    | What the agent couldn't verify, one entry each: a tool that failed, a file it didn't find, a value it inferred                             |
| `requires_human` | Whether the fix needs a person to act                                                                                                      |

`provenance` says what kind of artifact an excerpt is: `run_log`, `run_record`, `trace`, `job_definition`, `workspace_file`, `secrets_redacted`, `repository_comment`, `job_description`, or `inference`. The first six values are facts. The last three values are claims. `confidence: high` needs at least one fact.

On the platform, the result appears on the page of the failed job run, because the agent reports that job run as the entity it acted on. [Read the agent run result](index.md#read-the-agent-run-result) shows the full result envelope and an inspector result in it.

### Summary format

`summary` holds three headings, each over short bullets:

| Heading             | What the bullets answer                                                                                                                                  |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `## Diagnosis`      | The root cause: what failed, where, and why. One bullet quotes the evidence line carrying it. For a pipeline job the first bullet names the failing step |
| `## Recommendation` | The next action, written as the instruction itself and starting with its verb (`Set`, `Change`, `Unpause`), one action per bullet                        |
| `## Confidence`     | The limits of the diagnosis: every entry of `open_points`, and why this confidence                                                                       |

A person can paste the whole summary into a coding agent. For this reason, each Recommendation bullet names the target and the value. It doesn't tell the reader to investigate.

## Defaults and overrides

The agent definition declares these defaults. The agent job overrides them. Configuration overrides them for one agent run.

| Setting         | Default                                                                                                                                                                                                         |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `limits`        | `max_turns: 30`, `max_tokens: 1000000`                                                                                                                                                                          |
| `loop_run_args` | `retries: 2`. pydantic-ai lets the model call a failing tool again two times. After this, the tool call fails, the model sees the failed call, and the agent run continues. `claude-agent-sdk` ignores this key |

The agent definition declares no trigger. An agent job without `trigger=` runs only when you start it. To inspect every failed job in the workspace, declare `trigger="job.fail:*"`, and read the warning below first.

The agent definition names no model. The model comes from the agent job or from configuration, and the key is yours on the platform as it is locally. If neither sets a model, the loop uses `sonnet`. Give it one at least as capable as Claude Sonnet 5. See [Model and credentials](index.md#model-and-credentials).

:::warning
Do not give two agent jobs the trigger `job.fail:*`. `job.fail:*` selects every batch job in the workspace, agent jobs included. It never selects the job that declares it, so the inspector never triggers on its own failures. But a failed run of agent job A starts agent job B, and a failed run of B starts A. The loop does not stop. An agent run that ends `aborted` also counts as a failed job run. To limit the inspector to the jobs that it must watch, use a tag or section selector such as `job.fail:tag:ingest`, or `job.fail:pipeline_name:*` to watch every job declared with `@run.pipeline`. Excluding agent jobs from wide selectors is planned.
:::

Narrow the trigger and change the settings on the agent job:

```py notype
inspector = run.agent(
    "dlthub-platform:job-inspector",
    trigger="job.fail:tag:ingest",
    require={"profile": "access"},
    model="sonnet",
    limits={"max_turns": 20},
    instructions="focus on the loader step",
)
```

To change a setting for one agent run, pass it with `-c`:

```sh
dlthub local run job_inspector -c failed_run_id=<run-id> -c agent.model=sonnet
```

[Triggers for agents](index.md#triggers-for-agents) lists the selectors a `job.fail:` trigger accepts.

## Guardrails

- **Reads, never writes.** The agent definition declares `local: [read]` and `context: [read]`. The agent gets `Read`, `Glob`, and `Grep` over workspace files. It declares the feature groups `jobs`, `logs`, `telemetry`, `workspace`, `secrets`, and `config`, so through the dltHub MCP server it reads runs, logs, job definitions, telemetry, and the names and redacted values of secrets and variables. `secrets` serves no tool that writes, because writing one needs `local: write`.
- **No shell.** The agent definition declares no `execute` verb. The deny rules for credential files apply only to the file tools. A shell can read those files, and it can run the inspected job again.
- **No destination access.** The agent definition declares no `data` axis, so the agent can't query your data. A diagnosis uses run records, logs, job definitions, the dlt trace, workspace source, and the redacted view of secrets and variables. When the cause depends on the data in a table, the agent adds this to `open_points`. It names the table or the record that a person must read.
- **No changes to your workspace.** The system prompt also forbids changes, in addition to the declared access: the agent doesn't edit code, deploy, cancel, or rerun a job. A person applies the proposed fix.
- **Read-only credentials.** The job runs on the `access` profile, which an agent job takes by default, so the production credentials stay out of its environment. Pin `require={"profile": "access"}` to state it in the code. See [Profile of an agent job](index.md#profile-of-an-agent-job).

## Next steps

- [Background agents](index.md) covers declaring, running, and deploying agent jobs
- [Toolkits](../ai-harness/toolkits.md#dlthub-platform) shows the rest of the `dlthub-platform` toolkit
- [Triggers and scheduling](../pipeline-operations/triggers.md) covers the triggers available to all jobs
- [Monitoring and debugging](../pipeline-operations/monitoring.md) shows how to list runs and read their results
