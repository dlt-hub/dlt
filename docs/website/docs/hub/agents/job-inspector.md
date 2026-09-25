---
title: Job inspector agent
description: Verified dltHub agent that diagnoses a failed job run and reports a classification with evidence and a proposed fix
keywords: [dlthub platform, agents, job inspector, failed job run, diagnosis, dlthub-platform toolkit, job.fail]
---
# Job inspector agent

:::warning
This feature is in private preview
:::

`job-inspector` is an agent definition shipped with the [`dlthub-platform`](../ai-harness/toolkits.md#dlthub-platform) toolkit. It runs when a job fails, reads the run record, the logs, and the job definition, follows the traceback into the workspace source and a missing input back to the job that produces it, and reports a classification of the failure with evidence and a fix naming the target and the change. It inspects any batch job and doesn't change code or data.

You declare it like any other agent job. [Background agents](index.md) covers the mechanics this page builds on.

## Install the toolkit

Meet the [prerequisites](index.md#prerequisites) for agent jobs, then install the toolkit:

```sh
uv run dlthub ai toolkit install dlthub-platform
```

## Quick start: inspect failed jobs

Declare the `job-inspector` agent as a job in `__deployment__.py` and point its trigger at the jobs you want it to watch:

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

The job is named after the agent definition, `job_inspector`. `require={"profile": "access"}` keeps the production credentials out of the job's environment. See [Profile of an agent job](index.md#profile-of-an-agent-job). Run it locally against a run that already failed, then deploy:

```sh
# a single manual run, on a failed run id from `dlthub job runs list`
dlthub local run job_inspector -c failed_run_id=<run-id>

# push the job graph; every failed job tagged "ingest" now starts an inspector run
dlthub deploy
```

The transcript streams to your terminal. When the run ends you get a job result with a `status`, a Markdown `summary`, and the inspector's own fields (`classification`, `confidence`, `evidence`, `proposed_fix`, `fix_target`, `fix_change`, `open_points`, `requires_human`). On the platform the result appears on the failed run's page, because the agent reported that run as the entity it acted on.

## Agent inputs

| Input            | Meaning                                                                               |
| ---------------- | ------------------------------------------------------------------------------------- |
| `failed_run_id`  | Run id of the failed job run to inspect                                               |
| `failed_job_ref` | Job ref of the failed job. Its latest failed run is inspected when no run id is given |

Both are optional, and a run resolves them in order:

1. With a run id, that run is inspected.
2. With a job ref, its latest failed run is inspected.
3. With a `job.fail:<job ref>` trigger, the latest failed run of that job is inspected.
4. With none of them, the run ends with `status: aborted` and a `summary` naming the inputs that were empty.

A run started from a `job.fail:` trigger arrives with both inputs empty. The trigger string names the failed job, and the agent takes the job ref from `{{ run_context.trigger }}` in step 3. A run you start manually takes the inputs you give it on the command line or in configuration.

Only `job.fail:` resolves in step 3. A run started from a `job.success:` trigger reaches step 4 and ends `aborted`, because the trigger names a job but no failed run.

### Inspect a run that succeeded

Hand the inspector a run id and it reads that run whatever its status, so a green run is inspectable:

```sh
dlthub local run job_inspector -c failed_run_id=<run-id>
```

A green run that loaded nothing gets a description of the anomaly. The inspector reports the zero-row load in `summary` and leaves `fix_target` and `fix_change` empty, because a run that completed carries no error to trace back to a setting. Catch a silent shortfall with a [data quality](../data-quality/index.md) check on the loaded row count, and let that check's failed run be what starts the inspector.

## What it reports

| Field            | Meaning                                                                                                               |
| ---------------- | --------------------------------------------------------------------------------------------------------------------- |
| `status`         | `succeeded`, `failed`, or `aborted`                                                                                   |
| `summary`        | Markdown, in three sections: `Diagnosis`, `Recommendation`, `Confidence`. See [Summary format](#summary-format)       |
| `failed_run_id`  | The run it inspected, reported as an entity                                                                           |
| `failed_job_ref` | The job whose run it inspected, reported as an entity                                                                 |
| `classification` | `config`, `credentials`, `upstream_data`, `code`, `resources`, `transient`, or `unknown`                              |
| `confidence`     | `high`, `medium`, or `low`. It's `low` whenever the classification is `unknown`                                       |
| `evidence`       | A `source`, an `excerpt`, and a `provenance` per item. The source carries the line the excerpt sits on                |
| `proposed_fix`   | What a person should do next, naming the target and the change. The agent never applies it                            |
| `fix_target`     | The one thing the fix changes: a file path, a config key, a table or resource name, a secret name, or a job ref       |
| `fix_change`     | The exact value or code change to apply to `fix_target`, such as `cursor_path="ordered_at"`. Empty when unestablished |
| `open_points`    | What the agent couldn't verify, one entry each: a tool that failed, a file it didn't find, a value it inferred        |
| `requires_human` | Whether the fix needs a person to act                                                                                 |

`provenance` says what kind of artifact an excerpt is: `run_log`, `run_record`, `trace`, `job_definition`, `workspace_file`, `secrets_redacted`, `destination_query`, `repository_comment`, `job_description`, or `inference`. The first seven are facts and the last three are claims, so `confidence: high` rests on at least one fact.

On the platform the result appears on the failed run's page, because the agent reports that run as the entity it acted on. [Read the agent run result](index.md#read-the-agent-run-result) shows the full result envelope and an inspector result in it.

### Summary format

`summary` holds three headings, each over short bullets:

| Heading             | What the bullets answer                                                                                                                                  |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `## Diagnosis`      | The root cause: what failed, where, and why. One bullet quotes the evidence line carrying it. For a pipeline job the first bullet names the failing step |
| `## Recommendation` | The next action, written as the instruction itself and starting with its verb (`Set`, `Change`, `Unpause`), one action per bullet                        |
| `## Confidence`     | The limits of the diagnosis: every entry of `open_points`, and why this confidence                                                                       |

The format is meant to be pasted whole into a coding agent, so the Recommendation names the target and the value rather than asking the reader to investigate.

## Defaults and overrides

The definition ships these defaults. The agent job and the individual run override them.

| Setting         | Default                                                                                  |
| --------------- | ---------------------------------------------------------------------------------------- |
| `trigger`       | `job.fail:*`, every failed job in the workspace                                          |
| `limits`        | `max_turns: 30`, `max_tokens: 1000000`                                                   |
| `loop_run_args` | `retries: 2`, the number of times pydantic-ai lets the model correct a failing tool call |

The definition names no model, so the run takes the `sonnet` default or the model you set on the job. Give it one at least as capable as Claude Sonnet 5.

:::warning
Narrow the trigger as soon as a second agent job is deployed. `job.fail:*` matches every batch job in the workspace, agent jobs included. The declaring job is excluded, so the inspector never triggers on its own failures, but two agents both watching `job.fail:*` do trigger each other: a failed run of A starts B, a failed run of B starts A, and the pair keeps going. A tag or section selector such as `job.fail:tag:ingest` scopes the inspector to the jobs you want watched. Excluding agent jobs from wide selectors is planned.
:::

Narrow the trigger and change the settings on the job:

```py notype
inspector = run.agent(
    "dlthub-platform:job-inspector",
    trigger="job.fail:tag:ingest",
    require={"profile": "access"},
    model="opus",
    limits={"max_turns": 20},
    instructions="focus on the loader step",
)
```

Or for a single run:

```sh
dlthub local run job_inspector -c failed_run_id=<run-id> -c agent.model=opus
```

[Triggers for agents](index.md#triggers-for-agents) lists the selectors a `job.fail:` trigger accepts.

## Guardrails

The definition grants `local: [read]` and `context: [read]`. The agent reads workspace files with `Read`, `Glob`, and `Grep`, and reads runs, logs, job definitions, and telemetry through the dltHub MCP server. It has no shell, by design: the credential deny rules cover the file tools only, so `execute` would be a way around them and a way to rerun the job under inspection. The body rules it read-only on top of that: it doesn't edit code, deploy, cancel, or rerun a job, and a person applies the proposed fix.

The definition declares no `data` axis. A diagnosis is built from run records, logs, job definitions, the dlt trace, and workspace source, and the agent can't query your destination. When the cause turns on what a table holds, the agent puts that in `open_points` and names the query that would settle it.

Pin `require={"profile": "access"}` on the job. An agent job is a batch job, so without the pin it runs on `prod` with the production credentials in its environment. See [Profile of an agent job](index.md#profile-of-an-agent-job).

## Next steps

- [Background agents](index.md) covers declaring, running, and deploying agent jobs
- [Toolkits](../ai-harness/toolkits.md#dlthub-platform) shows the rest of the `dlthub-platform` toolkit
- [Triggers and scheduling](../pipeline-operations/triggers.md) covers the triggers available to all jobs
- [Monitoring and debugging](../pipeline-operations/monitoring.md) shows how to list runs and read their results
