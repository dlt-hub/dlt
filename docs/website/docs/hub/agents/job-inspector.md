---
title: Job inspector agent
description: Verified dltHub agent that diagnoses a failed job run and reports a classification with evidence and a proposed fix
keywords: [dlthub platform, agents, job inspector, failed job run, diagnosis, dlthub-platform toolkit, job.fail]
---

# Job inspector agent

:::warning
This feature is in private preview
:::

`job-inspector` is an agent definition shipped with the [`dlthub-platform`](../ai-harness/toolkits.md#dlthub-platform) toolkit. It runs when a job fails, reads the run record, the logs, and the job definition, and reports a classification of the failure with evidence and a proposed fix. It inspects pipeline jobs and agent jobs alike and doesn't change code or data.

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
)

__all__ = ["load_commits", "inspector"]
```

The job is named after the agent definition, `job_inspector`. Run it locally against a run that already failed, then deploy:

```sh
# a single manual run, on a failed run id from `dlthub job runs list`
dlthub local run job_inspector -c failed_run_id=<run-id>

# push the job graph; every failed job tagged "ingest" now starts an inspector run
dlthub deploy
```

The transcript streams to your terminal. When the run ends you get a job result with a `status`, a Markdown `summary`, and the inspector's own fields (`classification`, `confidence`, `evidence`, `proposed_fix`, `requires_human`). On the platform the result appears on the failed run's page, because the agent reported that run as the entity it acted on.

## Agent inputs

| Input | Meaning |
|-------|---------|
| `failed_run_id` | Run id of the failed job run to inspect |
| `failed_job_ref` | Job ref of the failed job. Its latest failed run is inspected when no run id is given |

Both are optional, and a run resolves them in order:

1. With a run id, that run is inspected.
2. With a job ref, its latest failed run is inspected.
3. With a `job.fail:<job ref>` trigger, the latest failed run of that job is inspected.
4. With none of them, the run ends with `status: aborted` and a `summary` naming the inputs that were empty.

A run started from a `job.fail:` trigger arrives with the failed job filled in, and a run you start manually takes the inputs you give it on the command line or in configuration.

## What it reports

| Field | Meaning |
|-------|---------|
| `status` | `succeeded`, `failed`, or `aborted` |
| `summary` | Markdown account of the diagnosis |
| `failed_run_id` | The run it inspected, reported as an entity |
| `classification` | `config`, `credentials`, `upstream_data`, `code`, `resources`, `transient`, or `unknown` |
| `confidence` | `high`, `medium`, or `low`. It's `low` whenever the classification is `unknown` |
| `evidence` | A source and an excerpt per piece of evidence, taken from logs and run records |
| `proposed_fix` | What a person should do next. The agent never applies it |
| `requires_human` | Whether the fix needs a person to act |

On the platform the result appears on the failed run's page, because the agent reports that run as the entity it acted on. [Read the agent run result](index.md#read-the-agent-run-result) shows the full result envelope and an inspector result in it.

## Defaults and overrides

The definition ships these defaults. The agent job and the individual run override them.

| Setting | Default |
|---------|---------|
| `trigger` | `job.fail:*`, every failed job in the workspace |
| `model` | `sonnet` |
| `limits` | `max_turns: 30`, `max_tokens: 1000000` |

Narrow the trigger and change the settings on the job:

```py notype
inspector = run.agent(
    "dlthub-platform:job-inspector",
    trigger="job.fail:tag:ingest",
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

The definition grants read access to the workspace, to data, and to runs, logs, and job definitions through the dltHub MCP server. The agent explains a failure. It doesn't edit code, deploy, or rerun a job, and a person applies the proposed fix.

## Next steps

- [Background agents](index.md) covers declaring, running, and deploying agent jobs
- [Toolkits](../ai-harness/toolkits.md#dlthub-platform) shows the rest of the `dlthub-platform` toolkit
- [Triggers and scheduling](../pipeline-operations/triggers.md) covers the triggers available to all jobs
- [Monitoring and debugging](../pipeline-operations/monitoring.md) shows how to list runs and read their results
