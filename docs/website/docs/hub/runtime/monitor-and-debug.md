---
title: Monitoring and debugging
description: Monitor pipeline health, view logs, and diagnose failures on the dltHub platform
keywords: [monitoring, observability, logs, debug, run status, metrics, dlthub platform]
---

# Monitoring and debugging

Use the dltHub CLI and the Web UI at [app.dlthub.com](https://app.dlthub.com) to monitor pipeline health, inspect logs, and diagnose failures.

## Check workspace status

Retrieve a workspace summary from the CLI:

```sh
dlthub workspace info
```

The command returns the workspace name, job count, latest run status, and the latest deployment and configuration versions. See [`dlthub workspace info`](../command-line-interface.md#dlthub-workspace-info).

## View logs

### From the CLI

Display logs for the latest run of a job:

```sh
dlthub job logs my_pipeline.py
```

Display logs for a specific run number:

```sh
dlthub job logs my_pipeline.py 3
```

To stream logs in real time while a run is in progress, pass `--follow`, or supply it directly to [`dlthub run`](../command-line-interface.md#dlthub-run).

For all options, see [`dlthub job logs`](../command-line-interface.md#dlthub-job-logs) and [`dlthub job runs logs`](../command-line-interface.md#dlthub-job-runs-logs).

### From the Web UI

Select any run on the Jobs page to open its **run detail page**, which provides:

- **Status bar** — status badge, trigger type, profile, start and end timestamps, and elapsed time (live-updating while the run is in progress)
- **Pipeline runs table** — every dlt pipeline executed during the job, with row counts and status
- **Log viewer** — real-time streaming logs (refreshed each second while the run is active) or static logs once a run has completed

## Understand run states

| Status         | Meaning                                              |
|----------------|------------------------------------------------------|
| **Pending**    | Run is queued, waiting to start                      |
| **Starting**   | Run is being initialized                             |
| **Running**    | Actively executing                                   |
| **Completed**  | Finished without errors                              |
| **Failed**     | Encountered an error — check logs for details        |
| **Cancelled**  | Manually stopped via CLI or Web UI                   |

## Diagnose a failed run

1. **Inspect the logs** — the log viewer on the run detail page contains the full execution output, including stack traces.
2. **Review the pipeline runs** — the pipeline-runs table on the run detail page lists each dlt pipeline executed during the job and its outcome. Open an individual pipeline run for detailed load information (tables loaded, row counts, bytes, duration).
3. **Consult the dashboard** — the Dashboard and Pipelines pages surface success-rate trends that help identify recurring issues.
4. **Verify the deployment** — the Deployment & Config page indicates the currently deployed code version. Sync the latest changes with [`dlthub deploy`](../command-line-interface.md#dlthub-deploy).

## Cancel an active run

Cancel the latest active run of a job:

```sh
dlthub job runs cancel my_pipeline.py
```

Cancel a specific run number:

```sh
dlthub job runs cancel my_pipeline.py 5
```

Cancel active runs across multiple matching jobs (for example everything tagged `ingest`):

```sh
dlthub job cancel "tag:ingest"

# preview without cancelling
dlthub job cancel "tag:ingest" --dry-run
```

See [`dlthub job runs cancel`](../command-line-interface.md#dlthub-job-runs-cancel) and [`dlthub job cancel`](../command-line-interface.md#dlthub-job-cancel). Cancellation is also available from the run detail page and the Jobs page context menu.

## Monitor pipeline metrics

The Web UI at [app.dlthub.com](https://app.dlthub.com) provides built-in dashboards and visualizations of pipeline telemetry, removing the need for an external observability stack. The Pipelines page surfaces aggregated metrics per pipeline:

- **Success rate** — percentage of successful runs over time
- **Rows loaded** — total data volume trends
- **Duration** — performance trends for identifying regressions
- **Charts** — time-series visualizations with toggleable views (Runs, Rows, Bytes, Duration)

The Dashboard page provides a workspace-wide overview, summarizing job status and recent run activity.

## Known limitations

- Batch jobs have a configurable **maximum runtime**. Jobs exceeding this limit are automatically cancelled. See [Platform limits](overview.md#platform-limits) for details.

## See also

- [dltHub platform overview](overview.md)
- [Profiles](../core-concepts/profiles-dlthub.md)
- [CLI reference](../command-line-interface.md)
