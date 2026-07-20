---
title: Running
description: Running a dlt pipeline in production
keywords: [running, production, tips]
---

# Adjust a pipeline to run in production

When running the pipeline in production, you may consider a few additions to your script. We'll use the script below as a starting point.

```py
import dlt

if __name__ == "__main__":
    pipeline = dlt.pipeline(pipeline_name="chess_pipeline", destination='duckdb', dataset_name="games_data")
    # get data for a few famous players
    data = chess_source(['magnuscarlsen', 'vincentkeymer', 'dommarajugukesh', 'rpragchess'], start_month="2022/11", end_month="2022/12")
    load_info = pipeline.run(data)
```

## Inspect and save the load info and trace

The `load_info` contains plenty of useful information on the recently loaded data. It contains the pipeline and dataset name, the destination information (without secrets), and a list of loaded packages. Package information contains its state (`COMPLETED/PROCESSED`) and a list of all jobs with their statuses, file sizes, types, and in case of failed jobs, the error messages from the destination.

```py
    # see when load was started
    print(load_info.started_at)
    # print the information on the first load package and all jobs inside
    print(load_info.load_packages[0])
    # print the information on the first completed job in the first load package
    print(load_info.load_packages[0].jobs["completed_jobs"][0])
    # see the refresh mode the package was extracted with (None if refresh was not requested)
    print(load_info.load_packages[0].refresh)
    # see the tables that were dropped and truncated in the destination, e.g. by refresh,
    # or the drop command
    print(load_info.load_packages[0].dropped_tables)
    print(load_info.load_packages[0].truncated_tables)
```

`load_info` may also be loaded into the destinations as below:

```py
    # we reuse the pipeline instance below and load to the same dataset as data
    pipeline.run([load_info], table_name="_load_info")
```

You can also get the runtime trace from the pipeline. It contains timing information on `extract`, `normalize`, and `load` steps and also all the config and secret values with full information from where they were obtained. You can display and load trace info as shown below. Use your code editor to explore the `trace` object further. The `normalize` step information contains the counts of rows per table of data that was normalized and then loaded.

```py
    # print human-friendly trace information
    print(pipeline.last_trace)
    # save trace to destination, sensitive data will be removed
    pipeline.run([pipeline.last_trace], table_name="_trace")
```

You can also access the last `extract`, `normalize`, and `load` infos directly:

```py
    # print human-friendly extract information
    print(pipeline.last_trace.last_extract_info)
    # print human-friendly normalization information
    print(pipeline.last_trace.last_normalize_info)
    # access row counts dictionary of normalize info
    print(pipeline.last_trace.last_normalize_info.row_counts)
    # print human-friendly load information
    print(pipeline.last_trace.last_load_info)
```

Please note that you can inspect the pipeline using [command line](../reference/command-line-interface.md#dlt-pipeline).

### Inspect, save, and alert on schema changes

In the package information, you can also see the list of all tables and columns created at the destination during the loading of that package. The code below displays all tables and schemas. Note that those objects are Typed Dictionaries; use your code editor to explore.

```py
    # print all the new tables/columns in
    for package in load_info.load_packages:
        for table_name, table in package.schema_update.items():
            print(f"Table {table_name}: {table.get('description')}")
            for column_name, column in table["columns"].items():
                print(f"\tcolumn {column_name}: {column['data_type']}")
```

You can save only the new tables and column schemas to the destination. Note that the code above that saves `load_info` saves this data as well.

```py
    # save just the new tables
    table_updates = [p.asdict()["tables"] for p in load_info.load_packages]
    pipeline.run(table_updates, table_name="_new_tables")
```

## Data left behind

By default, `dlt` leaves the loaded packages intact so they may be fully queried and inspected after loading. This behavior may be changed so that the successfully completed jobs are deleted from the loaded package. In that case, for a correctly behaving pipeline, only a minimum amount of data will be left behind. In `config.toml`:

```toml
[load]
delete_completed_jobs=true
```

Also, by default, `dlt` leaves data in the [staging dataset](../dlt-ecosystem/staging.md#staging-dataset), used during merge and replace load for deduplication. In order to clear it, put the following line in `config.toml`:

```toml
[load]
truncate_staging_dataset=true
```

## Using Slack to send messages

`dlt` provides basic support for sending Slack messages. You can configure the Slack incoming hook via [secrets.toml or environment variables](../general-usage/credentials/setup). Please note that **the Slack incoming hook is considered a secret and will be immediately blocked when pushed to a GitHub repository**. In `secrets.toml`:

```toml
[runtime]
slack_incoming_hook="https://hooks.slack.com/services/T04DHMAF13Q/B04E7B1MQ1H/TDHEI123WUEE"
```

or

```sh
RUNTIME__SLACK_INCOMING_HOOK="https://hooks.slack.com/services/T04DHMAF13Q/B04E7B1MQ1H/TDHEI123WUEE"
```

Then, the configured hook is available via the pipeline object. We also provide a convenience method to send Slack messages:

```py
from dlt.common.runtime.slack import send_slack_message

send_slack_message(pipeline.runtime_config.slack_incoming_hook, message)

```

### Send schema migration info to Slack
The code snippet below demonstrates automated Slack notifications for database table updates using the `send_slack_message` function.

```py
# Import the send_slack_message function from the dlt library
from dlt.common.runtime.slack import send_slack_message

# Define the URL for your Slack webhook
hook = "https://hooks.slack.com/services/xxx/xxx/xxx"

# Iterate over each package in the load_info object
for package in load_info.load_packages:
    # Iterate over each table in the schema_update of the current package
    for table_name, table in package.schema_update.items():
        # Iterate over each column in the current table
        for column_name, column in table["columns"].items():
            # Send a message to the Slack channel with the table
            # and column update information
            send_slack_message(
                hook,
                message=(
                    f"\tTable updated: {table_name}: "
                    f"Column changed: {column_name}: "
                    f"{column['data_type']}"
                )
            )
```
Refer to this [example](../examples/chess_production/) for a practical application of the method in a production environment.

## Enable Sentry tracing

`dlt` users can configure [Sentry](https://sentry.io) DSN to start receiving rich information on
executed pipelines, including encountered errors and exceptions. **Sentry tracing is disabled by
default.**

### When and what we send

An exception trace is sent when:

- Any Python logger (including `dlt`) logs an error.
- Any Python logger (including `dlt`) logs a warning (enabled only if the `dlt` logging level is
  `WARNING` or below).
- On unhandled exceptions.

A transaction trace is sent when the `pipeline.run` is called. We send information when
[extract, normalize, and load](../reference/explainers/how-dlt-works.md) steps are completed.

The data available in Sentry makes finding and documenting bugs easy, allowing you to easily find
bottlenecks and profile data extraction, normalization, and loading.

`dlt` adds a set of additional tags (e.g., pipeline name, destination name) to the Sentry data.

Please refer to the Sentry [documentation](https://docs.sentry.io/platforms/python/data-collected/).

### Enable pipeline tracing

To enable Sentry, you should configure the
[DSN](https://docs.sentry.io/product/sentry-basics/dsn-explainer/) in the `config.toml`:

```toml
[runtime]

sentry_dsn="https:///<...>"
```

Alternatively, you can use environment variables:

```sh
RUNTIME__SENTRY_DSN="https:///<...>"
```

The Sentry client is configured after the first pipeline is created with `dlt.pipeline()`. Feel free
to use `sentry_sdk` init again to cover your specific needs.

> 💡 `dlt` does not have Sentry client as a dependency. Remember to install it with `pip install sentry-sdk`.

### Disable all tracing

`dlt` allows you to completely disable pipeline tracing, including the anonymous telemetry and
Sentry. Using `config.toml`:

```toml
enable_runtime_trace=false
```

## Set the log level and format

You can set the log level and switch logging to JSON format.

```toml
[runtime]
log_level="INFO"
log_format="JSON"
```

`log_level` accepts the [Python standard logging level names](https://docs.python.org/3/library/logging.html#logging-levels).

- The default log level is `WARNING`.
- The `INFO` log level is useful when diagnosing problems in production.
- `CRITICAL` will disable logging.
- `DEBUG` should not be used in production.

`log_format` accepts:

- `json` to get the log in JSON format.
- [Python standard log format specifier](https://docs.python.org/3/library/logging.html#logrecord-attributes).

As with any other configuration, you can use environment variables instead of the TOML file.

- `RUNTIME__LOG_LEVEL` to set the log level.
- `LOG_FORMAT` to set the log format.

`dlt` logs to a logger named **dlt**. `dlt` logger uses a regular Python logger, so you can configure the handlers as per your requirement.

For example, to put logs to the file:
```py
import logging

# Create a logger
logger = logging.getLogger('dlt')

# Set the log level
logger.setLevel(logging.INFO)

# Create a file handler
handler = logging.FileHandler('dlt.log')

# Add the handler to the logger
logger.addHandler(handler)
```
You can intercept logs by using [loguru](https://loguru.readthedocs.io/en/stable/api/logger.html). To do so, follow the instructions below:

```py
import logging
import sys

import dlt
from loguru import logger as loguru_logger


class InterceptHandler(logging.Handler):

    @loguru_logger.catch(default=True, onerror=lambda _: sys.exit(1))
    def emit(self, record):
        # Get the corresponding Loguru level if it exists.
        try:
            level = loguru_logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find the caller from where the logged message originated.
        frame, depth = sys._getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        loguru_logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())

logger_dlt = logging.getLogger("dlt")
logger_dlt.addHandler(InterceptHandler())

loguru_logger.add("dlt_loguru.log")
```

## Handle exceptions, failed jobs, and retry the pipeline

When any of the steps of the pipeline fails, an exception of type `PipelineStepFailed` is raised.
Such an exception contains the pipeline step name, the pipeline object itself, and the step info, i.e.,
`LoadInfo`. It provides general information about where the problem occurred. In most cases,
you can and should obtain the causing exception using the standard Python exception chaining
(`__context__`).

There are two different types of exceptions in `__context__`:

1. **Terminal exceptions** are exceptions that **should not be retried** because the error
   situation will never recover without intervention. Examples include missing config and secret
   values, most of the `40x` HTTP errors, and several database errors (i.e., missing relations like
   tables). Each destination has its own set of terminal exceptions that `dlt` tries to
   preserve.
2. **Transient exceptions** are exceptions that may be retried.

The code below tells one exception type from another. Note that we provide retry strategy helpers that
do that for you.

```py
from dlt.common.exceptions import TerminalException

def check(ex: Exception):
    if isinstance(ex, TerminalException) or (ex.__context__ is not None and isinstance(ex.__context__, TerminalException)):
        return False
    return True
```

If pipeline fails, your best course of action is to retry as described [below](#retry-helpers-and-tenacity). You also have tools to
investigate the incident, fix it or start from scratch.
* in `extract` and `normalize` steps you can just abort the package and start from scratch - `dlt` will rollback state and schema changes. Read the section below for details (`load` step abort procedure applies).
* in `load` step you have more options - read the section below to understand how `dlt` deals with partial loads and inconsistent data.

### Handle problems in `load` step

Dealing with problems during `load` step is more complicated because `dlt` could already modified data in the destination.

1. [Retry the load](#retry-the-load) — still your best option.
2. [Fail particular jobs](#fail-or-retry-individual-jobs) (both terminal and not terminal) and then retry the rest of the package.
3. [Abort the package](#abort-the-package).

Whichever you pick, until a package is fully loaded its load id is not added to the `_dlt_loads`
table and the pipeline state at the destination stays at the point the package was created, so
incremental cursors are not advanced past data that did not load.

:::warning
Some jobs of a pending package may have already written to the destination and neither failing
jobs nor aborting the package reverts that. Before you act, check what was already written — see
[partially loaded packages](#partially-loaded-packages).
:::

#### How `dlt` reacts to a failed job

A job that fails with a **transient** error (network problems, overloaded destination) is retried
in place while the load runs: it is moved back to `new_jobs` with an increased retry count and
picked up again — see [how to configure the internal retry](#configure-the-internal-job-retry).

A job that fails with a **terminal** error (permission denied, malformed data) will not recover on
retry. Two options control what happens then: `raise_on_failed_jobs` (default `true`) and
`auto_abort_on_terminal_error` (default `false`).

By default, on the first failed job the job is queued for retry (moved back to `new_jobs` with an
increased retry count), the load package stays **pending** and `LoadClientJobTerminalRetry`
(terminal exception) is raised. All the jobs that were running in parallel are completed before
raising. You can then retry the package, give up on the job, or abort the whole package as
described in the sections below.

The exception message of every retry (terminal and transient) is saved in the `.exceptions` folder
of the load package, with the first line indicating `retry: terminal` or `retry: transient`.

The full behavior matrix:

| `auto_abort_on_terminal_error` | `raise_on_failed_jobs` | job | package | exception |
|---|---|---|---|---|
| `false` | `true` | queued for retry | stays pending | `LoadClientJobTerminalRetry` raised (default) |
| `false` | `false` | moved to `failed_jobs` | completed as loaded | none |
| `true` | `true` | moved to `failed_jobs` | [aborted](#abort-the-package), pending packages deleted, state restored | `LoadClientJobFailed` raised |
| `true` | `false` | moved to `failed_jobs` | [aborted](#abort-the-package), pending packages deleted, state restored | none |

If you prefer that packages with terminally failed jobs complete as loaded (the failed jobs move
to `failed_jobs` and no exception is raised):

```toml
# I hope you know what you are doing by setting this to false
load.raise_on_failed_jobs=false
```

In that mode, check for failed jobs yourself:

```py
# returns True if there are failed jobs in any of the load packages
print(load_info.has_failed_jobs)
# raises terminal exception if there are any failed jobs
load_info.raise_on_failed_jobs()
```

:::caution Breaking change (from `dlt` 1.30)
Previously a terminally failed job aborted the load package and raised `LoadClientJobFailed`; such a
package could not be retried. Now dlt keeps the package pending, queues the failed job for retry and
raises `LoadClientJobTerminalRetry`.
:::

Before deciding how to resolve an incident, inspect it:

```sh
# pending packages and their load ids
dlt pipeline <pipeline_name> info
# jobs matching a name fragment, with the full exception history across retries
dlt pipeline <pipeline_name> load-package <load_id> job <pattern>
# rows the package already wrote to the destination
dlt pipeline <pipeline_name> load-package <load_id> row-counts
```

#### Configure the internal job retry

Jobs that fail transiently are retried inside the running load step, without involving your code:

```toml
[load]
# stop the load when a job keeps failing and its retry count reaches a multiple of this value,
# 0 disables the limit and retries indefinitely
raise_on_max_retries=5
```

A retried job is restarted on the next pass of the load loop, right after its failure is detected —
there is no backoff between attempts of the same job. When a job fails `raise_on_max_retries` times,
the load stops with `LoadClientJobRetry` (a transient exception) and the package stays pending. A
subsequent `load()` resumes the package and grants the job another round of retries. If your
destination needs time to recover, let the load stop and pace the whole pipeline from the outside:
the [retry helpers](#retry-helpers-and-tenacity) treat `LoadClientJobRetry` as retryable and back
off exponentially.

#### Retry the load

Retrying is the safest option: run the pipeline (or just the load step) again and `dlt` resumes
the pending package. Completed jobs are never executed again, retried jobs keep their retry counts
and recorded exceptions, and jobs interrupted by a crash are resolved from their recorded outcomes
without re-execution — so a retry is safe also for
[partially loaded](#partially-loaded-packages) packages.

```py
import dlt

pipeline = dlt.attach("my_pipeline")
# resumes pending packages, completed jobs are not executed again
pipeline.load()
```

Note that `pipeline.run()` also loads pending packages first, but it warns and ignores new data
passed to it until the pending packages are processed. Retry resolves the incident when the cause
was fixed outside of the pipeline: a transient outage passed, permissions were granted, or the
destination schema was corrected. Wrap your production runs in the
[retry helpers](#retry-helpers-and-tenacity) to retry transient errors automatically.

#### Fail or retry individual jobs

When a particular job can never succeed — for example, it carries malformed data — move it to
`failed_jobs` and retry the rest of the package. Any job pending a retry can be failed, whether
its error was terminal or transient. Failing a job copies its exception message along, so it stays
visible after the package completes. Once the problematic jobs are failed, retry: the package
completes as loaded with the failed jobs recorded in it.

List the jobs of the pending package first to pick the ones to fail:

```sh
# all jobs of a package grouped by state, including those pending a retry
dlt pipeline <pipeline_name> load-package <load_id>
# jobs matching a name fragment, with the full exception history across retries
dlt pipeline <pipeline_name> load-package <load_id> job <pattern>
# failed jobs of all packages with their error messages
dlt pipeline <pipeline_name> failed-jobs
```

Then fail a job by its job id or full file name — the exception and the retry count are shown
before you confirm:

```sh
dlt pipeline <pipeline_name> load-package <load_id> fail-job <job_id>
```

The same from Python:

```py
import os
import dlt

pipeline = dlt.attach("my_pipeline")
load_id = pipeline.list_normalized_load_packages()[0]
# list (job file, folder) tuples of retried jobs. folder is "new_jobs", or "started_jobs"
# for jobs interrupted by a crashed load
jobs = pipeline.list_pending_retry_jobs_in_package(load_id)
job_file, folder = jobs[0]
# give up on a job: move it to failed_jobs together with its exception message
pipeline.fail_pending_job(load_id, os.path.basename(job_file))
# or move a failed job back for another retry
# pipeline.retry_failed_job(load_id, os.path.basename(job_file))
# retry the package: run load again
pipeline.load()
```

Jobs listed in `started_jobs` were interrupted by a crashed load and cannot be failed directly:
run the pipeline (their recorded outcomes are resolved without re-execution) or abort the package.
A job failed by mistake can be brought back for another retry with
`pipeline.retry_failed_job(load_id, job_file_name)`.

#### Abort the package

:::tip
If your pipeline is running on ephemeral storage ie. Airflow or dltHub where pipeline working folder
is wiped out after run - and your pipeline failed - you can consider load packages in that pipeline to be
effectively aborted and move to [investigating partially loaded packages](#partially-loaded-packages).
:::

To discard pending packages, abort them with `pipeline.abort_packages()` or the
[CLI command](../reference/command-line-interface.md#dlt-pipeline-abort-packages):

```sh
dlt pipeline <pipeline_name> abort-packages
```

The CLI shows the abort plan — which jobs will be failed and which packages deleted — and asks for
confirmation; `abort_packages(dry_run=True)` returns the same plan programmatically.

The oldest pending package (the one being loaded) is aborted with a record: its retried jobs are
moved to `failed_jobs` (so they stay visible in `failed-jobs` output) and the package is completed
as aborted (its load id is not added to `_dlt_loads`). All other pending packages, extracted ones
included, are deleted. Finally, the local pipeline state and schemas are restored from the
snapshot each package carries in its `.restore` folder — rewinding them (including incremental
cursors) to the point at which the aborted package started, so you can safely re-extract and
re-run. The whole operation works without destination access and the aborted package stays in the
local `loaded` storage together with its failed jobs and exception messages. Pass
`abort_packages(load_id=...)` to abort at a specific package: packages older than it are left
intact and stay loadable. The abort intent is persisted in the package state, so an abort
interrupted e.g. by a crash is finished by the next `load()` or `run()`.

:::tip Abort on terminal failures automatically
Set `auto_abort_on_terminal_error=true` to make `dlt` run exactly this abort on the first terminal
failure (and raise `LoadClientJobFailed`), as it did before `dlt` 1.30. Note that the abort is more
thorough than in previous versions: the old behavior left newer pending packages and the local
state inconsistent with the destination, while now they are cleaned up and restored.

```toml
[load]
auto_abort_on_terminal_error=true
```
:::

The `drop-pending-packages` CLI command and `Pipeline.drop_pending_packages` are deprecated aliases
that now abort: they emit a deprecation warning and otherwise behave like `abort_packages`.

### Partially loaded packages

A load package is *partially loaded* when some of its jobs committed data to the destination while others did not — for example, one table of a multi-table load succeeded, the destination schema was migrated, or a job was retried after an earlier attempt may already have written. When this happens, `dlt` flags the raised `PipelineStepFailed` with a `WARNING` that the package is partially loaded and the destination may be in an inconsistent state.

To inspect a suspected package, count its rows in the destination and check whether it completed:

```sh
dlt pipeline <pipeline_name> load-package <load_id> row-counts
```

:::tip
Load package does not need to be present locally - if you are investigating remore pipeline ie. running on Airflow, sync the newest
destination state with
```sh
dlt pipeline <name> sync
```
first.
:::

This reports the row count per table for that `load_id` (including `_dlt` tables) and whether the package is recorded as completed in `_dlt_loads`. It works even after the package is gone from the working directory. If there are rows for the `load_id` but the package is **not** in `_dlt_loads`, the load did not finish and your data is inconsistent.

Retrying the load is always the safest remedy: run the pipeline again (ideally wrapped in `tenacity`, see below) and `dlt` resumes the pending package and completes it. If retrying is not possible, the remedy depends on the write disposition. For `merge` and `replace` tables, do not delete rows by hand — abort the package with `pipeline.abort_packages()`, which restores the local checkpoint the package was created from, then run the pipeline again. For `append` tables you can instead remove the partially-loaded rows yourself. Attach the pipeline, take the `load_id` of the partially-loaded package, and set `root_table` to the affected append table. The root table carries `_dlt_load_id`, while nested tables link to it through `_dlt_parent_id`, so rows are deleted deepest-first with a subquery that walks up to the root:

<!--@@@DLT_SNIPPET ./running_snippets/running-snippets.py::delete_append_load_id-->

:::tip
Here's how to lower the chances of having your destination dataset in
inconsistent state.
1. `replace` write disposition with the default `truncate-and-insert` [strategy](../general-usage/full-loading.md) will truncate tables before loading.
2. `merge` write disposition will merge staging dataset tables into the destination dataset. This will happen only when all data for this table (and nested tables) got loaded.

Here's what you can do to deal with partially loaded packages:
1. Retry the load step in case of transient errors.
2. Use replace strategy with staging dataset so replace happens only when data for the table (and all nested tables) was fully loaded and is an atomic operation (if possible).
3. Use only "append" write disposition. When your load package fails, you are able to use `_dlt_load_id` to remove all unprocessed data.
4. Use "staging append" (`merge` disposition without primary key and merge key defined).

:::


### What `run` does inside

Before adding retry to pipeline steps, note how the `run` method actually works:

1. The `run` method will first use the `sync_destination` method to synchronize pipeline state and
   schemas with the destination. Obviously, at this point, a connection to the destination is
   established (which may fail and be retried).
2. Next, it will make sure that data from the previous runs is fully processed. If not, the `run` method
   normalizes, loads pending data items, and **exits**.
3. If there was no pending data, new data from the `data` argument is extracted, normalized, and loaded.

### Retry helpers and `tenacity`

By default, `dlt` does not retry any of the pipeline steps. This is left to the included helpers and
the [tenacity](https://tenacity.readthedocs.io/en/latest/) library. The snippet below will retry the
`load` stage with the `retry_load` strategy and define back-off or re-raise exceptions for any other
steps (`extract`, `normalize`) and for terminal exceptions.

```py
from tenacity import stop_after_attempt, retry_if_exception, Retrying, retry, wait_exponential
from dlt.common.runtime.slack import send_slack_message
from dlt.pipeline.helpers import retry_load

if __name__ == "__main__":
    pipeline = dlt.pipeline(pipeline_name="chess_pipeline", destination='duckdb', dataset_name="games_data")
    # get data for a few famous players
    data = chess_source(['magnuscarlsen', 'rpragchess'], start_month="2022/11", end_month="2022/12")
    try:

        for attempt in Retrying(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1.5, min=4, max=10), retry=retry_if_exception(retry_load()), reraise=True):
            with attempt:
                load_info = pipeline.run(data)
                send_slack_message(pipeline.runtime_config.slack_incoming_hook, "HOORAY 😄")
    except Exception:
        # we get here after all the retries
        send_slack_message(pipeline.runtime_config.slack_incoming_hook, "BOOO 🤯")
        raise
```

You can also use `tenacity` to decorate functions. This example additionally retries on `extract`:

```py
if __name__ == "__main__":
    pipeline = dlt.pipeline(pipeline_name="chess_pipeline", destination='duckdb', dataset_name="games_data")

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1.5, min=4, max=10), retry=retry_if_exception(retry_load(("extract", "load"))), reraise=True)
    def load():
        data = chess_source(['magnuscarlsen', 'vincentkeymer', 'dommarajugukesh', 'rpragchess'], start_month="2022/11", end_month="2022/12")
        return pipeline.run(data)

    load_info = load()
```

### Allow a graceful shutdown
`dlt` attempts a graceful shutdown of a running pipeline by installing custom signal handlers. In those handlers SIGINT (Ctrl-C) and SIGTERM
are intercepted. Handlers are activated when pipeline runs and have the following effect:
- `normalize` step: raises `SignalReceivedException` at certain checkpoints, typically immediately.
- `load` step: on the first received signal, it attempts to drain the job pool by not accepting new load jobs and waiting for executing jobs to complete.
   On a second signal, the default handler is called, resulting in a `KeyboardInterrupt` or immediate process termination (SIGTERM).
- `extract` step does not intercept signals and uses default handlers.

`normalize` and `extract` steps are atomic and can be terminated at any point without data loss. The `load` step [requires more attention](#partially-loaded-packages). Most production environments will try to terminate processes/jobs/pods gracefully by sending SIGTERM, waiting,
and then killing the process if it does not stop. Below are examples for common environments:

- Kubernetes:
  - Set a long `terminationGracePeriodSeconds` (e.g., 300s) so `dlt` can drain load jobs.
  - Optionally add a preStop hook to give the app a short head start before termination.

- Docker / Docker Compose:
  - Use a long `--stop-timeout` or `stop_grace_period`.

- GitHub Actions:
  - Choose a `timeout-minutes` large enough for graceful draining.
  

We recommend increasing those timeouts to a few minutes so that load jobs can be drained properly. Note that in this case **you can still end up with
a partially loaded package that should be retried without wiping out the pipeline working directory**. In that case, make sure the pipeline working directory (.dlt) is on persistent storage.

You can also opt to run the load step until completion after a signal is received. This gives `dlt` a chance to complete the current load package and then
terminate:
```toml
[load]
start_new_jobs_on_signal=true
```

Obviously, this requires a very long grace period to be defined in your production environment.

#### Signals in thread pools and orchestrators

:::warning
Note that signal interception is possible only in the main Python thread. If you offload pipeline runs to a thread pool ([or async pool with thread executors](../reference/performance.md#parallelism-within-a-single-process)), intercept signal handling before any pipeline runs in the pool:
```py
import asyncio

from dlt.common.runtime import signals


with signals.intercepted_signals():
    # load data
    asyncio.run(_run_async())
```

Signal interception works in orchestrators that run your code in a separate process and propagate SIGTERM/SIGINT:
- Dagster: default multiprocess and Kubernetes executors start a process per op/run; Kubernetes will send SIGTERM, respect the Pod grace period. Avoid thread-based executors for the pipeline step or wrap with `intercepted_signals` as shown above.
- Airflow: task runners execute each task in its own process (Local/Celery/Kubernetes executors). On cancel/timeout, the task process receives SIGTERM then SIGKILL; if using KubernetesExecutor, rely on the Pod grace period.
- Prefect: Subprocess flow/task runners and Kubernetes jobs deliver SIGTERM to your process; if you use thread-based concurrency inside a task, wrap the outermost entrypoint with intercepted_signals.

:::

#### Write custom signal handler
You can disable dlt signal handlers and prevent interception of SIGINT and SIGTERM: for all or for a selected pipeline:
```toml
[runtime]
intercept_signals=false
```
or
```toml
[pipelines.my_pipeline.runtime]
intercept_signals=false
```
and then install your own handlers.

Note that `signals.py` is a pretty simple module and you can call its methods from your own handler to plug into `dlt` signal handling machinery. We are working on making the `signals.py` pluggable to make it straightforward.
