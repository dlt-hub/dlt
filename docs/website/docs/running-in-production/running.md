---
title: Running
description: Running a dlt pipeline in production
keywords: [running, production, tips]
---

# Running

When running the pipeline in production, you may consider a few additions to your script. We'll use
the script below as a starting point.

```py
import dlt
from chess import chess

if __name__ == "__main__" :
    pipeline = dlt.pipeline(pipeline_name="chess_pipeline", destination='duckdb', dataset_name="games_data")
    # get data for a few famous players
    data = chess(['magnuscarlsen','vincentkeymer', 'dommarajugukesh', 'rpragchess'], start_month="2022/11", end_month="2022/12")
    load_info = pipeline.run(data)
```

## Inspect and save the load info and trace

The `load_info` contains plenty of useful information on the recently loaded data. It contains the
pipeline and dataset name, the destination information (without secrets) and list of loaded
packages. Package information contains its state (`COMPLETED/PROCESSED`) and list of all jobs with
their statuses, file sizes, types and in case of failed jobs-the error messages from the
destination.

```py
    # see when load was started
    print(load_info.started_at)
    # print the information on the first load package and all jobs inside
    print(load_info.load_packages[0])
    # print the information on the first completed job in first load package
    print(load_info.load_packages[0].jobs["completed_jobs"][0])
```

`load_info` may also be loaded into the destinations as below:

```py
    # we reuse the pipeline instance below and load to the same dataset as data
    pipeline.run([load_info], table_name="_load_info")
```

You can also get the runtime trace from the pipeline. It contains timing information on `extract`,
`normalize` and `load` steps and also all the config and secret values with full information from
where they were obtained. You can display and load trace info as shown below. Use your code editor
to explore `trace` object further. The `normalize` step information contains the counts of rows per
table of data that was normalized and then loaded.

```py
    # print human friendly trace information
    print(pipeline.last_trace)
    # save trace to destination, sensitive data will be removed
    pipeline.run([pipeline.last_trace], table_name="_trace")
```

You can also access the last `extract`, `normalize` and `load` infos directly:

```py
    # print human friendly extract information
    print(pipeline.last_trace.last_extract_info)
    # print human friendly normalization information
    print(pipeline.last_trace.last_normalize_info)
    # access row counts dictionary of normalize info
    print(pipeline.last_trace.last_normalize_info.row_counts)
    # print human friendly load information
    print(pipeline.last_trace.last_load_info)
```

Please note that you can inspect the pipeline using
[command line](../reference/command-line-interface.md#dlt-pipeline).

### Inspect, save and alert on schema changes

In the package information you can also see the list of all tables and columns created at the
destination during loading of that package. The code below displays all tables and schemas. Note that
those objects are Typed Dictionaries, use your code editor to explore.

```py
    # print all the new tables/columns in
    for package in load_info.load_packages:
        for table_name, table in package.schema_update.items():
            print(f"Table {table_name}: {table.get('description')}")
            for column_name, column in table["columns"].items():
                print(f"\tcolumn {column_name}: {column['data_type']}")
```

You can save only the new tables and column schemas to the destination. Note that the code above
that saves `load_info` saves this data as well.

```py
    # save just the new tables
    table_updates = [p.asdict()["tables"] for p in load_info.load_packages]
    pipeline.run(table_updates, table_name="_new_tables")
```

## Data left behind

By default `dlt` leaves the loaded packages intact so they may be fully queried and inspected after
load. This behavior may be changed so the successfully completed jobs are deleted from the loaded
package. In that case, for a correctly behaving pipeline, only minimum amount of data will be left
behind. In `config.toml`:

```toml
[load]
delete_completed_jobs=true
```

Also, by default, `dlt` leaves data in [staging dataset](../dlt-ecosystem/staging.md#staging-dataset), used during merge and replace load for deduplication. In order to clear it, put the following line in `config.toml`:

```toml
[load]
truncate_staging_dataset=true
```

## Using slack to send messages

`dlt` provides basic support for sending slack messages. You can configure Slack incoming hook via
[secrets.toml or environment variables](../general-usage/credentials/how_to_set_up_credentials). Please note that **Slack
incoming hook is considered a secret and will be immediately blocked when pushed to github
repository**. In `secrets.toml`:

```toml
[runtime]
slack_incoming_hook="https://hooks.slack.com/services/T04DHMAF13Q/B04E7B1MQ1H/TDHEI123WUEE"
```

or

```sh
RUNTIME__SLACK_INCOMING_HOOK="https://hooks.slack.com/services/T04DHMAF13Q/B04E7B1MQ1H/TDHEI123WUEE"
```

Then the configured hook is available via pipeline object, we also provide convenience method to
send Slack messages:

```py
from dlt.common.runtime.slack import send_slack_message

send_slack_message(pipeline.runtime_config.slack_incoming_hook, message)

```

## Enable Sentry tracing

You can enable exception and runtime [tracing via Sentry](../running-in-production/tracing.md)

## Set the log level and format

You can set log level and switch logging to json format

```toml
[runtime]
log_level="INFO"
log_format="JSON"
```

`log_level` accepts the
[Python standard logging level names](https://docs.python.org/3/library/logging.html#logging-levels).

- The default log level is `WARNING`.
- `INFO` log level is useful when diagnosing problems in production.
- `CRITICAL` will disable logging
- `DEBUG` should not be used in production.

`log_format` accepts:

- `json` to get the log in json format
- [Python standard log format specifier](https://docs.python.org/3/library/logging.html#logrecord-attributes)

As with any other configuration, you can use environment variables instead of the `toml` file.

- `RUNTIME__LOG_LEVEL` to set the log level
- `LOG_FORMAT` to set the log format

`dlt` logs to a logger named **dlt**. `dlt` logger uses a regular python logger so you can configure the handlers
as per your requirement.

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
from loguru import logger


class InterceptHandler(logging.Handler):

    @logger.catch(default=True, onerror=lambda _: sys.exit(1))
    def emit(self, record):
        # Get corresponding Loguru level if it exists.
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = sys._getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())

logger_dlt = logging.getLogger("dlt")
logger_dlt.addHandler(InterceptHandler())

logger.add("dlt_loguru.log")
```

## Handle exceptions, failed jobs and retry the pipeline

When any of the steps of the pipeline fails, an exception of type `PipelineStepFailed` is raised.
Such exception contains the pipeline step name, the pipeline object itself and the step info ie
`LoadInfo`. It provides the general information where the problem happened. In most of the cases,
you can and should obtain the causing exception using the standard Python exception chaining
(`__context__`).

There are two different types of exceptions in `__context__`:

1. **terminal exceptions** are exceptions that **should not be re-tried** because the error
   situation will never recover without an intervention. Examples are: missing config and secret
   values, most of the `40x` http errors, and several database errors (ie. missing relations like
   tables). Each of destinations has its own set of terminal exceptions that `dlt` tries to
   preserve.
1. **transient exceptions** are exceptions that may be retried.

Code below tells one exception type from another. Note that we provide retry strategy helpers that
does that for you.

```py
from dlt.common.exceptions import TerminalException

def check(ex: Exception):
    if isinstance(ex, TerminalException) or (ex.__context__ is not None and isinstance(ex.__context__, TerminalException)):
        return False
    return True
```

### Failed jobs

If any job in the package **fail terminally** it will be moved to `failed_jobs` folder and assigned
such status. By default **no exception is raised** and other jobs will be processed and completed.
You may inspect if the failed jobs are present by checking the load info as follows:

```py
# returns True if there are failed jobs in any of the load packages
print(load_info.has_failed_jobs)
# raises terminal exception if there are any failed jobs
load_info.raise_on_failed_jobs()
```

You may also abort the load package with `LoadClientJobFailed` (terminal exception) on a first
failed job. Such package is immediately moved to completed but its load id is not added to the
`_dlt_loads` table. All the jobs that were running in parallel are completed before raising. The dlt
state, if present, will not be visible to `dlt`. Here's example `config.toml` to enable this option:

```toml
# you should really load just one job at a time to get the deterministic behavior
load.workers=1
# I hope you know what you are doing by setting this to true
load.raise_on_failed_jobs=true
```

### What `run` does inside

Before adding retry to pipeline steps, note how `run` method actually works:

1. The `run` method will first use `sync_destination` method to synchronize pipeline state and
   schemas with the destination. Obviously at this point connection to the destination is
   established (which may fail and be retried)
1. Next it will make sure that data from the previous runs is fully processed. If not, `run` method
   normalizes, loads pending data items and **exits**
1. If there was no pending data, new data from `data` argument is extracted, normalized and loaded.

### Retry helpers and `tenacity`

By default `dlt` does not retry any of the pipeline steps. This is left to the included helpers and
the [tenacity](https://tenacity.readthedocs.io/en/latest/) library. Snippet below will retry the
`load` stage with the `retry_load` strategy and defined back-off or re-raise exception for any other
steps (`extract`, `normalize`) and for terminal exceptions.

```py
from tenacity import stop_after_attempt, retry_if_exception, Retrying, retry
from dlt.common.runtime.slack import send_slack_message
from dlt.pipeline.helpers import retry_load

if __name__ == "__main__" :
    pipeline = dlt.pipeline(pipeline_name="chess_pipeline", destination='duckdb', dataset_name="games_data")
    # get data for a few famous players
    data = chess(['magnuscarlsen', 'rpragchess'], start_month="2022/11", end_month="2022/12")
    try:

        for attempt in Retrying(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1.5, min=4, max=10), retry=retry_if_exception(retry_load(())), reraise=True):
            with attempt:
                load_info = p.run(data)
                send_slack_message(pipeline.runtime_config.slack_incoming_hook, "HOORAY 😄")
    except Exception:
        # we get here after all the retries
        send_slack_message(pipeline.runtime_config.slack_incoming_hook, "BOOO 🤯")
        raise
```

You can also use `tenacity` to decorate functions. This example additionally retries on `extract`:

```py
if __name__ == "__main__" :
    pipeline = dlt.pipeline(pipeline_name="chess_pipeline", destination='duckdb', dataset_name="games_data")

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1.5, min=4, max=10), retry=retry_if_exception(retry_load(("extract", "load"))), reraise=True)
    def load():
        data = chess(['magnuscarlsen','vincentkeymer', 'dommarajugukesh', 'rpragchess'], start_month="2022/11", end_month="2022/12")
        return pipeline.run(data)

    load_info = load()
```
