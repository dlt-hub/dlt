---
title: Run chess pipeline in production
description: Learn how run chess pipeline in production
keywords: [incremental loading, example]
---

import Header from '../_examples-header.md';

<Header
    intro="In this tutorial, you will learn how ."
    slug="chess_production"
    run_file="chess" />

## Run chess pipeline in production

In this example, you'll find a Python script that interacts with the Chess API to extract players and game data.

We'll learn how to:

- inspect packages post load
- load back load info, schema updates, and traces
- send notifications if schema evolved
- use context managers to retry pipeline stages separately
- run simple tests with sql_client (table counts, warn if no data)
- same as above but with normalize_info


## Init pipeline

<!--@@@DLT_SNIPPET_START ./code/chess-snippets.py::markdown_source-->
```py
import threading
from typing import Any, Iterator

import dlt

from dlt.common import sleep
from dlt.common.typing import StrAny, TDataItems
from dlt.sources.helpers.requests import client

@dlt.source
def chess(chess_url: str = dlt.config.value, title: str = "GM", max_players: int = 2, year: int = 2022, month: int = 10) -> Any:

    def _get_data_with_retry(path: str) -> StrAny:
        r = client.get(f"{chess_url}{path}")
        return r.json()  # type: ignore

    @dlt.resource(write_disposition="replace")
    def players() -> Iterator[TDataItems]:
        # return players one by one, you could also return a list that would be faster but we want to pass players item by item to the transformer
        yield from _get_data_with_retry(f"titled/{title}")["players"][:max_players]

    # this resource takes data from players and returns profiles
    # it uses `defer` decorator to enable parallel run in thread pool. defer requires return at the end so we convert yield into return (we return one item anyway)
    # you can still have yielding transformers, look for the test named `test_evolve_schema`
    @dlt.transformer(data_from=players, write_disposition="replace")
    @dlt.defer
    def players_profiles(username: Any) -> TDataItems:
        print(f"getting {username} profile via thread {threading.current_thread().name}")
        sleep(1)  # add some latency to show parallel runs
        return _get_data_with_retry(f"player/{username}")

    # this resource takes data from players and returns games for the last month if not specified otherwise
    @dlt.transformer(data_from=players, write_disposition="append")
    def players_games(username: Any) -> Iterator[TDataItems]:
        # https://api.chess.com/pub/player/{username}/games/{YYYY}/{MM}
        path = f"player/{username}/games/{year:04d}/{month:02d}"
        yield _get_data_with_retry(path)["games"]

    return players(), players_profiles, players_games
```
<!--@@@DLT_SNIPPET_END ./code/chess-snippets.py::markdown_source-->

[Chess: Setup Guide.](https://dlthub.com/docs/dlt-ecosystem/verified-sources/chess)

## Inspecting packages post load

To inspect a load process after running a pipeline, you can use the dlt command-line interface. Here are some commands you can use:

To get information about the pipeline:

```
dlt pipeline chess_pipeline info
```

To see the most recent load package info:

```
dlt pipeline chess_pipeline load-package
```

To see package info with a given load id:

```
dlt pipeline chess_pipeline load-package <load_id>
```

To see the schema changes introduced in the package:

```
dlt pipeline -v chess_pipeline load-package
```

To see the trace of the most recent data load:

```
dlt pipeline chess_pipeline trace
```

To check for failed jobs in a load package:

```
dlt pipeline chess_pipeline failed-jobs
```

For more details, you can refer to the [documentation.](https://dlthub.com/docs/walkthroughs/run-a-pipeline)

## Loading back load info, schema updates, and traces

To load back the `load_info`, schema updates, and traces for the chess_pipeline, you can use the dlt library in your Python script. Here's how you can do it:

Load `load_info` into the destination:

```python
# we reuse the pipeline instance below and load to the same dataset as data
pipeline.run([load_info], table_name="_load_info")
```

Save the runtime trace to the destination:
```python
# save trace to destination, sensitive data will be removed
pipeline.run([pipeline.last_trace], table_name="_trace")
```

Save the new tables and column schemas to the destination:
```python
# save just the new tables
table_updates = [p.asdict()["tables"] for p in load_info.load_packages]
pipeline.run(table_updates, table_name="_new_tables")
```
For more details, you can refer to the [documentation.](https://dlthub.com/docs/running-in-production/running)

## Sending notifications if schema evolved

To send notifications if the schema has evolved for the chess_pipeline, you can use the dlt library in your Python script. Here's how you can do it:

- Check for schema updates:
  ```python
  schema_updates = [p.asdict()["schema_update"] for p in load_info.load_packages]
  ```
- Send notifications if there are schema updates:
  ```python
  if schema_updates:
      # send notification
      send_notification("Schema has evolved for chess_pipeline")
  ```

In the above code, send_notification is a placeholder for the function you would use to send notifications. This could be an email, a message to a Slack channel, or any other form of notification.

Please note that you would need to implement the send_notification function according to your requirements.

## Using context managers to retry pipeline stages separately

To use context managers to retry pipeline stages separately for the chess_pipeline, you can use the tenacity library. Here's how you can do it:

```python
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
                load_info = pipeline.run(data)
        send_slack_message(pipeline.runtime_config.slack_incoming_hook, "HOORAY 😄")
    except Exception:
        # we get here after all the retries
        send_slack_message(pipeline.runtime_config.slack_incoming_hook, "BOOO 🤯")
        raise
```
In the above code, the Retrying context manager from tenacity is used to retry the run method of the pipeline if it raises an exception. The retry_load helper function is used to specify that only the load stage should be retried. If the run method succeeds, a success message is sent to a Slack channel. If all retries fail, an error message is sent to the Slack channel and the exception is re-raised.

## Running simple tests with sql_client (table counts, warn if no data)
To run simple tests with sql_client, such as checking table counts and warning if there is no data, you can use the sql_client's execute_query method. Here's an example:

```python
pipeline = dlt.pipeline(destination="duckdb", dataset_name="chess_data")
with pipeline.sql_client() as client:
    with client.execute_query("SELECT COUNT(*) FROM player") as cursor:
        count = cursor.fetchone()[0]
        if count == 0:
            print("Warning: No data in player table")
        else:
            print(f"Player table contains {count} rows")
```
In the above code, we first create a pipeline instance.
Then, we use the sql_client context manager to execute a SQL query that counts the number of rows in the player table. If the count is zero, a warning is printed. Otherwise, the number of rows is printed.

## Same as above but with normalize_info
To run simple tests with normalize_info, such as checking table counts and warning if there is no data, you can use the normalize_info's row_counts attribute. Here's an example:
```python
pipeline = dlt.pipeline(destination="duckdb", dataset_name="chess_data")
load_info = pipeline.run(data)
normalize_info = pipeline.last_trace.last_normalize_info

count = normalize_info.row_counts.get("player", 0)
if count == 0:
    print("Warning: No data in player table")
else:
    print(f"Player table contains {count} rows")
```