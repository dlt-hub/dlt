---
title: Run chess pipeline in production
description: Learn how run chess pipeline in production
keywords: [incremental loading, example]
---

import Header from '../_examples-header.md';

<Header
    intro="In this tutorial, you will learn how to investigate and track your loads."
    slug="chess_production"
    run_file="chess" />

## Run chess pipeline in production

In this example, you'll find a Python script that interacts with the Chess API to extract players and game data.

We'll learn how to:

- Examining packages after they have been loaded.
- Reloading load information, schema updates, and traces.
- Triggering notifications in case of schema evolution.
- Using context managers to independently retry pipeline stages.
- Run basic tests utilizing sql_client and normalize_info.


## Init pipeline

<!--@@@DLT_SNIPPET_START ./code/chess-snippets.py::markdown_source-->
```py
import threading
from typing import Any, Iterator

import dlt
from dlt.common import sleep
from dlt.common.runtime.slack import send_slack_message
from dlt.common.typing import StrAny, TDataItems
from dlt.sources.helpers.requests import client

@dlt.source
def chess(
    chess_url: str = dlt.config.value,
    title: str = "GM",
    max_players: int = 2,
    year: int = 2022,
    month: int = 10,
) -> Any:
    def _get_data_with_retry(path: str) -> StrAny:
        r = client.get(f"{chess_url}{path}")
        return r.json()  # type: ignore

    @dlt.resource(write_disposition="replace")
    def players() -> Iterator[TDataItems]:
        # return players one by one, you could also return a list
        # that would be faster but we want to pass players item by item to the transformer
        yield from _get_data_with_retry(f"titled/{title}")["players"][:max_players]

    # this resource takes data from players and returns profiles
    # it uses `defer` decorator to enable parallel run in thread pool.
    # defer requires return at the end so we convert yield into return (we return one item anyway)
    # you can still have yielding transformers, look for the test named `test_evolve_schema`
    @dlt.transformer(data_from=players, write_disposition="replace")
    @dlt.defer
    def players_profiles(username: Any) -> TDataItems:
        print(
            f"getting {username} profile via thread {threading.current_thread().name}"
        )
        sleep(1)  # add some latency to show parallel runs
        return _get_data_with_retry(f"player/{username}")

    # this resource takes data from players and returns games for the last month
    # if not specified otherwise
    @dlt.transformer(data_from=players, write_disposition="append")
    def players_games(username: Any) -> Iterator[TDataItems]:
        # https://api.chess.com/pub/player/{username}/games/{YYYY}/{MM}
        path = f"player/{username}/games/{year:04d}/{month:02d}"
        yield _get_data_with_retry(path)["games"]

    return players(), players_profiles, players_games
```
<!--@@@DLT_SNIPPET_END ./code/chess-snippets.py::markdown_source-->

Run the pipeline:

<!--@@@DLT_SNIPPET_START ./code/chess-snippets.py::markdown_pipeline-->
```py
if __name__ == "__main__":
    # create dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="chess_pipeline",
        destination="duckdb",
        dataset_name="chess_data",
    )
    max_players = 5
    load_info = pipeline.run(
        chess(chess_url="https://api.chess.com/pub/", max_players=max_players)
    )
    print(load_info)
```
<!--@@@DLT_SNIPPET_END ./code/chess-snippets.py::markdown_pipeline-->

## Inspecting packages

To inspect a load process after running a pipeline:

<!--@@@DLT_SNIPPET_START ./code/chess-snippets.py::markdown_inspect-->
```py
# see when load was started
print(f"Pipeline was started: {load_info.started_at}")
# print the information on the first load package and all jobs inside
print(f"First load package info: {load_info.load_packages[0]}")
# print the information on the first completed job in first load package
print(
    f"First completed job info: {load_info.load_packages[0].jobs['completed_jobs'][0]}"
)
```
<!--@@@DLT_SNIPPET_END ./code/chess-snippets.py::markdown_inspect-->


## Loading back load info, schema updates, and traces

To load back the `load_info`, schema updates, and traces for the pipeline:

<!--@@@DLT_SNIPPET_START ./code/chess-snippets.py::markdown_load_back-->
```py
# we reuse the pipeline instance below and load to the same dataset as data
pipeline.run([load_info], table_name="_load_info")
# save trace to destination, sensitive data will be removed
pipeline.run([pipeline.last_trace], table_name="_trace")

# print all the new tables/columns in
for package in load_info.load_packages:
    for table_name, table in package.schema_update.items():
        print(f"Table {table_name}: {table.get('description')}")
        for column_name, column in table["columns"].items():
            print(f"\tcolumn {column_name}: {column['data_type']}")

# save the new tables and column schemas to the destination:
table_updates = [p.asdict()["tables"] for p in load_info.load_packages]
pipeline.run(table_updates, table_name="_new_tables")
```
<!--@@@DLT_SNIPPET_END ./code/chess-snippets.py::markdown_load_back-->

## Sending notifications if schema evolved

To send notifications if the schema has evolved for the pipeline:

<!--@@@DLT_SNIPPET_START ./code/chess-snippets.py::markdown_notify-->
```py
# check for schema updates:
schema_updates = [p.schema_update for p in load_info.load_packages]
# send notifications if there are schema updates
if schema_updates:
    # send notification
    send_slack_message(
        pipeline.runtime_config.slack_incoming_hook, "Schema was updated!"
    )
```
<!--@@@DLT_SNIPPET_END ./code/chess-snippets.py::markdown_notify-->

## Using context managers to retry pipeline stages separately

To use context managers to retry pipeline stages separately for the pipeline:

<!--@@@DLT_SNIPPET_START ./code/chess-snippets.py::markdown_retry_cm-->
```py
from tenacity import (
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from dlt.common.runtime.slack import send_slack_message
from dlt.pipeline.helpers import retry_load

# get data for a few famous players
data = chess(chess_url="https://api.chess.com/pub/", max_players=max_players)
try:
    for attempt in Retrying(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1.5, min=4, max=10),
        retry=retry_if_exception(retry_load(())),
        reraise=True,
    ):
        with attempt:
            pipeline.run(data)
except Exception:
    # we get here after all the retries
    raise
```
<!--@@@DLT_SNIPPET_END ./code/chess-snippets.py::markdown_retry_cm-->

You can also use tenacity to decorate functions:

<!--@@@DLT_SNIPPET_START ./code/chess-snippets.py::markdown_retry-->
```py
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from dlt.pipeline.helpers import retry_load

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1.5, min=4, max=10),
    retry=retry_if_exception(retry_load(("extract", "load"))),
    reraise=True,
)
def load_data_with_retry():
    data = chess(chess_url="https://api.chess.com/pub/", max_players=max_players)
    return pipeline.run(data)
```
<!--@@@DLT_SNIPPET_END ./code/chess-snippets.py::markdown_retry-->

<!--@@@DLT_SNIPPET_START ./code/chess-snippets.py::markdown_retry_run-->
```py
load_info = load_data_with_retry()
```
<!--@@@DLT_SNIPPET_END ./code/chess-snippets.py::markdown_retry_run-->

## Running simple tests with sql_client (table counts, warn if no data)
To run simple tests with `sql_client`, such as checking table counts and
warning if there is no data, you can use the `execute_query` method:

<!--@@@DLT_SNIPPET_START ./code/chess-snippets.py::markdown_sql_client-->
```py
with pipeline.sql_client() as client:
    with client.execute_query("SELECT COUNT(*) FROM players") as cursor:
        count = cursor.fetchone()[0]
        if count == 0:
            print("Warning: No data in players table")
        else:
            print(f"Players table contains {count} rows")
```
<!--@@@DLT_SNIPPET_END ./code/chess-snippets.py::markdown_sql_client-->

To run simple tests with `normalize_info`, such as checking table counts and
warning if there is no data, you can use the `row_counts` attribute:

<!--@@@DLT_SNIPPET_START ./code/chess-snippets.py::markdown_norm_info-->
```py
normalize_info = pipeline.last_trace.last_normalize_info
count = normalize_info.row_counts.get("players", 0)
if count == 0:
    print("Warning: No data in players table")
else:
    print(f"Players table contains {count} rows")
```
<!--@@@DLT_SNIPPET_END ./code/chess-snippets.py::markdown_norm_info-->