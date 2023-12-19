---
title: Run chess pipeline in production
description: Learn how run chess pipeline in production
keywords: [incremental loading, example]
---

import Header from '../_examples-header.md';

<Header
    intro="In this tutorial, you will learn how to investigate, track, retry and test your loads."
    slug="chess_production"
    run_file="chess"
    destination="duckdb" />

## Run chess pipeline in production

In this example, you'll find a Python script that interacts with the Chess API to extract players and game data.

We'll learn how to:

- Inspecting packages after they have been loaded.
- Loading back load information, schema updates, and traces.
- Triggering notifications in case of schema evolution.
- Using context managers to independently retry pipeline stages.
- Run basic tests utilizing `sql_client` and `normalize_info`.

### Init chess source

<!--@@@DLT_SNIPPET_START ./code/chess-snippets.py::markdown_source-->
```py
import threading
from typing import Any, Iterator

import dlt
from dlt.common import sleep
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
        print(f"getting {username} profile via thread {threading.current_thread().name}")
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


### Using context managers to retry pipeline stages separately

<!--@@@DLT_SNIPPET_START ./code/chess-snippets.py::markdown_retry_cm-->
```py
from tenacity import (
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from dlt.common import logger
from dlt.common.runtime.slack import send_slack_message
from dlt.pipeline.helpers import retry_load

MAX_PLAYERS = 5

def load_data_with_retry(pipeline, data):
    try:
        for attempt in Retrying(
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1.5, min=4, max=10),
            retry=retry_if_exception(retry_load(())),
            reraise=True,
        ):
            with attempt:
                logger.info(
                    f"Running the pipeline, attempt={attempt.retry_state.attempt_number}"
                )
                load_info = pipeline.run(data)
                logger.info(str(load_info))

            # raise on failed jobs
            load_info.raise_on_failed_jobs()
            # send notification
            send_slack_message(
                pipeline.runtime_config.slack_incoming_hook, "Data was successfully loaded!"
            )
    except Exception:
        # we get here after all the failed retries
        # send notification
        send_slack_message(pipeline.runtime_config.slack_incoming_hook, "Something went wrong!")
        raise

    # we get here after a successful attempt
    # see when load was started
    logger.info(f"Pipeline was started: {load_info.started_at}")
    # print the information on the first load package and all jobs inside
    logger.info(f"First load package info: {load_info.load_packages[0]}")
    # print the information on the first completed job in first load package
    logger.info(
        f"First completed job info: {load_info.load_packages[0].jobs['completed_jobs'][0]}"
    )

    # check for schema updates:
    schema_updates = [p.schema_update for p in load_info.load_packages]
    # send notifications if there are schema updates
    if schema_updates:
        # send notification
        send_slack_message(pipeline.runtime_config.slack_incoming_hook, "Schema was updated!")

    # To run simple tests with `sql_client`, such as checking table counts and
    # warning if there is no data, you can use the `execute_query` method
    with pipeline.sql_client() as client:
        with client.execute_query("SELECT COUNT(*) FROM players") as cursor:
            count = cursor.fetchone()[0]
            if count == 0:
                logger.info("Warning: No data in players table")
            else:
                logger.info(f"Players table contains {count} rows")

    # To run simple tests with `normalize_info`, such as checking table counts and
    # warning if there is no data, you can use the `row_counts` attribute.
    normalize_info = pipeline.last_trace.last_normalize_info
    count = normalize_info.row_counts.get("players", 0)
    if count == 0:
        logger.info("Warning: No data in players table")
    else:
        logger.info(f"Players table contains {count} rows")

    # we reuse the pipeline instance below and load to the same dataset as data
    logger.info("Saving the load info in the destination")
    pipeline.run([load_info], table_name="_load_info")
    # save trace to destination, sensitive data will be removed
    logger.info("Saving the trace in the destination")
    pipeline.run([pipeline.last_trace], table_name="_trace")

    # print all the new tables/columns in
    for package in load_info.load_packages:
        for table_name, table in package.schema_update.items():
            logger.info(f"Table {table_name}: {table.get('description')}")
            for column_name, column in table["columns"].items():
                logger.info(f"\tcolumn {column_name}: {column['data_type']}")

    # save the new tables and column schemas to the destination:
    table_updates = [p.asdict()["tables"] for p in load_info.load_packages]
    pipeline.run(table_updates, table_name="_new_tables")

    return load_info
```
<!--@@@DLT_SNIPPET_END ./code/chess-snippets.py::markdown_retry_cm-->

### Run the pipeline

<!--@@@DLT_SNIPPET_START ./code/chess-snippets.py::markdown_pipeline-->
```py
if __name__ == "__main__":
    # create dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="chess_pipeline",
        destination="duckdb",
        dataset_name="chess_data",
    )
    # get data for a few famous players
    data = chess(chess_url="https://api.chess.com/pub/", max_players=MAX_PLAYERS)
    load_data_with_retry(pipeline, data)
```
<!--@@@DLT_SNIPPET_END ./code/chess-snippets.py::markdown_pipeline-->