---
title: 'How to load data with dlt and DuckDB'
description: Load the Rick and Morty API into DuckDB and work with the loaded data
keywords: [how to, rick and morty, rest api, duckdb, incremental loading]
---

# How to load data with dlt and DuckDB

The [Rick and Morty API](https://rickandmortyapi.com/) is a public REST API that needs no credentials, paginates its endpoints, and returns records with nested objects and lists. That makes it a good subject for a tour of `dlt`: in this guide you paginate one endpoint, see how `dlt` unpacks the nested JSON into tables in [DuckDB](../dlt-ecosystem/destinations/duckdb.md), query the result from Python, and then extend the pipeline to three endpoints that load only new records on every further run.

## Setup

Create a folder for the project and install `dlt` with DuckDB support:

```sh
mkdir rick_and_morty
cd rick_and_morty
pip install "dlt[duckdb]"
```

There is nothing else to configure. The API is open, and the DuckDB destination creates a database file next to your script.

## 1. Load one endpoint

Create `rick_and_morty_pipeline.py`:

```py
import dlt
from dlt.sources.helpers import requests


@dlt.resource(write_disposition="replace")
def characters():
    url = "https://rickandmortyapi.com/api/character"
    while url:
        response = requests.get(url)
        response.raise_for_status()
        page = response.json()
        # a page carries its records under "results"
        yield page["results"]
        # "info.next" is null on the last page, which ends the loop
        url = page["info"]["next"]


pipeline = dlt.pipeline(
    pipeline_name="rick_and_morty",
    destination="duckdb",
    dataset_name="rick_and_morty_data",
)

load_info = pipeline.run(characters)
print(load_info)
```

The [resource](../general-usage/resource.md) is an ordinary Python generator: it follows the `next` link the API returns and yields each page as a list of records. The `replace` [write disposition](../general-usage/full-loading.md) makes every run start the table from scratch, so you can rerun the script as often as you like while you experiment.

Run it:

```sh
python rick_and_morty_pipeline.py
```

```text
Pipeline rick_and_morty load step finished in 0.30 seconds
1 load package(s) were loaded to destination duckdb and into dataset rick_and_morty_data
The duckdb destination used duckdb:////home/user/rick_and_morty/rick_and_morty.duckdb location to store data
Load package 1786308122.464831 is LOADED and contains no failed jobs
```

## 2. Look at what dlt created

A character record is not flat. It carries two nested objects and a list:

```json
{
  "id": 1,
  "name": "Rick Sanchez",
  "status": "Alive",
  "species": "Human",
  "origin": {"name": "Earth (C-137)", "url": "https://rickandmortyapi.com/api/location/1"},
  "location": {"name": "Citadel of Ricks", "url": "https://rickandmortyapi.com/api/location/3"},
  "episode": ["https://rickandmortyapi.com/api/episode/1"],
  "created": "2017-11-04T18:48:46.250Z"
}
```

You described none of that to `dlt` and created no tables. `dlt` inferred a [schema](../general-usage/schema.md) from the data and loaded it into two tables:

- `characters` holds one row per character. The nested `origin` and `location` objects became the columns `origin__name`, `origin__url`, `location__name`, and `location__url`, and the `created` string was detected as a timestamp.
- `characters__episode` is a [nested table](../general-usage/destination-tables.md#nested-tables) for the `episode` list. Every element becomes a row that keeps the URL in a `value` column and points back to its character through `_dlt_parent_id`.

`dlt` also adds `_dlt_id` to every row and `_dlt_load_id` to every root row, so you can trace a row back to the load that produced it.

To browse the tables, launch the workspace dashboard from the folder you ran the script in (it needs `marimo` installed):

```sh
dlt pipeline rick_and_morty show
```

[Run a pipeline](run-a-pipeline.md) covers the other inspection commands, such as `info` and `trace`.

## 3. Query the loaded data

You do not need a DuckDB client to read the data back. `pipeline.dataset()` returns a [dataset](../general-usage/dataset-access/dataset.md) that you query with Python expressions or SQL and read as records, DataFrames, or Arrow tables:

```py
dataset = pipeline.dataset()

characters_df = (
    dataset["characters"]
    .select("id", "name", "species", "origin__name")
    .order_by("id")
    .limit(5)
    .df()
)
print(characters_df)
```

```text
   id          name species                   origin__name
0   1  Rick Sanchez   Human                  Earth (C-137)
1   2   Morty Smith   Human                        unknown
2   3  Summer Smith   Human  Earth (Replacement Dimension)
3   4    Beth Smith   Human  Earth (Replacement Dimension)
4   5   Jerry Smith   Human  Earth (Replacement Dimension)
```

The nested table is a regular table as well, so you can join it back to its parent on `_dlt_parent_id` and count the episodes each character appears in:

```py
top_characters = pipeline.dataset().query(
    "SELECT c.name, count(e.value) AS episode_count"
    " FROM characters AS c"
    " JOIN characters__episode AS e ON e._dlt_parent_id = c._dlt_id"
    " GROUP BY c.name"
    " ORDER BY episode_count DESC"
    " LIMIT 5"
)
print(top_characters.df())
```

`print(pipeline.dataset().row_counts().df())` prints the row count of every table, which is a quick way to check what a run produced.

## 4. Load several endpoints from one source

The API also exposes `location` and `episode`, and all three endpoints paginate the same way. Group them into a [source](../general-usage/source.md) so that a single `pipeline.run()` loads all of them into one dataset. Create `rick_and_morty_all.py`:

```py
from typing import Any, Iterator

import dlt
from dlt.sources.helpers import requests

BASE_URL = "https://rickandmortyapi.com/api"


def get_pages(endpoint: str) -> Iterator[Any]:
    """Yield all pages of a Rick and Morty API endpoint."""
    url = f"{BASE_URL}/{endpoint}"
    while url:
        response = requests.get(url)
        response.raise_for_status()
        page = response.json()
        yield page["results"]
        url = page["info"]["next"]


@dlt.source(name="rick_and_morty")
def rick_and_morty_source():
    @dlt.resource(primary_key="id", write_disposition="merge")
    def characters(created=dlt.sources.incremental("created")):
        yield from get_pages("character")

    @dlt.resource(primary_key="id", write_disposition="merge")
    def locations(created=dlt.sources.incremental("created")):
        yield from get_pages("location")

    @dlt.resource(primary_key="id", write_disposition="merge")
    def episodes(created=dlt.sources.incremental("created")):
        yield from get_pages("episode")

    return characters, locations, episodes


pipeline = dlt.pipeline(
    pipeline_name="rick_and_morty_all",
    destination="duckdb",
    dataset_name="rick_and_morty_data",
)

load_info = pipeline.run(rick_and_morty_source())
print(load_info)
print(pipeline.last_trace.last_normalize_info)
```

Each resource gets its own table, and each of them gets a nested table for its list field, so the run creates `characters` and `characters__episode`, `locations` and `locations__residents`, `episodes` and `episodes__characters`.

:::note
This script uses a new pipeline name, which keeps it away from the tables of step 1. Switching an existing table from `replace` to `merge` is not free: `dlt` has to add a `_dlt_root_id` column to its nested tables, and DuckDB cannot add that column to a table that already holds data. If you want to reuse the first pipeline, drop its tables first, as described in [switch from append/replace to merge](../general-usage/merge-loading.md#switch-from-appendreplace-to-merge).
:::

## 5. Load only what is new

Two things changed in the resources besides the grouping:

- `write_disposition="merge"` with `primary_key="id"` makes a record that arrives again with the same `id` [replace the stored row](../general-usage/merge-loading.md) instead of adding a duplicate.
- `dlt.sources.incremental("created")` turns the `created` field of a record into a [cursor](../general-usage/incremental/cursor.md). `dlt` keeps the highest `created` value it has seen in the [pipeline state](../general-usage/state.md) and skips the records at or below it on the next run.

Run the script a second time. Unless the API gained a character, location, or episode in the meantime, every record is older than the stored cursor value, nothing reaches the destination, and the load step does not even start:

```text
Pipeline rick_and_morty_all load step did not start
0 load package(s) were loaded to destination duckdb and into dataset None
The duckdb destination used duckdb:////home/user/rick_and_morty/rick_and_morty_all.duckdb location to store data
No data found to normalize
```

The state behind this lives next to your data in the `_dlt_pipeline_state` table, so a run from another machine against the same dataset picks up where this one stopped.

:::note
These endpoints take no filter parameter for `created`, so `dlt` still requests every page and applies the cursor to the records it receives. Here, incremental loading saves work in the normalize and load steps, not HTTP requests.
:::

## 6. The same source, declared instead of coded

The pagination loop and the three near-identical resources are what the [REST API source](../dlt-ecosystem/verified-sources/rest_api/basic.md) takes over. The same pipeline, written as configuration:

```py
import dlt
from dlt.sources.rest_api import rest_api_source

rick_and_morty_api = rest_api_source(
    {
        "client": {
            "base_url": "https://rickandmortyapi.com/api/",
            "paginator": {
                "type": "json_link",
                "next_url_path": "info.next",
            },
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "endpoint": {
                "data_selector": "results",
            },
        },
        "resources": [
            {"name": "characters", "endpoint": {"path": "character"}},
            {"name": "locations", "endpoint": {"path": "location"}},
            {"name": "episodes", "endpoint": {"path": "episode"}},
        ],
    }
)

pipeline = dlt.pipeline(
    pipeline_name="rick_and_morty_rest_api",
    destination="duckdb",
    dataset_name="rick_and_morty_data",
)

load_info = pipeline.run(rick_and_morty_api)
print(load_info)
```

This produces the same six tables: the `json_link` paginator follows `info.next`, `data_selector` picks the records out of `results`, and `resource_defaults` applies the merge settings to all three resources.

## What's next

- Configure the REST API source further, including its own incremental settings, in the [REST API tutorial](../tutorial/rest-api.md).
- Let the schema follow the API: when a record grows a field, `dlt` adds the column for you. See [schema evolution](../general-usage/schema-evolution.md).
- Send records to different tables depending on their content with a [table name function](dispatch-to-multiple-tables.md).
- Swap `duckdb` for another [destination](../dlt-ecosystem/destinations/index.md) when you want to share the data: [moving from local to production](share-a-dataset.md).
