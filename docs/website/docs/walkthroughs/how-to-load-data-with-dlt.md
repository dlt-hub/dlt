---
title: How to load data with dlt
description: Load characters, locations, and episodes from the Rick and Morty API into DuckDB with dlt
keywords: [rick and morty, rest api, duckdb, merge, incremental loading]
---

# How to load data with dlt

This guide walks through a complete pipeline: it reads characters, locations, and episodes from the public [Rick and Morty API](https://rickandmortyapi.com/) and loads them into a local [DuckDB](../dlt-ecosystem/destinations/duckdb.md) database. The API needs no credentials and no registration, so you can run every snippet on this page as it is.

Along the way you will see what `dlt` does for you: pagination, nested lists turned into child tables, `merge` writes that survive a re-run, incremental loading, and Python access to the loaded data.

## Prerequisites

- Python 3.10 or higher, in a virtual environment. See the [installation guide](../reference/installation.md).
- `dlt` with the DuckDB extra:

  ```sh
  pip install "dlt[duckdb]"
  ```

## Look at the API first

Every endpoint of the Rick and Morty API answers with the same envelope: a `info` object with the link to the next page, and a `results` list with the records. A request to `https://rickandmortyapi.com/api/character` returns:

```json
{
  "info": {
    "count": 826,
    "pages": 42,
    "next": "https://rickandmortyapi.com/api/character?page=2",
    "prev": null
  },
  "results": [
    {
      "id": 1,
      "name": "Rick Sanchez",
      "status": "Alive",
      "species": "Human",
      "type": "",
      "gender": "Male",
      "origin": {
        "name": "Earth (C-137)",
        "url": "https://rickandmortyapi.com/api/location/1"
      },
      "location": {
        "name": "Citadel of Ricks",
        "url": "https://rickandmortyapi.com/api/location/3"
      },
      "image": "https://rickandmortyapi.com/api/character/avatar/1.jpeg",
      "episode": [
        "https://rickandmortyapi.com/api/episode/1",
        "https://rickandmortyapi.com/api/episode/2"
      ],
      "url": "https://rickandmortyapi.com/api/character/1",
      "created": "2017-11-04T18:48:46.250Z"
    }
  ]
}
```

Three things in this response drive the configuration below: the next page link at `info.next`, the records under `results`, and the `created` timestamp that you will use for incremental loading.

## Load the API into DuckDB

Create a file called `rick_and_morty_pipeline.py`:

```py
import dlt
from dlt.sources.rest_api import rest_api_source

rick_and_morty = rest_api_source({
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
    "resources": ["character", "location", "episode"],
})

pipeline = dlt.pipeline(
    pipeline_name="rick_and_morty",
    destination="duckdb",
    dataset_name="rick_and_morty_data",
)

load_info = pipeline.run(rick_and_morty)

print(load_info)
print(pipeline.last_trace.last_normalize_info)
```

The [REST API source](../dlt-ecosystem/verified-sources/rest_api/basic.md) builds one resource per endpoint from this declarative configuration:

- `base_url` is prepended to each resource name, so `"character"` becomes a request to `https://rickandmortyapi.com/api/character`.
- `paginator` tells the client where the link to the next page lives. `json_link` follows the URL found at `info.next` until the API stops returning one. See [pagination](../dlt-ecosystem/verified-sources/rest_api/basic.md#pagination) for the other paginator types.
- `data_selector` picks the record list out of the envelope, so the `info` object does not end up in your tables.
- `resource_defaults` applies to all three resources: `id` is the primary key and `merge` is the [write disposition](../general-usage/merge-loading.md), which matters when you [run the script again](#run-it-again-without-duplicating-rows).

:::tip
Both `paginator` and `data_selector` are optional here, because `dlt` detects them from the first response. Setting them explicitly documents the shape of the API for the next person reading your pipeline.
:::

Run the script:

```sh
python rick_and_morty_pipeline.py
```

```text
Pipeline rick_and_morty load step finished in 0.53 seconds
1 load package(s) were loaded to destination duckdb and into dataset rick_and_morty_data
The duckdb destination used duckdb:////home/user-name/rick_and_morty/rick_and_morty.duckdb location to store data
Load package 1786193992.8244705 is LOADED and contains no failed jobs
Normalized data for the following tables:
- character: 826 row(s)
- character__episode: 1267 row(s)
- location: 126 row(s)
- location__residents: 804 row(s)
- episode: 51 row(s)
- episode__characters: 1266 row(s)
```

## What dlt created

Three endpoints produced six tables. `character`, `location`, and `episode` hold one row per record, and the three tables with the double underscore are [nested tables](../general-usage/destination-tables.md#nested-tables): every list in the JSON becomes a table of its own, linked to its parent row by `_dlt_parent_id`. The `episode` list of a character lands in `character__episode`, the `residents` list of a location in `location__residents`.

Objects are flattened instead, and the column names keep the path. This is the `character` table:

```text
id                bigint
name              text
status            text
species           text
type              text
gender            text
image             text
url               text
created           timestamp
location__name    text
location__url     text
origin__name      text
origin__url       text
_dlt_load_id      text
_dlt_id           text
```

The `origin` and `location` objects became `origin__name`, `origin__url`, `location__name`, and `location__url`. `created` was inferred as a `timestamp` from the ISO string in the JSON, and `id` as `bigint`. The two `_dlt_` columns identify the row and the load package that produced it. Read [destination tables](../general-usage/destination-tables.md) for what `dlt` stores next to your data.

You did not declare any of this. If the API adds a field tomorrow, the column appears on the next run. Read [schema evolution](../general-usage/schema-evolution.md) for how far that goes and how to constrain it.

## Explore the loaded data

`pipeline.dataset()` gives you the loaded tables in Python, without opening the DuckDB file yourself:

```py
import dlt

pipeline = dlt.pipeline(
    pipeline_name="rick_and_morty",
    destination="duckdb",
    dataset_name="rick_and_morty_data",
)
dataset = pipeline.dataset()

# how many rows ended up in each table
print(dataset.row_counts().df())

# the first rows of the character table
print(dataset.character.select("id", "name", "status", "species", "origin__name").head().df())

# any SQL query against the same dataset
print(dataset("SELECT status, count(*) AS n FROM character GROUP BY status ORDER BY n DESC").df())
```

```text
            table_name  row_count
0            character        826
1             location        126
2              episode         51
3   character__episode       1267
4  location__residents        804
5  episode__characters       1266

   id          name status species                   origin__name
0   1  Rick Sanchez  Alive   Human                  Earth (C-137)
1   2   Morty Smith  Alive   Human                        unknown
2   3  Summer Smith  Alive   Human  Earth (Replacement Dimension)
3   4    Beth Smith  Alive   Human  Earth (Replacement Dimension)
4   5   Jerry Smith  Alive   Human  Earth (Replacement Dimension)

    status    n
0    Alive  439
1     Dead  287
2  unknown  100
```

Relations are lazy: nothing is read from DuckDB until you call `df()`, `arrow()`, or iterate. Read [access loaded data in Python](../general-usage/dataset-access/dataset.md) for chunked reads, joins, and Ibis expressions.

To browse the same data in a UI, run the workspace dashboard from the folder that holds your script:

```sh
pip install marimo
dlt pipeline rick_and_morty show
```

## Run it again without duplicating rows

Run `python rick_and_morty_pipeline.py` a second time. The row counts do not change: `character` still holds 826 rows, not 1652.

That is the `"write_disposition": "merge"` and `"primary_key": "id"` from `resource_defaults` at work. `dlt` loads the incoming rows into a staging dataset and upserts them into the final tables on `id`, so a character that changed is updated in place and one you already have is not duplicated. The nested tables follow their parent rows. See [merge loading](../general-usage/merge-loading.md) for the other merge strategies.

Use `"write_disposition": "replace"` instead when you want each run to start from an empty table, and `"append"` when the API only ever emits new immutable records.

## Load only the records that are new

Every record in this API carries a `created` timestamp, so `dlt` can track how far it got and skip records it has already seen. Add an `incremental` section to `resource_defaults` in `rick_and_morty_pipeline.py`:

```py
import dlt
from dlt.sources.rest_api import rest_api_source

rick_and_morty = rest_api_source({
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
            "incremental": {
                "cursor_path": "created",
                "initial_value": "2017-11-04T00:00:00.000Z",
            },
        },
    },
    "resources": ["character", "location", "episode"],
})

pipeline = dlt.pipeline(
    pipeline_name="rick_and_morty",
    destination="duckdb",
    dataset_name="rick_and_morty_data",
)

load_info = pipeline.run(rick_and_morty)

print(load_info)
```

`cursor_path` names the field to track in each record and `initial_value` is where the first run starts. `dlt` keeps the highest `created` it saw in the [pipeline state](../general-usage/state.md), so the run after that one loads nothing at all:

```text
Pipeline rick_and_morty load step did not start
0 load package(s) were loaded to destination duckdb and into dataset None
No data found to normalize
```

:::note
The Rick and Morty API has no "created since" query parameter, so `dlt` still requests every page and drops the records it has already loaded on the client side. When an API does accept such a parameter, pass the cursor to it with the `{incremental.start_value}` placeholder and the requests themselves get cheaper. See [using placeholders for incremental loading](../dlt-ecosystem/verified-sources/rest_api/basic.md#using-placeholders-for-incremental-loading).
:::

Read [incremental loading](../general-usage/incremental-loading.md) and the [cursor-based](../general-usage/incremental/cursor.md) guide for deduplication, lag, and backfills.

## Narrow the load with query parameters

The API filters characters server side by `status`, `species`, `gender`, and `name`. Replace the plain `"character"` entry with an explicit resource to pass query parameters:

```py
import dlt
from dlt.sources.rest_api import rest_api_source

alive_characters = rest_api_source({
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
        {
            "name": "character",
            "endpoint": {
                "path": "character",
                "params": {"status": "alive"},
            },
        },
        "location",
        "episode",
    ],
})

pipeline = dlt.pipeline(
    pipeline_name="rick_and_morty",
    destination="duckdb",
    dataset_name="rick_and_morty_data",
)

load_info = pipeline.run(alive_characters)

print(load_info)
```

The `character` table now receives 439 rows instead of 826, and the API serves them in 22 pages instead of 42. The values in `resource_defaults` still apply, because an explicit resource only overrides the keys it sets.

## Write the resource yourself

The REST API source is a convenience layer. When you want plain Python, for example to call several endpoints in one function or to reshape records before they are yielded, write a [resource](../general-usage/resource.md) and use the same HTTP helpers that the source uses:

```py
import dlt
from dlt.sources.helpers.rest_client import paginate
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator


@dlt.resource(name="character", primary_key="id", write_disposition="merge")
def characters(status: str = "alive"):
    yield from paginate(
        "https://rickandmortyapi.com/api/character",
        params={"status": status},
        paginator=JSONLinkPaginator(next_url_path="info.next"),
        data_selector="results",
    )


pipeline = dlt.pipeline(
    pipeline_name="rick_and_morty_custom",
    destination="duckdb",
    dataset_name="rick_and_morty_data",
)

load_info = pipeline.run(characters(status="dead"))

print(load_info)
```

`paginate()` yields one page of records at a time, so the whole API is never held in memory. The decorator carries the same hints you set declaratively above, and the resulting tables, including `character__episode`, are identical.

## What's next

- Move the same dataset to a warehouse by [changing the destination](share-a-dataset.md).
- [Run and troubleshoot the pipeline](run-a-pipeline.md): progress bars, load packages, failed jobs.
- Send each record to a different table with [dispatching](dispatch-to-multiple-tables.md), the way GitHub events are split by event type.
- Configure authentication, resource relationships, and response transformations in the [REST API source](../dlt-ecosystem/verified-sources/rest_api/basic.md) reference.
