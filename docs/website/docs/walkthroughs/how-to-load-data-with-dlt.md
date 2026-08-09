---
title: How to load data with dlt
description: Load the Rick and Morty API into DuckDB with dlt and explore the tables it creates
keywords: [rest api, duckdb, rick and morty, nested tables, merge]
---

# How to load data with dlt

This is a practical example of loading a public REST API into a local database. We use the
[Rick and Morty API](https://rickandmortyapi.com/documentation), which needs no credentials, and
[duckdb](../dlt-ecosystem/destinations/duckdb) as a destination. The API paginates its responses and
returns nested objects and lists, so it shows what `dlt` does with a typical API payload: pagination,
nested tables, and reloading without duplicates.

## Setup

Install `dlt` with duckdb support:

```sh
pip install "dlt[duckdb]"
```

## 1. Load three endpoints

Create a new file `rick_and_morty_pipeline.py` and paste the following code:

```py
import dlt
from dlt.sources.rest_api import rest_api_source

source = rest_api_source({
    "client": {
        "base_url": "https://rickandmortyapi.com/api/",
    },
    "resource_defaults": {
        "primary_key": "id",
        "write_disposition": "merge",
    },
    "resources": ["character", "episode", "location"],
})

pipeline = dlt.pipeline(
    pipeline_name="rick_and_morty",
    destination="duckdb",
    dataset_name="rick_and_morty_data",
)

load_info = pipeline.run(source)
print(load_info)
```

Now run the script:

```sh
python rick_and_morty_pipeline.py
```

```text
Pipeline rick_and_morty load step finished in 0.49 seconds
1 load package(s) were loaded to destination duckdb and into dataset rick_and_morty_data
The duckdb destination used duckdb:////home/user-name/rick_and_morty.duckdb location to store data
Load package 1786268457.810038 is LOADED and contains no failed jobs
```

What the configuration does:

* Each string in `resources` is used as the endpoint path, the resource name, and the table name. So
  `"character"` is requested from `https://rickandmortyapi.com/api/character` and lands in the
  `character` table.
* Nothing in the configuration mentions pagination. The API returns `{"info": {...}, "results": [...]}`
  with a link to the next page in `info.next`, and the [REST API source detects both the paginator and
  the `results` field](../dlt-ecosystem/verified-sources/rest_api/basic#pagination) on its own.
* `resource_defaults` applies `primary_key` and `write_disposition` to all three resources at once, so
  every endpoint is loaded with the [merge write disposition](../general-usage/merge-loading) on `id`.

## 2. See the tables that were created

The three endpoints produce six tables. Objects nested in a record are flattened into columns of the
parent table, and lists become separate [nested tables](../general-usage/destination-tables#nested-tables)
linked to their parent:

| Table | What it holds |
| --- | --- |
| `character` | One row per character. The `origin` and `location` objects become the `origin__name`, `origin__url`, `location__name`, and `location__url` columns. |
| `character__episode` | One row per entry of a character's `episode` list, in the `value` column. |
| `episode` | One row per episode. |
| `episode__characters` | One row per entry of an episode's `characters` list. |
| `location` | One row per location. |
| `location__residents` | One row per entry of a location's `residents` list. |

`dlt` also adds `_dlt_id` and `_dlt_load_id` columns and its own tables for load history and pipeline
state - see [destination tables](../general-usage/destination-tables).

Print the schema that was inferred from the API responses:

```sh
dlt pipeline rick_and_morty schema
```

Or browse the data and the load status in the workspace dashboard:

```sh
pip install marimo
dlt pipeline rick_and_morty show
```

:::tip
Nested tables are convenient, but you do not have to keep them: you can
[reduce the nesting level](../general-usage/source#reduce-the-nesting-level-of-generated-tables) and
store the lists as JSON columns instead.
:::

## 3. Query the loaded data

You do not need a duckdb client to look at the result. Every pipeline exposes its
[dataset](../general-usage/dataset-access/dataset):

```py
import dlt

pipeline = dlt.pipeline(
    pipeline_name="rick_and_morty",
    destination="duckdb",
    dataset_name="rick_and_morty_data",
)
dataset = pipeline.dataset()

# a table_name / row_count overview of the whole dataset
print(dataset.row_counts().df())

# a few characters, as a pandas dataframe
print(
    dataset["character"]
    .select("id", "name", "status", "species", "location__name")
    .limit(5)
    .df()
)
```

```text
   id          name status species                 location__name
0   1  Rick Sanchez  Alive   Human               Citadel of Ricks
1   2   Morty Smith  Alive   Human               Citadel of Ricks
2   3  Summer Smith  Alive   Human  Earth (Replacement Dimension)
3   4    Beth Smith  Alive   Human  Earth (Replacement Dimension)
4   5   Jerry Smith  Alive   Human  Earth (Replacement Dimension)
```

`select`, `limit`, and `df` are executed on duckdb, not in Python, so you can explore tables that do
not fit in memory. Use `arrow()` or `fetchall()` instead of `df()` if you prefer Arrow tables or plain
rows.

## 4. Run the pipeline again

Run `python rick_and_morty_pipeline.py` a second time and query `dataset.row_counts()` again: the
counts stay the same. Because the resources declare `primary_key="id"` and
`write_disposition="merge"`, a character that is loaded again replaces the row with the same `id`
instead of being appended, and the rows of its nested tables are replaced along with it.

The Rick and Morty API has no "changed since" filter, so every run still downloads all pages. If your
API can return only new or updated records, configure
[incremental loading](../general-usage/incremental-loading) and let `dlt` pass the last seen value as
a query parameter.

:::tip
When the API starts returning a field you have not seen before, `dlt` adds the column on the next run
rather than failing the load. See [schema evolution](../general-usage/schema-evolution) for how to
follow and control those changes.
:::

## 5. Load only part of an endpoint

The character endpoint accepts filters as query parameters. Configure a resource explicitly to pass
them, and give it a name so that it lands in a table of its own:

```py
import dlt
from dlt.sources.rest_api import rest_api_source

alive_humans = rest_api_source({
    "client": {
        "base_url": "https://rickandmortyapi.com/api/",
    },
    "resources": [
        {
            "name": "alive_human",
            "primary_key": "id",
            "write_disposition": "merge",
            "endpoint": {
                "path": "character",
                "params": {"status": "alive", "species": "human"},
            },
        },
    ],
})

pipeline = dlt.pipeline(
    pipeline_name="rick_and_morty",
    destination="duckdb",
    dataset_name="rick_and_morty_data",
)

load_info = pipeline.run(alive_humans)
print(load_info)
```

This requests `https://rickandmortyapi.com/api/character?status=alive&species=human` and loads the
result into the `alive_human` table, next to the tables from the previous runs.

## 6. Write the source in Python instead

Declarative configuration is not the only option. When you need full control over the requests, write
a [resource](../general-usage/resource) as a Python generator. The `paginate` helper does the same
paginator detection the REST API source does:

```py
import dlt
from dlt.sources.helpers.rest_client import paginate

@dlt.resource(name="character", primary_key="id", write_disposition="merge")
def characters(status: str = "alive"):
    for page in paginate(
        "https://rickandmortyapi.com/api/character",
        params={"status": status},
    ):
        yield page

pipeline = dlt.pipeline(
    pipeline_name="rick_and_morty_python",
    destination="duckdb",
    dataset_name="rick_and_morty_data",
)

load_info = pipeline.run(characters)
print(load_info)
```

The resource yields whole pages - `dlt` unpacks lists of records for you - and the decorator carries
the same table name, primary key, and write disposition you configured declaratively above.

## Next steps

* [Run and troubleshoot a pipeline](run-a-pipeline) to inspect load packages and traces.
* [REST API source](../dlt-ecosystem/verified-sources/rest_api/basic) for authentication, resource
  relationships, and response transformations.
* [Moving from local to production](share-a-dataset) to send the same data to a warehouse instead of
  duckdb.
