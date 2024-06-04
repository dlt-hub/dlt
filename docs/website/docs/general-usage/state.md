---
title: State
description: Explanation of what a dlt state is
keywords: [state, metadata, dlt.current.resource_state, dlt.current.source_state]
---

# State

The pipeline state is a Python dictionary which lives alongside your data; you can store values in
it and, on next pipeline run, request them back.

## Read and write pipeline state in a resource

You read and write the state in your resources. Below we use the state to create a list of chess
game archives which we then use to
[prevent requesting duplicates](incremental-loading.md#advanced-state-usage-storing-a-list-of-processed-entities).

```py
@dlt.resource(write_disposition="append")
def players_games(chess_url, player, start_month=None, end_month=None):
    # create or request a list of archives from resource scoped state
    checked_archives = dlt.current.resource_state().setdefault("archives", [])
    # get list of archives for a particular player
    archives = player_archives(chess_url, player)
    for url in archives:
        if url in checked_archives:
            print(f"skipping archive {url}")
            continue
        else:
            print(f"getting archive {url}")
            checked_archives.append(url)
        # get the filtered archive
        r = requests.get(url)
        r.raise_for_status()
        yield r.json().get("games", [])
```

Above, we request the resource-scoped state. The `checked_archives` list stored under `archives`
dictionary key is private and visible only to the `players_games` resource.

The pipeline state is stored locally in
[pipeline working directory](pipeline.md#pipeline-working-directory) and as a consequence - it
cannot be shared with pipelines with different names. You must also make sure that data written into
the state is JSON Serializable. Except standard Python types, `dlt` handles `DateTime`, `Decimal`,
`bytes` and `UUID`.

## Share state across resources and read state in a source

You can also access the source-scoped state with `dlt.current.source_state()` which can be shared
across resources of a particular source and is also available read-only in the source-decorated
functions. The most common use case for the source-scoped state is to store mapping of custom fields
to their displayable names. You can take a look at our
[pipedrive source](https://github.com/dlt-hub/verified-sources/blob/master/sources/pipedrive/__init__.py#L118)
for an example of state passed across resources.

:::tip
[decompose your source](../reference/performance.md#source-decomposition-for-serial-and-parallel-resource-execution)
in order to, for example run it on Airflow in parallel. If you cannot avoid that, designate one of
the resources as state writer and all the other as state readers. This is exactly what `pipedrive`
pipeline does. With such structure you will still be able to run some of your resources in
parallel.
:::
:::caution
The `dlt.state()` is a deprecated alias to `dlt.current.source_state()` and will be soon
removed.
:::

## Syncing state with destination

What if you run your pipeline on, for example, Airflow where every task gets a clean filesystem and
[pipeline working directory](pipeline.md#pipeline-working-directory) is always deleted? `dlt` loads
your state into the destination together with all other data and when faced with a clean start, it
will try to restore state from the destination.

The remote state is identified by pipeline name, the destination location (as given by the
credentials) and destination dataset. To re-use the same state, use the same pipeline name and
destination.

The state is stored in the `_dlt_pipeline_state` table at the destination and contains information
about the pipeline, pipeline run (that the state belongs to) and state blob.

`dlt` has `dlt pipeline sync` command where you can
[request the state back from that table](../reference/command-line-interface.md#sync-pipeline-with-the-destination).

> 💡 If you can keep the pipeline working directory across the runs, you can disable the state sync
> by setting `restore_from_destination=false` i.e. in your `config.toml`.

## When to use pipeline state

- `dlt` uses the state internally to implement
  [last value incremental loading](incremental-loading.md#incremental_loading-with-last-value). This
  use case should cover around 90% of your needs to use the pipeline state.
- [Store a list of already requested entities](incremental-loading.md#advanced-state-usage-storing-a-list-of-processed-entities)
  if the list is not much bigger than 100k elements.
- [Store large dictionaries of last values](incremental-loading.md#advanced-state-usage-tracking-the-last-value-for-all-search-terms-in-twitter-api)
  if you are not able to implement it with the standard incremental construct.
- Store the custom fields dictionaries, dynamic configurations and other source-scoped state.

## When not to use pipeline state

Do not use dlt state when it may grow to millions of elements. Do you plan to store modification
timestamps of all of your millions of user records? This is probably a bad idea! In that case you
could:

- Store the state in dynamo-db, redis etc. taking into the account that if the extract stage fails
  you'll end with invalid state.
- Use your loaded data as the state. `dlt` exposes the current pipeline via `dlt.current.pipeline()`
  from which you can obtain
  [sqlclient](../dlt-ecosystem/transformations/sql.md)
  and load the data of interest. In that case try at least to process your user records in batches.

## Inspect the pipeline state

You can inspect pipeline state with
[`dlt pipeline` command](../reference/command-line-interface.md#dlt-pipeline):

```sh
dlt pipeline -v chess_pipeline info
```

will display source and resource state slots for all known sources.

## Reset the pipeline state: full or partial

**To fully reset the state:**

- Drop the destination dataset to fully reset the pipeline.
- [Set the `dev_mode` flag when creating pipeline](pipeline.md#do-experiments-with-dev-mode).
- Use the `dlt pipeline drop --drop-all` command to
  [drop state and tables for a given schema name](../reference/command-line-interface.md#selectively-drop-tables-and-reset-state).

**To partially reset the state:**

- Use the `dlt pipeline drop <resource_name>` command to
  [drop state and tables for a given resource](../reference/command-line-interface.md#selectively-drop-tables-and-reset-state).
- Use the `dlt pipeline drop --state-paths` command to
  [reset the state at given path without touching the tables and data](../reference/command-line-interface.md#selectively-drop-tables-and-reset-state).
