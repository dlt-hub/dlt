---
title: State
description: Explanation of what a dlt state is
keywords: [state, metadata, dlt.current.resource_state, dlt.current.source_state]
---

# State

The pipeline state is a Python dictionary that lives alongside your data; you can store values in
it and, on the next pipeline run, request them back.

## Read and write pipeline state in a resource

You read and write the state in your resources. Below, we use the state to create a list of chess
game archives, which we then use to
[prevent requesting duplicates](incremental-loading.md#advanced-state-usage-storing-a-list-of-processed-entities).

```py
@dlt.resource(write_disposition="append")
def players_games(chess_url, player, start_month=None, end_month=None):
    # create or request a list of archives from resource-scoped state
    checked_archives = dlt.current.resource_state().setdefault("archives", [])
    # get a list of archives for a particular player
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

Above, we request the resource-scoped state. The `checked_archives` list stored under the `archives`
dictionary key is private and visible only to the `players_games` resource.

The pipeline state is stored locally in the
[pipeline working directory](pipeline.md#pipeline-working-directory) and, as a consequence, it
cannot be shared with pipelines with different names. You must also make sure that data written into
the state is JSON serializable. Except for standard Python types, `dlt` handles `DateTime`, `Decimal`,
`bytes`, and `UUID`.

## Share state across resources and read state in a source

You can also access the source-scoped state with `dlt.current.source_state()`, which can be shared
across resources of a particular source and is also available read-only in the source-decorated
functions. The most common use case for the source-scoped state is to store a mapping of custom fields
to their displayable names. You can take a look at our
[pipedrive source](https://github.com/dlt-hub/verified-sources/blob/master/sources/pipedrive/__init__.py#L118)
for an example of state passed across resources.

:::tip
[Decompose your source](../reference/performance.md#source-decomposition-for-serial-and-parallel-resource-execution)
to, for example, run it on Airflow in parallel. If you cannot avoid that, designate one of
the resources as the state writer and all others as state readers. This is exactly what the `pipedrive`
pipeline does. With such a structure, you will still be able to run some of your resources in
parallel.
:::
:::caution
The `dlt.state()` is a deprecated alias to `dlt.current.source_state()` and will soon be
removed.
:::

## Syncing state with destination

What if you run your pipeline on, for example, Airflow, where every task gets a clean filesystem and
the [pipeline working directory](pipeline.md#pipeline-working-directory) is always deleted? `dlt` loads
your state into the destination along with all other data, and when faced with a clean start, it
will try to restore the state from the destination.

The remote state is identified by the pipeline name, the destination location (as given by the
credentials), and the destination dataset. To reuse the same state, use the same pipeline name and
destination.

The state is stored in the `_dlt_pipeline_state` table at the destination and contains information
about the pipeline, the pipeline run (to which the state belongs), and the state blob.

`dlt` has a `dlt pipeline sync` command where you can
[request the state back from that table](../reference/command-line-interface.md#sync-pipeline-with-the-destination).

> 💡 If you can keep the pipeline working directory across the runs, you can disable the state sync
> by setting `restore_from_destination=false` in your `config.toml`.

## When to use pipeline state

- `dlt` uses the state internally to implement
  [last value incremental loading](incremental-loading.md#incremental_loading-with-last-value). This
  use case should cover around 90% of your needs to use the pipeline state.
- [Store a list of already requested entities](incremental-loading.md#advanced-state-usage-storing-a-list-of-processed-entities)
  if the list is not much bigger than 100k elements.
- [Store large dictionaries of last values](incremental-loading.md#advanced-state-usage-tracking-the-last-value-for-all-search-terms-in-twitter-api)
  if you are not able to implement it with the standard incremental construct.
- Store custom fields dictionaries, dynamic configurations, and other source-scoped state.

## Do not use pipeline state if it can grow to millions of records

Do not use `dlt` state when it may grow to millions of elements. Do you plan to store modification
timestamps of all your millions of user records? This is probably a bad idea! In that case, you
could:

- Store the state in DynamoDB, Redis, etc., taking into account that if the extract stage fails,
  you'll end up with an invalid state.
- Use your loaded data as the state. `dlt` exposes the current pipeline via `dlt.current.pipeline()`
  from which you can obtain
  [sqlclient](../dlt-ecosystem/transformations/sql.md)
  and load the data of interest. In that case, try at least to process your user records in batches.

### Access data in the destination instead of pipeline state

In the example below, we load recent comments made by a given `user_id`. We access the `user_comments` table to select the maximum comment id for a given user.
```py
import dlt

@dlt.resource(name="user_comments")
def comments(user_id: str):
    current_pipeline = dlt.current.pipeline()
    # find the last comment id for the given user_id by looking in the destination
    max_id: int = 0
    # on the first pipeline run, the user_comments table does not yet exist so do not check at all
    # alternatively, catch DatabaseUndefinedRelation which is raised when an unknown table is selected
    if not current_pipeline.first_run:
        with current_pipeline.sql_client() as client:
            # we may get the last user comment or None which we replace with 0
            max_id = (
                client.execute_sql(
                    "SELECT MAX(_id) FROM user_comments WHERE user_id=?", user_id
                )[0][0]
                or 0
            )
    # use max_id to filter our results (we simulate an API query)
    yield from [
        {"_id": i, "value": letter, "user_id": user_id}
        for i, letter in zip([1, 2, 3], ["A", "B", "C"])
        if i > max_id
    ]
```
When the pipeline is first run, the destination dataset and `user_comments` table do not yet exist. We skip the destination query by using the `first_run` property of the pipeline. We also handle a situation where there are no comments for a user_id by replacing None with 0 as `max_id`.

## Inspect the pipeline state

You can inspect the pipeline state with the [`dlt pipeline` command](../reference/command-line-interface.md#dlt-pipeline):

```sh
dlt pipeline -v chess_pipeline info
```

This will display the source and resource state slots for all known sources.

## Reset the pipeline state: full or partial

**To fully reset the state:**

- Drop the destination dataset to fully reset the pipeline.
- [Set the `dev_mode` flag when creating the pipeline](pipeline.md#do-experiments-with-dev-mode).
- Use the `dlt pipeline drop --drop-all` command to [drop the state and tables for a given schema name](../reference/command-line-interface.md#selectively-drop-tables-and-reset-state).

**To partially reset the state:**

- Use the `dlt pipeline drop <resource_name>` command to [drop the state and tables for a given resource](../reference/command-line-interface.md#selectively-drop-tables-and-reset-state).
- Use the `dlt pipeline drop --state-paths` command to [reset the state at a given path without touching the tables and data](../reference/command-line-interface.md#selectively-drop-tables-and-reset-state).

