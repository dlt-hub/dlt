---
title: Incremental loading
description: Incremental loading with dlt
keywords: [incremental loading, loading methods, append, merge]
---

# Incremental loading

Incremental loading is the act of loading only new or changed data, not old records that we have already loaded. It enables low-latency and low-cost data transfer.

The challenge of incremental pipelines is that if we do not keep track of the state of the load (i.e., which increments were loaded and which are to be loaded). Read more about state [here](state.md).

## Choosing a write disposition

### The 3 write dispositions:

- **Full load**: This replaces the destination dataset with whatever the source produced on this run. To achieve this, use `write_disposition='replace'` in your resources. Learn more in the [full loading docs](./full-loading.md).

- **Append**: This appends the new data to the destination. Use `write_disposition='append'`.

- **Merge**: This merges new data to the destination using `merge_key` and/or deduplicates/upserts new data using `primary_key`. Use `write_disposition='merge'`.

### Two simple questions determine the write disposition you use

<div style={{textAlign: 'center'}}>

![write disposition flowchart](/img/write-dispo-choice.png)

</div>

The "write disposition" you choose depends on the dataset and how you can extract it.

To find the "write disposition" you should use, the first question you should ask yourself is, "Is my data stateful or stateless?" Stateful data has a state that is subject to change - for example, a user's profile. Stateless data cannot change - for example, a recorded event, such as a page view.

Because stateless data does not need to be updated, we can just append it.

For stateful data, a second question arises - Can I extract it incrementally from the source? If not, then we need to replace the entire dataset. If, however, we can request the data incrementally, such as "all users added or modified since yesterday," then we can simply apply changes to our existing dataset with the merge write disposition.

## Merge incremental loading

The `merge` write disposition is used in two scenarios:

1. You want to keep only one instance of a certain record, i.e., you receive updates of the `user` state from an API and want to keep just one record per `user_id`.
1. You receive data in daily batches, and you want to make sure that you always keep just a single instance of a record for each batch, even in case you load an old batch or load the current batch several times a day (i.e., to receive "live" updates).

The `merge` write disposition loads data to a `staging` dataset, deduplicates the staging data if a `primary_key` is provided, deletes the data from the destination using `merge_key` and `primary_key`, and then inserts the new records. All of this happens in a single atomic transaction for a parent and all child tables.

The example below loads all the GitHub events and updates them in the destination using "id" as the primary key, making sure that only a single copy of the event is present in the `github_repo_events` table:

```python
@dlt.resource(primary_key="id", write_disposition="merge")
def github_repo_events():
    yield from _get_event_pages()
```

You can use compound primary keys:

```python
@dlt.resource(primary_key=("id", "url"), write_disposition="merge")
...
```

The example below merges on a column `batch_day` that holds the day for which a given record is valid. Merge keys also can be compound:

```python
@dlt.resource(merge_key="batch_day", write_disposition="merge")
def get_daily_batch(day):
    yield _get_batch_from_bucket(day)
```

As with any other write disposition, you can use it to load data ad hoc. Below we load issues with top reactions for the `duckdb` repo. The lists have, obviously, many overlapping issues, but we want to keep just one instance of each.

```python
p = dlt.pipeline(destination="bigquery", dataset_name="github")
issues = []
reactions = ["%2B1", "-1", "sm
<!---
grammarcheck: true
-->
