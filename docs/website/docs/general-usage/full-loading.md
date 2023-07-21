---
title: Full loading
description: Full loading with dlt
keywords: [full loading, loading methods, replace]
---
# Full loading

Full loading is the act of fully reloading the data of your tables. All existing data
will be removed and replaced by whatever the source produced on this run. Resources
that are not selected while performing a full load will not replace any data in the destination.

## Performing a full load

To perform a full load on one or more of your resources, choose the `write_disposition='replace'` for this resource:

```python
p = dlt.pipeline(destination="bigquery", dataset_name="github")
issues = []
reactions = ["%2B1", "-1", "smile", "tada", "thinking_face", "heart", "rocket", "eyes"]
for reaction in reactions:
    for page_no in range(1, 3):
      page = requests.get(f"https://api.github.com/repos/{repo}/issues?state=all&sort=reactions-{reaction}&per_page=100&page={page_no}", headers=headers)
      print(f"got page for {reaction} page {page_no}, requests left", page.headers["x-ratelimit-remaining"])
      issues.extend(page.json())
p.run(issues, write_disposition="replace", primary_key="id", table_name="issues")
```

## Choosing the correct replace strategy for your full load

`dlt` implements three different strategies for doing a full load on your table: `truncate-and-insert`, `insert-from-staging` and `staging-optimized`. The exact behaviour of these strategies can also vary between the available destinations.

You can select a strategy with a setting in your `config.toml` file. If you do not select a strategy, dlt will default to `truncate-and-insert`.

```toml
[destination]
# set the optimized replace strategy
replace_strategy = "staging-optimized"
```

### The `truncate-and-insert` strategy

The `truncate-and-insert` replace strategy is the default and the fastest of all three strategies. If you load data with this setting, then the
destination tables will be truncated and the new data will be inserted. The downside of this strategy is, that there is no transaction safety
that ensures that parent tables and child tables will be in a consistent state if your pipeline run fails at some point during the loading. You
may end up with new data in the main table and stale data in the its child tables. Furthermore on large loads, there is a possibility that not
all data present in the resource will actually end up in the destination table, as `dlt` chunks data into predefined (and configurable) filesizes
before loading into the destination, and there is no mechnism to ensure that chunked data will be appended back together. If you have large loads
or consistently failing pipeline runs, it is better to choose one of the other two strategies. This strategy behaves the same way across all
destinations.

### The `insert-from-staging` strategy

The `insert-from-staging` is the slowest of all three strategies. It will load all new data into staging tables away from your final destination tables and will then truncate and insert the new data in one transaction. It also maintains a consistent state between child and parent tables.
Use this strategy if you have large loads or the requirement for consistent destination datasets with zero downtime and the `optimized` strategy does not work for you. This strategy behaves the same way across all destinations.

### The `staging-optimized` strategy

The `staging-optimized` strategy has all the upsides of the `insert-from-staging` but implements certain optimizations for faster loading on some destinations.
This comes at the cost of destination tables being dropped and recreated in some cases, which will mean that any views or other constraints you have
placed on those tables will be dropped with the table. If you have a setup where you need to retain your destination tables, do not use the `staging-optimized`
strategy. If you do not care about tables being dropped but need the upsides of the `insert-from-staging` with some performance (and cost) saving
opportunities, you should use this strategy. The `staging-optimized` strategy behaves differently across destinations:

* Postgres: After loading the new data into the staging tables, the destination tables will be dropped and replaced by the staging tables. No data needs to be moved, so this strategy is almost as fast as `truncate-and-insert`.
* bigquery: After loading the new data into the staging tables, the destination tables will be dropped and
  recreated with a [clone command](https://cloud.google.com/bigquery/docs/table-clones-create) from the staging tables. This is a low cost and fast way to create a second independent table from the data of another. Learn
  more about [table cloning on bigquery](https://cloud.google.com/bigquery/docs/table-clones-intro).
* snowflake: After loading the new data into the staging tables, the destination tables will be dropped and
  recreated with a [clone command](https://docs.snowflake.com/en/sql-reference/sql/create-clone) from the staging tables. This is a low cost and fast way to create a second independent table from the data of another. Learn
  more about [table cloning on snowflake](https://docs.snowflake.com/en/user-guide/object-clone).

For all other destinations the `staging-optimized` will fall back to the behavior of the `insert-from-staging` strategy.


