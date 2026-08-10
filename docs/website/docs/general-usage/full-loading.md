---
title: Full loading
description: Full loading with dlt
keywords: [full loading, loading methods, replace]
---
# Full loading

Full loading is the act of fully reloading the data of your tables. All existing data will be removed and replaced by whatever the source produced on this run. Resources that are not selected while performing a full load will not replace any data in the destination.

## Performing a full load

To perform a full load on one or more of your resources, choose the `write_disposition='replace'` for this resource:

```py
p = dlt.pipeline(destination="bigquery", dataset_name="github")
issues = []
reactions = ["%2B1", "-1", "smile", "tada", "thinking_face", "heart", "rocket", "eyes"]
for reaction in reactions:
    for page_no in range(1, 3):
      page = requests.get(f"https://api.github.com/repos/{REPO_NAME}/issues?state=all&sort=reactions-{reaction}&per_page=100&page={page_no}", headers=headers)
      print(f"Got page for {reaction} page {page_no}, requests left", page.headers["x-ratelimit-remaining"])
      issues.extend(page.json())
p.run(issues, write_disposition="replace", primary_key="id", table_name="issues")
```

:::caution
All tables that belong to a `replace` resource are truncated on each load, including nested tables and tables created by
[dispatching to many tables](resource.md#dispatch-data-to-many-tables) or as table variants.

Note that a table does not need to receive any data to get truncated.
:::

## Choosing the correct replace strategy for your full load

dlt implements three different strategies for doing a full load on your table: `truncate-and-insert`, `insert-from-staging`, and `staging-optimized`. The exact behavior of these strategies can also vary between the available destinations.

You can select a strategy with a setting in your `config.toml` file. If you do not select a strategy, dlt uses the first strategy that the destination supports for the table being loaded, which is `truncate-and-insert` wherever it is available.

```toml
[destination]
# Set the optimized replace strategy
replace_strategy = "staging-optimized"
```

`replace_strategy` belongs to the destination configuration, so you can also scope it to one destination type or to one pipeline:

```toml
# only for the duckdb destination
[destination.duckdb]
replace_strategy = "insert-from-staging"

# only for the pipeline named "github_issues"
[github_issues.destination]
replace_strategy = "staging-optimized"
```

Or pass it to the destination factory, which takes precedence over `config.toml`:

<!--@@@DLT_SNIPPET ./full-loading-snippets.py::set_replace_strategy-->

There is no per-resource setting: every table loaded through one destination uses the configured strategy, and the only per-table variation is the narrowing described in [Which strategies your destination supports](#which-strategies-your-destination-supports).

### The `truncate-and-insert` strategy

The `truncate-and-insert` replace strategy is the fastest of all three strategies and the default wherever the destination supports it. If you load data with this setting, then the destination tables will be truncated at the beginning of the load, and the new data will be inserted consecutively but not within the same transaction.
The downside of this strategy is that your tables will have no data for a while until the load is completed. You may end up with new data in some tables and no data in other tables if the load fails during the run. Such an incomplete load may be detected by checking if the [_dlt_loads table contains a load id](destination-tables.md#load-packages-and-load-ids) from _dlt_load_id of the replaced tables. If you prefer to have no data downtime, please use one of the other strategies.

### The `insert-from-staging` strategy

The `insert-from-staging` strategy is the slowest of all three strategies. It will load all new data into staging tables away from your final destination tables and will then truncate and insert the new data in one transaction.
It also maintains a consistent state between nested and root tables at all times. Use this strategy if you have the requirement for consistent destination datasets with zero downtime and the `optimized` strategy does not work for you.
This strategy behaves the same way across all destinations.

### The `staging-optimized` strategy

The `staging-optimized` strategy has all the upsides of the `insert-from-staging` but implements certain optimizations for faster loading on some destinations. This comes at the cost of destination tables being dropped and recreated in some cases, which means that any views or other constraints you have placed on those tables will be dropped with the table. If you have a setup where you need to retain your destination tables, do not use the `staging-optimized` strategy. If you do not care about tables being dropped but need the upsides of the `insert-from-staging` with some performance (and cost) saving opportunities, you should use this strategy. The `staging-optimized` strategy behaves differently across destinations:

* Postgres: After loading the new data into the staging tables, the destination tables will be dropped and replaced by the staging tables. No data needs to be moved, so this strategy is almost as fast as `truncate-and-insert`.
* BigQuery: After loading the new data into the staging tables, the destination tables will be dropped and recreated with a [clone command](https://cloud.google.com/bigquery/docs/table-clones-create) from the staging tables. This is a low-cost and fast way to create a second independent table from the data of another. Learn more about [table cloning on BigQuery](https://cloud.google.com/bigquery/docs/table-clones-intro).
* Snowflake: After loading the new data into the staging tables, the destination tables will be dropped and recreated with a [clone command](https://docs.snowflake.com/en/sql-reference/sql/create-clone) from the staging tables. This is a low-cost and fast way to create a second independent table from the data of another. Learn more about [table cloning on Snowflake](https://docs.snowflake.com/en/user-guide/object-clone).

For all other destinations, please look at their respective documentation pages to see if and how the `staging-optimized` strategy is implemented. Destinations that do not implement it will not fall back to another strategy, as explained below.

### Which strategies your destination supports

Each destination declares the replace strategies it can use in its [capabilities](destination.md#inspect-destination-capabilities), and some destinations narrow that list per table. Inspect both for your own destination and table:

<!--@@@DLT_SNIPPET ./full-loading-snippets.py::check_replace_strategies-->

`supported_replace_strategies` lists everything the destination may use. `resolve_replace_strategy` applies the per-table narrowing and returns the strategy that will actually be used for that table, or `None` if the strategy you passed is not available for it. The table format is what narrows the list today: on the `filesystem` and `athena` destinations, tables with the `delta` or `iceberg` table format are always replaced with `insert-from-staging`, and all other tables only with `truncate-and-insert`.

If you request a strategy that is not available, dlt does not fall back to another one: the load step fails with `PipelineStepFailed` wrapping an `AssertionError` whose message names the table (`Must be able to get replace strategy for issues`), and no data is loaded. Pick a strategy from the supported list, or change the table format if that is what restricts the choice.

The destinations that ship with dlt declare the following strategies:

* `truncate-and-insert` only: [lance](../dlt-ecosystem/destinations/lance.md), [LanceDB](../dlt-ecosystem/destinations/lancedb.md), [Qdrant](../dlt-ecosystem/destinations/qdrant.md), and [Weaviate](../dlt-ecosystem/destinations/weaviate.md). Any other configured strategy fails on these.
* `truncate-and-insert` and `insert-from-staging`, but not `staging-optimized`: [Athena](../dlt-ecosystem/destinations/athena.md), [Dremio](../dlt-ecosystem/destinations/dremio.md), [DuckDB](../dlt-ecosystem/destinations/duckdb.md), [DuckLake](../dlt-ecosystem/destinations/ducklake.md), [Fabric](../dlt-ecosystem/destinations/fabric.md), [filesystem](../dlt-ecosystem/destinations/filesystem.md), [MotherDuck](../dlt-ecosystem/destinations/motherduck.md), [Redshift](../dlt-ecosystem/destinations/redshift.md), [SQLAlchemy](../dlt-ecosystem/destinations/sqlalchemy.md), and [Synapse](../dlt-ecosystem/destinations/synapse.md).
* All three strategies: [BigQuery](../dlt-ecosystem/destinations/bigquery.md), [ClickHouse](../dlt-ecosystem/destinations/clickhouse.md), [Databricks](../dlt-ecosystem/destinations/databricks.md), [MS SQL](../dlt-ecosystem/destinations/mssql.md), [Postgres](../dlt-ecosystem/destinations/postgres.md), and [Snowflake](../dlt-ecosystem/destinations/snowflake.md).

A destination may also reject a strategy for reasons it can only check against the live database. ClickHouse `staging-optimized`, for example, requires the Atomic or Shared database engine.
