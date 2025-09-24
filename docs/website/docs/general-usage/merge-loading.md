---
title: Merge loading
description: Merge loading with dlt
keywords: [merge, incremental loading, delete-insert, scd2, upsert]
---
# Merge loading

Merge loading allows you to update existing data in your destination tables, rather than [replacing all data](./full-loading.md). This approach is ideal when you want to update only specific records without replacing entire tables or to keep the history of data changes.

To perform a merge load, you need to specify the `write_disposition` as `merge` on your resource and provide a `primary_key` or `merge_key`.

Depending on your use case, you can choose from three different merge strategies.

## Merge strategies

1. [`delete-insert` (default strategy)](#delete-insert-strategy)
2. [`scd2` strategy](#scd2-strategy)
3. [`upsert` strategy](#upsert-strategy)

## `delete-insert` strategy

The default `delete-insert` strategy is used in two scenarios:

1. You want to keep only one instance of a certain record, i.e., you receive updates of the `user` state from an API and want to keep just one record per `user_id`.
2. You receive data in daily batches, and you want to make sure that you always keep just a single instance of a record for each batch, even in case you load an old batch or load the current batch several times a day (i.e., to receive "live" updates).

The `delete-insert` strategy loads data to a `staging` dataset, deduplicates the staging data if a `primary_key` is provided, deletes the data from the destination using `merge_key` and `primary_key`, and then inserts the new records. All of this happens in a single atomic transaction for a root and all nested tables.

Example below loads all the GitHub events and updates them in the destination using "id" as the primary key, making sure that only a single copy of the event is present in the `github_repo_events` table:

```py
@dlt.resource(primary_key="id", write_disposition="merge")
def github_repo_events():
    yield from _get_event_pages()
```

You can use compound primary keys:

```py
@dlt.resource(primary_key=("id", "url"), write_disposition="merge")
def resource():
    ...
```

Example below merges on a column `batch_day` that holds the day for which the given record is valid.
Merge keys also can be compound:

```py
@dlt.resource(merge_key="batch_day", write_disposition="merge")
def get_daily_batch(day):
    yield _get_batch_from_bucket(day)
```

As with any other write disposition, you can use it to load data ad hoc. Below we load issues with top reactions for the `duckdb` repo. The lists have, obviously, many overlapping issues, but we want to keep just one instance of each.

```py
p = dlt.pipeline(destination="bigquery", dataset_name="github")
issues = []
reactions = ["%2B1", "-1", "smile", "tada", "thinking_face", "heart", "rocket", "eyes"]
for reaction in reactions:
    for page_no in range(1, 3):
      page = requests.get(f"https://api.github.com/repos/{REPO_NAME}/issues?state=all&sort=reactions-{reaction}&per_page=100&page={page_no}", headers=headers)
      print(f"got page for {reaction} page {page_no}, requests left", page.headers["x-ratelimit-remaining"])
      issues.extend(page.json())
p.run(issues, write_disposition="merge", primary_key="id", table_name="issues")
```

Example below dispatches GitHub events to several tables by event type, keeps one copy of each event by "id" and skips loading of past records using "last value" incremental. As you can see, all of this we can just declare in our resource.

```py
@dlt.resource(primary_key="id", write_disposition="merge", table_name=lambda i: i['type'])
def github_repo_events(last_created_at = dlt.sources.incremental("created_at", "1970-01-01T00:00:00Z")):
    """A resource taking a stream of github events and dispatching them to tables named by event type. Deduplicates by 'id'. Loads incrementally by 'created_at' """
    yield from _get_rest_pages("events")
```

:::note
If you use the `merge` write disposition, but do not specify merge or primary keys, merge will fallback to `append`.
The appended data will be inserted from a staging table in one transaction for most destinations in this case.
:::

### Control deduplication of staging data

By default, `primary_key` deduplication is arbitrary. You can pass the `dedup_sort` column hint with a value of `desc` or `asc` to influence which record remains after deduplication. Using `desc`, the records sharing the same `primary_key` are sorted in descending order before deduplication, making sure the record with the highest value for the column with the `dedup_sort` hint remains. `asc` has the opposite behavior.

```py
@dlt.resource(
    primary_key="id",
    write_disposition="merge",
    columns={"created_at": {"dedup_sort": "desc"}}  # select "latest" record
)
def resource():
    ...
```

**Example: deduplication with timestamp based sorting**

```py
# Sample data
data = [
    {"id": 1, "metadata_modified": "2024-01-01", "value": "A"},
    {"id": 1, "metadata_modified": "2024-01-02", "value": "B"},
    {"id": 2, "metadata_modified": "2024-01-01", "value": "C"},
    {"id": 2, "metadata_modified": "2024-01-01", "value": "D"},  # Same metadata_modified as above
]

# Define the resource with dedup_sort configuration
@dlt.resource(
    primary_key='id',
    write_disposition='merge',
    columns={
        "metadata_modified": {"dedup_sort": "desc"}
    }
)
def sample_data():
    for item in data:
        yield item
```
Output:
| id  | metadata_modified | value |
|-----|------------------|-------|
|  1  | 2024-01-02       | B     |
|  2  | 2024-01-01       | C     |

When this resource is executed, the following deduplication rules are applied:

1. For records with different values in the `dedup_sort` column:
   - The record with the highest value is kept when using `desc`.
   - For example, between records with id=1, the one with `"metadata_modified"="2024-01-02"` is kept.

2. For records with identical values in the `dedup_sort` column:
   - The first occurrence encountered is kept.
   - For example, between records with id=2 and identical `"metadata_modified"="2024-01-01"`, the first record (value="C") is kept.

### Disable deduplication
If staging data is already deduplicated (or was always clean) you can disable it. Deduplication is preformed by the database backend so you
may save some costs:

```py
@dlt.resource(primary_key="id", write_disposition={"disposition": "merge", "strategy": "delete-insert", "deduplicated": True})
def github_repo_events():
    yield from _get_event_pages()
```

### Delete records
The `hard_delete` column hint can be used to delete records from the destination dataset. The behavior of the delete mechanism depends on the data type of the column marked with the hint:
1) `bool` type: only `True` leads to a delete—`None` and `False` values are disregarded.
2) Other types: each `not None` value leads to a delete.

Each record in the destination table with the same `primary_key` or `merge_key` as a record in the source dataset that's marked as a delete will be deleted.

Deletes are propagated to any nested table that might exist. For each record that gets deleted in the root table, all corresponding records in the nested table(s) will also be deleted. Records in parent and nested tables are linked through the `root key` that is explained in the next section.

#### Example: with primary key and boolean delete column
```py
@dlt.resource(
    primary_key="id",
    write_disposition="merge",
    columns={"deleted_flag": {"hard_delete": True}}
)
def resource():
    # This will insert a record (assuming a record with id = 1 does not yet exist).
    yield {"id": 1, "val": "foo", "deleted_flag": False}

    # This will update the record.
    yield {"id": 1, "val": "bar", "deleted_flag": None}

    # This will delete the record.
    yield {"id": 1, "val": "foo", "deleted_flag": True}

    # Similarly, this would have also deleted the record.
    # Only the key and the column marked with the "hard_delete" hint suffice to delete records.
    yield {"id": 1, "deleted_flag": True}
...
```

#### Example: with merge key and non-boolean delete column
```py
@dlt.resource(
    merge_key="id",
    write_disposition="merge",
    columns={"deleted_at_ts": {"hard_delete": True}})
def resource():
    # This will insert two records.
    yield [
        {"id": 1, "val": "foo", "deleted_at_ts": None},
        {"id": 1, "val": "bar", "deleted_at_ts": None}
    ]

    # This will delete two records.
    yield {"id": 1, "val": "foo", "deleted_at_ts": "2024-02-22T12:34:56Z"}
...
```

#### Example: with primary key and "dedup_sort" hint
```py
@dlt.resource(
    primary_key="id",
    write_disposition="merge",
    columns={"deleted_flag": {"hard_delete": True}, "lsn": {"dedup_sort": "desc"}})
def resource():
    # This will insert one record (the one with lsn = 3).
    yield [
        {"id": 1, "val": "foo", "lsn": 1, "deleted_flag": None},
        {"id": 1, "val": "baz", "lsn": 3, "deleted_flag": None},
        {"id": 1, "val": "bar", "lsn": 2, "deleted_flag": True}
    ]

    # This will insert nothing, because the "latest" record is a delete.
    yield [
        {"id": 2, "val": "foo", "lsn": 1, "deleted_flag": False},
        {"id": 2, "lsn": 2, "deleted_flag": True}
    ]
...
```

:::note
Indexing is important for doing lookups by column value, especially for merge writes, to ensure acceptable performance in some destinations.
:::

### Switch from append/replace to merge

:::tip
Root key propagation & merge apply only to nested tables. If your resource does not create nested tables you may ignore
this chapter.
:::

Merge write disposition requires that the `_dlt_id` (`row_key`) of the root table be propagated to nested tables. This concept is similar to a foreign key but always references the root (top level) table, skipping any intermediate parents. We call it `root key`. The root key is automatically propagated for all tables that have the `merge` write disposition set. We do not enable it elsewhere because it takes up storage space.

If you plan for some of resources to do merges but your initial backfill is append (or replace / full refresh) you should:
1. [Enable root key propagation right away](#forcing-root-key-propagation)
2. or, if you are sure that nested tables are max 1 level deep: [Explicitly disable root key propagation](#disable-root-key-propagation)

If you try to switch to merge after nested tables were already created you'll get a warning and NULL column violation from your
destination. You can fix your pipeline by:
1. Drop affected resources using `dlt pipeline ... drop` command or by using `refresh` argument on the pipeline. This will drop 
data from related resources and reset the schema so NOT NULL columns can be created.
2. If you have nested tables up to 1 nesting level you may [Explicitly disable root key propagation](#disable-root-key-propagation)
3. You can fix your nested tables in both staging and final datasets. Add `_dlt_root_id` to all nested tables and copy data
from related [root (top level) tables](../general-usage/schema.md#nested-references-root-and-nested-tables) `_dlt_id` (`row_key`).
In that case `dlt` will update pipeline schema but will skip database migration.

#### Forcing root key propagation

Nevertheless, in some cases, you may want to permanently enable root key propagation.

`Root key` propagation is automatically enabled for all tables that have the `merge` write disposition set from the beginning. We do not always enable it by default because it takes up additional storage space. Nevertheless, in some cases, you may want to permanently enable `root key` propagation.

To enable `root key` propagation on an existing source or resource, you must drop and recreate its tables, since the `_dlt_root_id` column cannot be added to tables that already contain data.

For example, suppose you used the [Facebook Ads](../dlt-ecosystem/verified-sources/facebook_ads.md) verified source, where the `merge` write disposition and `root key` are not enabled by default, to load the `ads` resource:
```py
pipeline = dlt.pipeline(
    pipeline_name='facebook_ads_pipeline',
    destination='duckdb',
    dataset_name='facebook_ads_data',
)
my_facebook_ads = facebook_ads_source()
pipeline.run(my_facebook_ads.with_resources("ads"))
```

If you want to change the `ads` resource to use `merge`, you must first drop the existing resource tables from the destination:

```sh
dlt pipeline facebook_ads_pipeline drop ads
```

This command removes the `ads` table and all its nested tables from the destination, allowing them to be later recreated with a schema that includes the `_dlt_root_id` column.

Next, enable `root key` propagation and run the pipeline once with `replace`, followed by `merge`:

```py
pipeline = dlt.pipeline(
    pipeline_name='facebook_ads_pipeline',
    destination='duckdb',
    dataset_name='facebook_ads_data',
)
my_facebook_ads = facebook_ads_source()

my_facebook_ads.root_key = True

pipeline.run(my_facebook_ads.with_resources("ads"), write_disposition="replace")

pipeline.run(my_facebook_ads.with_resources("ads"), write_disposition="merge")
```

In this example, enabling `my_facebook_ads.root_key = True` and running the pipeline once with `replace` ensures that the tables are recreated with the `_dlt_root_id` column. Once this column is present, subsequent `merge` runs can be executed successfully.

If you have defined your own source with the `@dlt.source` decorator, you can also enable `root key` propagation by adding `@dlt.source(root_key=True)`.

#### Disable root key propagation
If your source generates single level of nested table (nested tables do not have nested tables) ie. with `max_table_nesting=1` you can disable root key propagation
by setting `root_key` to `False` on the source level. In that case `dlt` will use `parent_key` which is identical to `root_key` for level 1 nested tables. Note that currently you cannot disable propagation on the resource level.

:::tip
If you switched from `append` to `merge` and you forgot to set `root_key` on your source, it is too late to set it to `True` if you already have data in
the destination. However, if you are sure that you do not have nested tables in nested tables (nesting level = 1), you can set it to `False` so the
existing `parent_key` will be used.
:::

## `scd2` strategy
`dlt` can create [Slowly Changing Dimension Type 2](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row) (SCD2) destination tables for dimension tables that change in the source. By default, the resource is expected to provide a full extract of the source table each run, but [incremental extracts](#example-incremental-scd2) are also possible. A row hash is stored in `_dlt_id` and used as surrogate key to identify source records that have been inserted, updated, or deleted. A `NULL` value is used by default to indicate an active record, but it's possible to use a configurable high timestamp (e.g. 9999-12-31 00:00:00.000000) instead.

:::note
The `unique` hint for `_dlt_id` in the root table is set to `false` when using `scd2`. This differs from [default behavior](./destination-tables.md#nested-tables). The reason is that the surrogate key stored in `_dlt_id` contains duplicates after an _insert-delete-reinsert_ pattern:
1. A record with surrogate key X is inserted in a load at `t1`.
2. The record with surrogate key X is deleted in a later load at `t2`.
3. The record with surrogate key X is reinserted in an even later load at `t3`.

After this pattern, the `scd2` table in the destination has two records for surrogate key X: one for the validity window `[t1, t2]`, and one for `[t3, NULL]`. A duplicate value exists in `_dlt_id` because both records have the same surrogate key.

Note that:
- The composite key `(_dlt_id, _dlt_valid_from)` is unique.
- `_dlt_id` remains unique for nested tables—`scd2` does not affect this.
:::

### Example: `scd2` merge strategy
```py
@dlt.resource(
    write_disposition={"disposition": "merge", "strategy": "scd2"}
)
def dim_customer():
    # initial load
    yield [
        {"customer_key": 1, "c1": "foo", "c2": 1},
        {"customer_key": 2, "c1": "bar", "c2": 2}
    ]

pipeline.run(dim_customer())  # first run — 2024-04-09 18:27:53.734235
...
```

*`dim_customer` destination table after first run—inserted two records present in initial load and added validity columns:*

| `_dlt_valid_from` | `_dlt_valid_to` | `customer_key` | `c1` | `c2` |
| -- | -- | -- | -- | -- |
| 2024-04-09 18:27:53.734235 | NULL | 1 | foo | 1 |
| 2024-04-09 18:27:53.734235 | NULL | 2 | bar | 2 |

```py
...
def dim_customer():
    # second load — record for customer_key 1 got updated
    yield [
        {"customer_key": 1, "c1": "foo_updated", "c2": 1},
        {"customer_key": 2, "c1": "bar", "c2": 2}
]

pipeline.run(dim_customer())  # second run — 2024-04-09 22:13:07.943703
```

*`dim_customer` destination table after second run—inserted new record for `customer_key` 1 and retired old record by updating `_dlt_valid_to`:*

| `_dlt_valid_from` | `_dlt_valid_to` | `customer_key` | `c1` | `c2` |
| -- | -- | -- | -- | -- |
| 2024-04-09 18:27:53.734235 | **2024-04-09 22:13:07.943703** | 1 | foo | 1 |
| 2024-04-09 18:27:53.734235 | NULL | 2 | bar | 2 |
| **2024-04-09 22:13:07.943703** | **NULL** | **1** | **foo_updated** | **1** |

```py
...
def dim_customer():
    # third load — record for customer_key 2 got deleted
    yield [
        {"customer_key": 1, "c1": "foo_updated", "c2": 1},
    ]

pipeline.run(dim_customer())  # third run — 2024-04-10 06:45:22.847403
```

*`dim_customer` destination table after third run—retired deleted record by updating `_dlt_valid_to`:*

| `_dlt_valid_from` | `_dlt_valid_to` | `customer_key` | `c1` | `c2` |
| -- | -- | -- | -- | -- |
| 2024-04-09 18:27:53.734235 | 2024-04-09 22:13:07.943703 | 1 | foo | 1 |
| 2024-04-09 18:27:53.734235 | **2024-04-10 06:45:22.847403** | 2 | bar | 2 |
| 2024-04-09 22:13:07.943703 | NULL | 1 | foo_updated | 1 |

### Example: incremental `scd2`
A `merge_key` can be provided to work with incremental extracts instead of full extracts. The `merge_key` lets you define which absent rows are considered "deleted". Compound natural keys are allowed and can be specified by providing a list of column names as `merge_key`.

*Case 1: do not retire absent records*

You can set the natural key as `merge_key` to prevent retirement of absent rows. In this case you don't consider any absent row deleted. Records are not retired in the destination if their corresponding natural keys are not present in the source extract. This allows for incremental extracts that only contain updated records.

```py
@dlt.resource(
    merge_key="customer_key",
    write_disposition={"disposition": "merge", "strategy": "scd2"}
)
def dim_customer():
    # initial load
    yield [
        {"customer_key": 1, "c1": "foo", "c2": 1},
        {"customer_key": 2, "c1": "bar", "c2": 2}
    ]

pipeline.run(dim_customer())  # first run — 2024-04-09 18:27:53.734235
...
```
*`dim_customer` destination table after first run:*

| `_dlt_valid_from` | `_dlt_valid_to` | `customer_key` | `c1` | `c2` |
| -- | -- | -- | -- | -- |
| 2024-04-09 18:27:53.734235 | NULL | 1 | foo | 1 |
| 2024-04-09 18:27:53.734235 | NULL | 2 | bar | 2 |

```py
...
def dim_customer():
    # second load — record for customer_key 1 got updated, customer_key 2 absent
    yield [
        {"customer_key": 1, "c1": "foo_updated", "c2": 1},
]

pipeline.run(dim_customer())  # second run — 2024-04-09 22:13:07.943703
```

*`dim_customer` destination table after second run—customer key 2 was not retired:*

| `_dlt_valid_from` | `_dlt_valid_to` | `customer_key` | `c1` | `c2` |
| -- | -- | -- | -- | -- |
| 2024-04-09 18:27:53.734235 | **2024-04-09 22:13:07.943703** | 1 | foo | 1 |
| 2024-04-09 18:27:53.734235 | NULL | 2 | bar | 2 |
| **2024-04-09 22:13:07.943703** | **NULL** | **1** | **foo_updated** | **1** |

:::tip
If you decide to undo the previous configuration that prevented retiring absent records for an existing pipeline,
and want to start retiring them again,
you must explicitly unset the `merge_key`:
```py
@dlt.resource(
    columns={"customer_key": {"merge_key": False}},
    write_disposition={"disposition": "merge", "strategy": "scd2"}
)
def dim_customer():
    ...
```
Simply omitting `merge_key` from the decorator will not disable the behavior. Aternatively, you can disable the `merge_key` hint for the affected column in the import schema.
:::

*Case 2: only retire records for given partitions*

:::note
Technically this is not SCD2 because the key used to merge records is not a natural key.
:::

You can set a "partition" column as `merge_key` to retire absent rows for given partitions. In this case you only consider absent rows deleted if their partition value is present in the extract. Physical partitioning of the table is not required—the word "partition" is used conceptually here.

```py
@dlt.resource(
    merge_key="date",
    write_disposition={"disposition": "merge", "strategy": "scd2"}
)
def some_data():
    # load 1 — "2024-01-01" partition
    yield [
        {"date": "2024-01-01", "name": "a"},
        {"date": "2024-01-01", "name": "b"},
    ]

pipeline.run(some_data())  # first run — 2024-01-02 03:03:35.854305
...
```

*`some_data` destination table after first run:*

| `_dlt_valid_from` | `_dlt_valid_to` | `date` | `name` |
| -- | -- | -- | -- |
| 2024-01-02 03:03:35.854305 | NULL | 2024-01-01 | a |
| 2024-01-02 03:03:35.854305 | NULL | 2024-01-01 | b |

```py
...
def some_data():
    # load 2 — "2024-01-02" partition
    yield [
        {"date": "2024-01-02", "name": "c"},
        {"date": "2024-01-02", "name": "d"},
    ]

pipeline.run(some_data())  # second run — 2024-01-03 03:01:11.943703
...
```

*`some_data` destination table after second run—added 2024-01-02 records, did not touch 2024-01-01 records:*

| `_dlt_valid_from` | `_dlt_valid_to` | `date` | `name` |
| -- | -- | -- | -- |
| 2024-01-02 03:03:35.854305 | NULL | 2024-01-01 | a |
| 2024-01-02 03:03:35.854305 | NULL | 2024-01-01 | b |
| **2024-01-03 03:01:11.943703** | **NULL** | **2024-01-02** | **c** |
| **2024-01-03 03:01:11.943703** | **NULL** | **2024-01-02** | **d** |

```py
...
def some_data():
    # load 3 — reload "2024-01-01" partition
    yield [
        {"date": "2024-01-01", "name": "a"},  # unchanged
        {"date": "2024-01-01", "name": "bb"},  # new
    ]

pipeline.run(some_data())  # third run — 2024-01-03 10:30:05.750356
...
```

*`some_data` destination table after third run—retired b, added bb, did not touch 2024-01-02 partition:*

| `_dlt_valid_from` | `_dlt_valid_to` | `date` | `name` |
| -- | -- | -- | -- |
| 2024-01-02 03:03:35.854305 | NULL | 2024-01-01 | a |
| 2024-01-02 03:03:35.854305 | **2024-01-03 10:30:05.750356** | 2024-01-01 | b |
| 2024-01-03 03:01:11.943703 | NULL | 2024-01-02 | c |
| 2024-01-03 03:01:11.943703 | NULL | 2024-01-02 | d |
| **2024-01-03 10:30:05.750356** | **NULL** | **2024-01-01** | **bb** |

### Handling nested structures with SCD type 2
To explore how SCD Type 2 handles nested JSON structures, refer to the hands-on demonstration provided in the Colab Notebook linked below.

Execute all steps directly in your browser:
[Open in Colab.](https://colab.research.google.com/drive/1GpG3JKGWveB-kR7eNvlJLr6oO0nM7Fbv?usp=sharing)


### Example: configure validity column names
`_dlt_valid_from` and `_dlt_valid_to` are used by default as validity column names. Other names can be configured as follows:
```py
@dlt.resource(
    write_disposition={
        "disposition": "merge",
        "strategy": "scd2",
        "validity_column_names": ["from", "to"],  # will use "from" and "to" instead of default values
    }
)
def dim_customer():
    ...
...
```

### Example: configure active record timestamp
You can configure the literal used to indicate an active record with `active_record_timestamp`. The default literal `NULL` is used if `active_record_timestamp` is omitted or set to `None`. Provide a date value if you prefer to use a high timestamp instead.
```py
@dlt.resource(
    write_disposition={
        "disposition": "merge",
        "strategy": "scd2",
        # accepts various types of date/datetime objects
        "active_record_timestamp": "9999-12-31",
    }
)
def dim_customer():
    ...
```

### Example: configure boundary timestamp
You can configure the "boundary timestamp" used for record validity windows with `boundary_timestamp`. The provided date(time) value is used as "valid from" for new records and as "valid to" for retired records. The timestamp at which a load package is created is used if `boundary_timestamp` is omitted.
```py
@dlt.resource(
    write_disposition={
        "disposition": "merge",
        "strategy": "scd2",
        # accepts various types of date/datetime objects
        "boundary_timestamp": "2024-08-21T12:15:00+00:00",
    }
)
def dim_customer():
    ...
```

### Example: Use your own row hash
By default, `dlt` generates a row hash based on all columns provided by the resource and stores it in `_dlt_id`. You can use your own hash instead by specifying `row_version_column_name` in the `write_disposition` dictionary. You might already have a column present in your resource that can naturally serve as a row hash, in which case it's more efficient to use those pre-existing hash values than to generate new artificial ones. This option also allows you to use hashes based on a subset of columns, in case you want to ignore changes in some of the columns. When using your own hash, values for `_dlt_id` are randomly generated.
```py
@dlt.resource(
    write_disposition={
        "disposition": "merge",
        "strategy": "scd2",
        "row_version_column_name": "row_hash",  # the column "row_hash" should be provided by the resource
    }
)
def dim_customer():
    ...
...
```

### 🧪 Use scd2 with Arrow tables and Panda frames
`dlt` will not add a **row hash** column to the tabular data automatically (we are working on it).
You need to do that yourself by adding a transform function to the `scd2` resource that computes row hashes (using pandas.util, should be fairly fast).
```py
import dlt
from dlt.sources.helpers.transform import add_row_hash_to_table

scd2_r = dlt.resource(
          arrow_table,
          name="tabular",
          write_disposition={
              "disposition": "merge",
              "strategy": "scd2",
              "row_version_column_name": "row_hash",
          },
      ).add_map(add_row_hash_to_table("row_hash"))
```
`add_row_hash_to_table` is the name of the transform function that will compute and create the `row_hash` column that is declared as holding the hash by `row_version_column_name`.

:::tip
You can modify existing resources that yield data in tabular form by calling `apply_hints` and passing the `scd2` config in `write_disposition` and then by
adding the transform with `add_map`.
:::

### Nested tables
Nested tables, if any, do not contain validity columns. Validity columns are only added to the root table. Validity column values for records in nested tables can be obtained by joining the root table using `_dlt_root_id` (`root_key`).

### Limitations

* You cannot use columns like `updated_at` or integer `version` of a record that are unique within a `primary_key` (even if it is defined). The hash column
must be unique for a root table. We are working to allow `updated_at` style tracking.
* We do not detect changes in nested tables (except new records) if the row hash of the corresponding parent row does not change. Use `updated_at` or a similar
column in the root table to stamp changes in nested data.

## `upsert` strategy

:::warning
The `upsert` merge strategy is currently supported for these destinations:
- `athena`
- `bigquery`
- `databricks`
- `mssql`
- `postgres`
- `snowflake`
- `filesystem` with `delta` table format (see limitations [here](../dlt-ecosystem/destinations/delta-iceberg#known-limitations)) and `iceberg` table format
:::

The `upsert` merge strategy does primary-key based *upserts*:
- *update* a record if the key exists in the target table
- *insert* a record if the key does not exist in the target table

You can [delete records](#delete-records) with the `hard_delete` hint.

### `upsert` versus `delete-insert`

Unlike the default `delete-insert` merge strategy, the `upsert` strategy:
1. needs a `primary_key`
2. expects this `primary_key` to be unique (`dlt` does not deduplicate)
3. does not support `merge_key`
4. uses `MERGE` or `UPDATE` operations to process updates

### Example: `upsert` merge strategy
```py
@dlt.resource(
    write_disposition={"disposition": "merge", "strategy": "upsert"},
    primary_key="my_primary_key"
)
def my_upsert_resource():
    ...
...
```