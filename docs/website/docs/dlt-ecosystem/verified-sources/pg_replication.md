---
title: Postgres replication
description: Replicate Postgres tables with logical replication (CDC), from the initial snapshot to the changes made after it
keywords: [postgres, postgres replication, logical replication, database replication, cdc, change data capture]
---
# Postgres replication

import Header from './_source-info-header.md';

<Header/>

[Postgres](https://www.postgresql.org/) is one of the most popular relational database management systems. This verified source replicates Postgres tables into any dlt destination. It uses [logical replication](https://www.postgresql.org/docs/current/logicaldecoding.html) and the built-in `pgoutput` plugin. This process is also called change data capture (CDC).

dlt loads the rows that exist when the replication starts (the **initial load**). Then dlt loads every insert, update and delete committed after that. It does not miss or repeat a change.

dlt runs the source as a regular pipeline, for example on a schedule. Each run loads the changes committed since the previous run and then stops.

Both the initial load and the replication can produce Arrow tables instead of Python rows. With `backend="pyarrow"`, dlt decodes inserts with `numpy` and writes them as parquet. This backend is about 5 times faster than the default `python` backend, so it is better for large tables. See [Backends](#backends).

| Name                      | Description                                                                                     |
| ------------------------- | ----------------------------------------------------------------------------------------------- |
| `init_replication`        | Creates the publication and the replication slot, and can return resources for the initial load |
| `replication_resource`    | Loads the changes from the replication slot                                                     |
| `replication_slot_exists` | Tells if the replication slot exists, so the initial load runs only once                        |

## Prerequisites

### Postgres server

Enable logical replication on the server. This setting takes effect after a restart:

```sql
ALTER SYSTEM SET wal_level = logical;
```

The server also needs a free [replication slot](https://www.postgresql.org/docs/current/runtime-config-replication.html) (`max_replication_slots`) and a free WAL sender (`max_wal_senders`) for each replicated pipeline.

| Requirement     |                                                                                                        |
| --------------- | ------------------------------------------------------------------------------------------------------ |
| Postgres        | 10 or higher                                                                                           |
| dlt             | 1.22.0 or higher                                                                                       |
| Python packages | `psycopg2-binary`, `sqlalchemy` 2.0 or higher. The `pyarrow` backend also needs `pyarrow` and `numpy`. |

Replicating a whole schema (without `table_names`) needs Postgres 15 or higher. Postgres 13 and higher can publish partitioned tables. On older versions, publish their partitions instead. Logical decoding on a standby needs Postgres 16.

Managed services (Amazon RDS and Aurora, Google Cloud SQL, Azure, Supabase, Neon) support logical replication with `pgoutput`. On some of these services, you must enable it in the settings. On Amazon RDS:

1. Set `rds.logical_replication = 1` in the [parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PostgreSQL.Replication.ReadReplicas.html) of the instance.
2. `CREATE ROLE ... REPLICATION` does not work on RDS. Grant `rds_replication` to the replication user instead:

    ```sql
    GRANT rds_replication TO replication_user;
    ```

3. Enforce SSL in the credentials:

    ```toml
    sources.pg_replication.credentials="postgresql://replication_user:password@host.rds.amazonaws.com:5432/dlt_data?sslmode=require&connect_timeout=300"
    ```

### User and publication

The pipeline user needs the `REPLICATION` attribute and `SELECT` on the replicated tables. A [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) publishes the changes. Only the owner of the tables or a superuser can create a publication. The simplest setup is a publication that the table owner creates:

```sql
CREATE ROLE replication_user WITH LOGIN REPLICATION PASSWORD 'password';
GRANT USAGE ON SCHEMA public TO replication_user;
GRANT SELECT ON public.orders, public.customers TO replication_user;

-- run as the owner of the tables or a superuser
CREATE PUBLICATION my_pub FOR TABLE public.orders, public.customers;
```

If a publication already publishes all tables, `init_replication` uses it without changes. If the publication is missing or does not publish all tables, `init_replication` creates or extends it. This needs the `CREATE` privilege on the database and the ownership of the tables. A whole schema needs a superuser. Without these privileges, `init_replication` raises `PublicationPrivilegesException` with the statement for the owner, for example:

```sql
CREATE PUBLICATION "my_pub" FOR TABLE "public"."orders", "public"."customers" WITH (publish = 'insert, update, delete');
```

The `initial_snapshot` modes that copy the tables also need `CREATE` on the schema, see [Initial load](#initial-load). [Row-level security](https://www.postgresql.org/docs/current/ddl-rowsecurity.html) filters what a user without `BYPASSRLS` reads in an initial load, but not the replicated changes. Such an initial load fails, so the destination never gets only some of the rows.

### Tables

* dlt merges updates and deletes on the primary key, or on the columns of the [replica identity](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY) index. A table without either of them needs `publish="insert"`. dlt appends its changes.
* Set `REPLICA IDENTITY FULL` on tables with large text, JSON, array or binary values that get updates, see [Unchanged TOASTed values](#unchanged-toasted-values). Also set it on tables whose deleted rows you keep with their last values, see [Handling deletes](#handling-deletes):

    ```sql
    ALTER TABLE public.orders REPLICA IDENTITY FULL;
    ```

### Initialize the verified source

1. Run the following command:

    ```cli
    dlt init pg_replication duckdb
    ```

    It adds the source and a [pipeline example](#check-the-example-pipeline) with [DuckDB](../destinations/duckdb.md) as the destination. Replace `duckdb` with any other [destination](../destinations/index.md).

2. Install the dependencies:

    ```cli
    pip install -r requirements.txt
    ```

3. Add the credentials of the replication user to `.dlt/secrets.toml`:

    ```toml
    [sources.pg_replication]
    credentials="postgresql://replication_user:password@localhost:5432/dlt_data"
    ```

    Add the credentials of your destination as described in its [documentation](../destinations/index.md). Read more about [credentials](../../general-usage/credentials/index.md).

### Check the example pipeline

The [pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/pg_replication_pipeline.py) added by `dlt init` shows how to use the source. Its demos create the Postgres tables and make the changes themselves, in a Postgres that you can write to and that has logical replication enabled. To replicate your own tables, read [Setting up the pipeline](#setting-up-the-pipeline).

| Function                                   | Demonstrates                                                                                           |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------ |
| `replicate_single_table_with_initial_load` | Initial load of your tables on the first run and the replication of their later changes on every run   |
| `replicate_single_table_demo`              | Insert, update and delete of rows, each loaded by a replication run                                    |
| `replicate_with_initial_load_demo`         | Rows present before the replication starts come from the initial load, later rows from the replication |
| `replicate_added_table_demo`               | Initial load of a table added to the replication later                                                 |
| `replicate_entire_schema_demo`             | Replication of all tables of a schema, including a table created later (Postgres 15+, superuser)       |
| `replicate_with_column_selection_demo`     | Loading only some columns of a table with `include_columns`                                            |

## Setting up the pipeline

The replication has two steps, both run with the same pipeline:

1. **Initial load**: `init_replication` creates the publication and the replication slot and returns resources that load the tables as they are at the moment the slot is created. You run it once, when the slot does not exist yet.
2. **Replication**: `replication_resource` loads the changes committed after the slot was created. You run it on a schedule.

Postgres exports a snapshot at the moment it creates the replication slot. This exported snapshot contains every transaction committed before this moment. The slot contains every transaction committed after it. The two steps do not miss or repeat a change, also when other sessions write to the tables during the setup. You do not need to lock the tables.

### Initial load

1. Create the publication and the replication slot, and get the resources for the initial load:

    ```py notype
    import dlt
    from pg_replication import init_replication

    snapshot = init_replication(
        slot_name="my_slot",
        pub_name="my_pub",
        schema_name="public",
        table_names=["orders", "customers"],
        initial_snapshot="direct",
        backend="pyarrow",
    )
    ```

    `init_replication` uses or creates the publication `my_pub` with the two tables. It creates the replication slot `my_slot`. It returns one [`sql_table`](sql_database/index.md) resource per table, which reads the table in the exported snapshot of the slot.

2. Run the initial load in the same process:

    ```py notype
    pipeline = dlt.pipeline(
        pipeline_name="pg_replication",
        destination="duckdb",
        dataset_name="replicated",
    )
    pipeline.run(snapshot)
    ```

    The tables get the `merge` write disposition and the primary key of the Postgres tables. A publication of inserts only (`publish="insert"`) uses `append`.

3. Run the replication for the first time, see [Replication](#replication). It loads the changes made during the initial load.

The exported snapshot of the slot exists while the connection of `init_replication` is open. The connection closes after the last table is read, or when Python garbage collects the returned object. You must run the initial load right after `init_replication`. The `initial_snapshot` argument chooses how the initial load reads the tables:

| `initial_snapshot` | How the initial load reads the tables                                                                                | When to use                                                                     |
| ------------------ | -------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| `"direct"`         | Not copied, the tables are read in the exported snapshot of the slot                                                 | Load in the same process, without copies and without `CREATE` on the schema     |
| `"table"`          | Copied into tables `_dlt_<table>_s_<snapshot>` in the schema of the replicated tables                                | Load later or in another process                                                |
| `"unlogged_table"` | Copied into [unlogged](https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-UNLOGGED) tables | Large tables: the copy is not written to the WAL, but a server crash empties it |
| not set            | No initial load                                                                                                      | You only need the changes made from now on                                      |

With copies, `init_replication` returns resources that read the copies at any time later:

```py notype
snapshot = init_replication(
    slot_name="my_slot",
    pub_name="my_pub",
    schema_name="public",
    table_names=["orders", "customers"],
    initial_snapshot="table",
)
pipeline.run(snapshot)
```

dlt does not drop the copies. After the initial load, drop the `_dlt_<table>_s_<snapshot>` tables.

:::caution
Run the initial load **before** the first run of `replication_resource`. An initial load after the replicated changes overwrites those rows with older values. Postgres exports the snapshot only when it creates the slot. This happens on the first call of `init_replication`, or with `reset=True`. `reset=True` drops the slot and creates it again.
:::

A pipeline script that runs on a schedule must run the initial load only once. Check the slot with `replication_slot_exists` before you call `init_replication`:

```py notype
from pg_replication import replication_slot_exists

if not replication_slot_exists("my_slot"):
    snapshot = init_replication(
        slot_name="my_slot",
        pub_name="my_pub",
        schema_name="public",
        table_names=["orders", "customers"],
        initial_snapshot="direct",
    )
    pipeline.run(snapshot)
```

`init_replication` does not check which tables your destination already has. On an existing slot, it loads every table that you pass, as the table is at the time of the read. See [Tables added later](#tables-added-later).

#### Initial load with your own `sql_table`

A direct initial load reads the tables with `sql_table`. To configure how each table is read (query, chunk size, backend, hints), create your own `sql_table` resources in a `with` block. Pass them the callbacks of the returned object:

```py notype
import dlt
from dlt.sources.sql_database import sql_table
from pg_replication import init_replication

credentials = dlt.secrets["sources.pg_replication.credentials"]


def recent_orders(query, table):
    return query.where(table.c.created_at >= "2025-01-01")


with init_replication(
    slot_name="my_slot",
    pub_name="my_pub",
    schema_name="public",
    table_names=["orders", "customers"],
    initial_snapshot="direct",
) as snapshot:
    orders = sql_table(
        credentials,
        table="orders",
        schema="public",
        backend="pyarrow",
        query_adapter_callback=recent_orders,
        engine_adapter_callback=snapshot.engine_adapter_callback,
        type_adapter_callback=snapshot.type_adapter_callback,
        table_adapter_callback=snapshot.table_adapter_callback,
    )
    orders.apply_hints(write_disposition="merge", primary_key="id")
    pipeline.run(orders)
```

`engine_adapter_callback` makes every query read the exported snapshot. `type_adapter_callback` and `table_adapter_callback` give the columns the same types and nullability as replicated columns. The same callbacks work with `sql_database`. Use the same backend as in `replication_resource`, see [Backends](#backends).

An initial load with a query filter, like `recent_orders` above, loads only the selected rows. The replication still loads every later change. If a row that the query did not select gets an update, dlt loads the complete row.

#### Initial load by another pipeline

`replication_resource` keeps the types of the columns loaded before, see [Schema changes](#schema-changes). It finds them in the schema of its own pipeline. If another pipeline ran the initial load into the same dataset, pass that pipeline as `snapshot_pipeline` to the first replication run:

```py notype
pipeline.run(replication_resource("my_slot", "my_pub", snapshot_pipeline=snapshot_pipeline))
```

If the working directory of that pipeline is gone, restore its schema with `snapshot_pipeline.sync_destination()` first.

#### Tables added later

Call `init_replication` again with the added tables and `initial_snapshot`. On an existing slot, `init_replication` adds the tables to the publication if they are missing. It returns resources that load the tables as they are at the time of the read. Postgres exports a snapshot only when it creates the slot.

The next replication run replays the changes that the slot holds for these tables. Some of these changes are already in the loaded rows. The merge makes the result consistent:

```py notype
added = init_replication(
    slot_name="my_slot",
    pub_name="my_pub",
    schema_name="public",
    table_names="invoices",
    initial_snapshot="direct",
)
pipeline.run(added)
pipeline.run(replication_resource("my_slot", "my_pub"))
```

`init_replication` loads all tables in `table_names`, so pass only the added tables. If the initial load of a table fails, call `init_replication` again for this table.

As with the initial load, run these resources before the next replication run. This needs version 1.0.2 of the source and a publication of updates or deletes. With inserts only, the tables use `append`, which loads the overlap twice. For this reason, `init_replication` raises `ValueError`. Without `initial_snapshot`, `init_replication` adds the tables but does not load their existing rows.

#### Tables removed

`init_replication` never removes tables from the publication. To stop replicating a table, drop it from the publication as the owner of the publication:

```sql
ALTER PUBLICATION my_pub DROP TABLE public.invoices;
```

dlt does not load its later changes. Its destination table keeps the rows loaded so far.

### Replication

Run `replication_resource` with the same pipeline on a schedule, for example every few minutes:

```py notype
import dlt
from pg_replication import replication_resource

pipeline = dlt.pipeline(
    pipeline_name="pg_replication",
    destination="duckdb",
    dataset_name="replicated",
)
pipeline.run(replication_resource("my_slot", "my_pub", backend="pyarrow"))
```

Each run opens one replication connection, loads the changes committed before the run started and stops. It does not wait for new changes, so the latency of the replication is the schedule interval plus the duration of a run.

* **Progress**: dlt stores the position of the last loaded transaction in the pipeline state. At the start of the next run, dlt confirms the changes up to that position. The slot then releases them. The position moves only after the run that loaded the changes succeeds. A run that fails, or loses its working directory before the load, loses nothing: the next run loads its changes again.
* **Batches**: dlt yields changes in batches of about `target_batch_size` changes. It never splits a transaction. To limit the duration of a run on a long backlog, use `add_limit`. The next run continues where the previous run stopped:

    ```py notype
    pipeline.run(replication_resource("my_slot", "my_pub").add_limit(max_time=3600))
    ```

* **Destination tables**: dlt merges changes on the primary key. dlt supports compound keys. The `_dlt_lsn` column holds the position of the change. It orders the changes of a row within a load. Rows of the initial load have no position.
* **Deletes**: dlt sets `_dlt_deleted_ts` to the commit time of the delete and deletes the row, see [Handling deletes](#handling-deletes). A primary key update deletes the row under the old key.
* **New columns**: dlt adds them to the destination tables, also in the middle of a run, see [Schema changes](#schema-changes).

:::caution
Drop the slots that you do not use with `SELECT pg_drop_replication_slot('my_slot');`. A slot keeps all changes until the pipeline confirms them. If the pipeline does not run, Postgres keeps the WAL for the slot. The WAL can fill the disk of the server.
:::

## Full code example

<details>

<summary>Show full code example</summary>

```py notype
import dlt
from pg_replication import init_replication, replication_resource, replication_slot_exists

SLOT, PUB = "my_slot", "my_pub"


def get_pipeline() -> dlt.Pipeline:
    # the same pipeline and dataset for the snapshot and the replication
    return dlt.pipeline(
        pipeline_name="pg_replication",
        destination="duckdb",
        dataset_name="replicated",
    )


def initial_load() -> None:
    """Creates the publication and the slot and loads the snapshot, run once."""
    snapshot = init_replication(
        slot_name=SLOT,
        pub_name=PUB,
        schema_name="public",
        table_names=["orders", "customers"],
        initial_snapshot="direct",
        backend="pyarrow",
    )
    pipeline = get_pipeline()
    print(pipeline.run(snapshot))


def replicate() -> None:
    """Loads the changes committed since the previous run, run on a schedule."""
    pipeline = get_pipeline()
    print(pipeline.run(replication_resource(SLOT, PUB, backend="pyarrow")))


if __name__ == "__main__":
    # Postgres exports the snapshot only when it creates the slot
    if not replication_slot_exists(SLOT):
        initial_load()
    replicate()
```

</details>

## Handling deletes

By default, dlt deletes the rows that Postgres deletes. With `hard_delete=False`, dlt uses soft deletes instead. A soft delete keeps the row in the destination and marks it as deleted. dlt sets `_dlt_deleted_ts` of the row to the commit time of the delete:

```py notype
pipeline.run(replication_resource("my_slot", "my_pub", hard_delete=False))
```

A delete sends the whole old row only when the table has `REPLICA IDENTITY FULL`. Otherwise, it sends only the primary key or the columns of the replica identity index. The kept row then gets `NULL` in all other columns. A `NOT NULL` destination column rejects this `NULL`, see [NOT NULL columns](#not-null-columns). With soft deletes, a primary key update keeps the row under the old key and marks it as deleted.

## Schema changes

Postgres sends the structure of a table with its first change after the structure changed. `replication_resource` adds new columns and checks that changed columns still fit the destination.

### Data types

The initial load and the replicated changes give the columns the same types:

| Postgres                                                                | dlt                           |
| ----------------------------------------------------------------------- | ----------------------------- |
| `smallint`, `integer`, `bigint`                                         | `bigint`                      |
| `real`, `double precision`                                              | `double`                      |
| `numeric(p,s)`                                                          | `decimal(p,s)`                |
| `boolean`                                                               | `bool`                        |
| `date`                                                                  | `date`                        |
| `time`, `time with time zone`                                           | `time`                        |
| `timestamp`                                                             | `timestamp` without time zone |
| `timestamp with time zone`                                              | `timestamp` with time zone    |
| `json`, `jsonb`, arrays                                                 | `json`                        |
| `bytea`                                                                 | `binary`                      |
| `text`, `varchar(n)`, `char(n)`, `uuid`, enums, `interval`, other types | `text`                        |

Domains get the type of their base type. dlt loads `infinity` and `-infinity` dates and timestamps as the maximum and minimum values.

New tables and columns get no length (`varchar(n)` becomes `text`) and no integer size, so later changes of the Postgres columns fit. To keep them, pass `reflection_level="full_with_precision"` to both `init_replication` and `replication_resource`. `numeric(p,s)` keeps its precision and scale on both levels.

`numeric` columns without precision get dlt's default `decimal` precision and scale (38, 9). Values with more decimals fail with the `pyarrow` backend and are cut to 9 decimals by the `python` backend. Set precision and scale in both `init_replication` and `replication_resource`:

```py notype
columns = {"orders": {"amount": {"data_type": "decimal", "precision": 38, "scale": 15}}}
snapshot = init_replication(..., columns=columns, initial_snapshot="direct")
pipeline.run(replication_resource("my_slot", "my_pub", columns=columns))
```

### New, renamed and dropped columns

dlt adds new columns. dlt adds a renamed column under its new name. The old column stays and gets `NULL` in new and updated rows. A dropped column also stays and gets `NULL`.

### Type changes

dlt never changes the type of an existing destination column. `replication_resource` keeps the types of the columns loaded before and checks that every value of the new Postgres type fits:

| Destination column                            | Fits when the new Postgres type is                                                     |
| --------------------------------------------- | -------------------------------------------------------------------------------------- |
| `bigint`                                      | an integer type that is not larger                                                     |
| `decimal(P,S)`                                | `numeric(p,s)` with `s <= S` and `p - s <= P - S`                                      |
| `text`                                        | any type loaded as `text`, with a length only a `varchar` or `char` that is not longer |
| `double`                                      | `real`, `double precision`                                                             |
| `json`                                        | `json`, `jsonb`, arrays                                                                |
| `timestamp`, `date`, `time`, `bool`, `binary` | the same type, with the same time zone                                                 |

If a type change does not fit, the replication stops with `IncompatibleColumnTypeException` before it loads the change. An example is `integer` changed to `numeric(12,2)`: the `bigint` destination column cannot keep the decimals. The change stays in the slot. To continue, do one of these steps:

* Change the type of the destination column yourself. Then drop the table from the staging dataset (`<dataset>_staging`). Merge loads use this dataset first, and dlt creates the table again. Then pass the `columns` from the error message to `replication_resource`, for example `columns={"orders": {"amount": {"data_type": "decimal", "precision": 12, "scale": 2}}}`. dlt does not check the types in `columns`.
* Stop replicating the column with `exclude_columns`.
* Load the table again, see [Full refresh](#full-refresh).

### NOT NULL columns

New tables and columns get `NOT NULL` only on the primary key, so a renamed, dropped or excluded column can get `NULL`. To keep `NOT NULL` of the Postgres columns in new tables, pass `reflect_not_null=True` to both `init_replication` and `replication_resource`.

If a change has `NULL` for a `NOT NULL` destination column, the replication stops with `NullInNotNullColumnException`. This happens, for example, after a rename or `DROP NOT NULL` in Postgres. To continue, drop `NOT NULL` of the destination column yourself. Then pass `nullable: True` for this column in `columns`.

### Generated columns

Postgres does not replicate the values of [generated columns](https://www.postgresql.org/docs/current/ddl-generated-columns.html) before Postgres 18, so `init_replication` leaves them out of the initial load. Postgres 18 replicates stored generated columns of publications created with `publish_generated_columns = stored`.

### Table and column names

The [naming convention](../../general-usage/naming-convention.md) of the destination normalizes table and column names. Two tables or two columns can normalize to the same name, like `Items` and `items`. Tables with the same name in two schemas of the publication also collide. Then `init_replication` and `replication_resource` stop with `TableNameCollisionException` or `ColumnNameCollisionException`. A case-sensitive naming convention like `sql_cs_v1` keeps names that differ in case apart. Replicate tables of different schemas with separate publications and slots, into separate datasets.

## When replication stops

If the replication cannot load a change, it stops before this change. The change stays in the slot and the next run stops at the same place until you resolve it. The error tells what to do:

| Error                                                         | Cause                                                                                |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `IncompatibleColumnTypeException`                             | A column type changed, see [Type changes](#type-changes)                             |
| `NullInNotNullColumnException`                                | `NULL` in a `NOT NULL` destination column, see [NOT NULL columns](#not-null-columns) |
| `UnchangedToastException`                                     | See [Unchanged TOASTed values](#unchanged-toasted-values)                            |
| `ValueConversionException`                                    | A value that dlt cannot load, for example a date before year 1                       |
| `NoPrimaryKeyException`                                       | A table without a key in a publication of updates or deletes, see [Tables](#tables)  |
| `TableNameCollisionException`, `ColumnNameCollisionException` | See [Table and column names](#table-and-column-names)                                |

### Skip a change

Errors caused by a change name the transaction of the change and the statement that skips it, for example:

```sql
SELECT pg_replication_slot_advance('my_slot', '0/75A1F8A1');
```

Make sure that no replication runs. Then run the statement (Postgres 11+).

The slot skips every transaction committed before that position. This also skips all changes of the published tables that were not loaded yet. These are all changes since the last successful run, and the destination does not get them. Unlike a Postgres subscriber, a slot cannot skip a single transaction. The next run logs that the slot is past the stored position and continues.

Errors about changed columns, like type changes, cannot be skipped, because every later change has the same columns.

### Unchanged TOASTed values

Postgres stores large values (more than about 2 KB, such as long texts, JSON documents or arrays) [out of line](https://www.postgresql.org/docs/current/storage-toast.html). An update that does not change such a value does not send it in the new row. With `REPLICA IDENTITY FULL`, the update also sends the old row, and dlt takes the value from there.

Without `REPLICA IDENTITY FULL`, such an update stops the replication with `UnchangedToastException`. If dlt loads such an update, the value in the destination changes to `NULL`. `init_replication` warns about published tables at risk. Before these tables get updates, set `REPLICA IDENTITY FULL` on them:

```sql
ALTER TABLE public.orders REPLICA IDENTITY FULL;
```

If you do not need the large columns, exclude them in both `init_replication` and `replication_resource` instead:

```py notype
exclude_columns = {"orders": ["notes"]}
snapshot = init_replication(..., exclude_columns=exclude_columns, initial_snapshot="direct")
pipeline.run(snapshot)
pipeline.run(replication_resource("my_slot", "my_pub", exclude_columns=exclude_columns))
```

`include_columns` works the same way: columns that are not listed are not loaded.

## Full refresh

:::warning
If you run a full refresh, dlt drops the replicated tables in the destination. Then dlt loads them again with a new initial load.
:::

Drop the replicated tables and the replication state. The replication resource has the name of the slot:

```cli
dlt pipeline pg_replication drop my_slot
```

Then create the slot again and run a new initial load as in [Initial load](#initial-load):

```py notype
snapshot = init_replication(
    slot_name="my_slot",
    pub_name="my_pub",
    schema_name="public",
    table_names=["orders", "customers", "invoices"],
    initial_snapshot="direct",
    reset=True,
)
pipeline.run(snapshot)
```

`reset=True` drops the slot and the publication and creates them again. A publication of another role is kept.

## Backends

Both `init_replication` and `replication_resource` take the `backend` argument:

| `backend`            | Loads                                                          | Requires           |
| -------------------- | -------------------------------------------------------------- | ------------------ |
| `"python"` (default) | rows as Python dicts                                           |                    |
| `"pyarrow"`          | Arrow tables, inserts decoded with `numpy`, written as parquet | `pyarrow`, `numpy` |

Both backends load the same values and column types. The table shows the throughput of extract and normalize, from a local Postgres 17 into the `filesystem` destination. Each value is the median of three runs:

| Changes                            | `python`      | `pyarrow`      |
| ---------------------------------- | ------------- | -------------- |
| 500,000 inserts                    | 33,000 rows/s | 187,000 rows/s |
| 250,000 updates and 50,000 deletes | 32,000 rows/s | 156,000 rows/s |

Both backends used about 250 MB of memory. The `python` backend uses more than half of its time to normalize rows. The `pyarrow` backend writes the Arrow tables to parquet without changes.

:::caution
Use the same backend for the initial load and the replication. Arrow tables do not get the `_dlt_load_id` and `_dlt_id` columns. Rows of the `python` backend add them as `NOT NULL` columns. As a result, changes of one backend fail to load into a table loaded with the other. This also applies to your own `sql_table` for a direct initial load: use an Arrow backend (`pyarrow`, `pandas`, `connectorx`) only with `replication_resource(backend="pyarrow")`. To mix backends, set `add_dlt_load_id` and `add_dlt_id` of the [parquet normalizer](arrow-pandas.md) for all loads.
:::

## Limitations and troubleshooting

### Not replicated

* `TRUNCATE`: publications that `init_replication` creates do not publish it. A publication that you create without the `publish` option publishes it. The replication ignores it and logs a warning.
* Defaults of added columns: Postgres writes no changes when it fills the default into existing rows. These rows keep `NULL` in the destination until they change.
* Generated columns before Postgres 18, see [Generated columns](#generated-columns).
* dlt loads a renamed table into a new table, with only its changed rows. A dropped and re-created table leaves a publication of listed tables.
* Partitions are replicated under their own names, because the publication is created without `publish_via_partition_root`.
* The initial load ignores the [row filters](https://www.postgresql.org/docs/current/logical-replication-row-filter.html) and [column lists](https://www.postgresql.org/docs/current/logical-replication-col-lists.html) of the publication (Postgres 15+).
* Sequences, DDL and large objects, see the [Postgres restrictions](https://www.postgresql.org/docs/current/logical-replication-restrictions.html).

### Slots and WAL

* `max_slot_wal_keep_size` (Postgres 13+) and `idle_replication_slot_timeout` (Postgres 18) invalidate a slot that falls behind or stays idle, also one idle between scheduled runs. An invalidated slot cannot be read: load the tables again, see [Full refresh](#full-refresh).
* Postgres creates a slot only after all running transactions end. `init_replication` hangs while a session is idle in a transaction.
* While dlt reads a direct initial load, vacuum cannot remove rows that were deleted or updated after the exported snapshot. This applies to the whole database.
* `replication slot "my_slot" is active for PID ...`: another run reads the slot. A run waits for the slot to be free for a while, then fails. Do not run two pipelines on one slot.

### Values

* The `pyarrow` backend loads `numeric` `NaN` as `NULL`. `Infinity` and `-Infinity` fail this backend. The `python` backend passes all three values to the destination.
* dlt reads values with pinned session settings (`datestyle`, `intervalstyle`, `extra_float_digits`, `timezone`, `bytea_output`). Role and database defaults do not change them.

## Reference

### `init_replication`

```py notype
def init_replication(
    slot_name: str = dlt.config.value,
    pub_name: str = dlt.config.value,
    schema_name: str = dlt.config.value,
    table_names: Optional[Union[str, Sequence[str]]] = dlt.config.value,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    publish: str = "insert, update, delete",
    persist_snapshots: bool = False,  # deprecated, same as initial_snapshot="table"
    include_columns: Optional[Mapping[str, Sequence[str]]] = None,
    exclude_columns: Optional[Mapping[str, Sequence[str]]] = None,
    columns: Optional[Mapping[str, TTableSchemaColumns]] = None,
    reset: bool = False,
    backend: Literal["python", "pyarrow"] = "python",
    initial_snapshot: Optional[Literal["table", "unlogged_table", "direct"]] = None,
    reflection_level: Literal["full", "full_with_precision"] = "full",
    reflect_not_null: bool = False,
) -> Optional[Union[DltResource, List[DltResource]]]: ...
```

| Configuration                                     | Minimum privileges                                                  |
| ------------------------------------------------- | ------------------------------------------------------------------- |
| Publication created by the table owner            | `REPLICATION`, `SELECT` on the tables                               |
| Creates or extends a publication of listed tables | `REPLICATION`, `CREATE` on the database, ownership of the tables    |
| `table_names` not set                             | a superuser, or a publication `FOR TABLES IN SCHEMA` created by one |
| `initial_snapshot="table"` or `"unlogged_table"`  | `CREATE` on the schema                                              |
| `initial_snapshot="direct"`                       | `SELECT` on the tables                                              |

See the [source code](https://github.com/dlt-hub/verified-sources/blob/master/sources/pg_replication/helpers.py) for all arguments.

### `replication_slot_exists`

```py notype
def replication_slot_exists(
    slot_name: str = dlt.config.value,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
) -> bool: ...
```

It returns `True` if the replication slot exists. It reads `pg_replication_slots` and needs no privileges. See [Initial load](#initial-load).

### `replication_resource`

```py notype
@dlt.resource(name=lambda args: args["slot_name"])
def replication_resource(
    slot_name: str,
    pub_name: str,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    include_columns: Optional[Dict[str, Sequence[str]]] = None,
    exclude_columns: Optional[Dict[str, Sequence[str]]] = None,
    columns: Optional[Dict[str, TTableSchemaColumns]] = None,
    target_batch_size: int = 1000,
    flush_slot: bool = True,
    backend: Literal["python", "pyarrow"] = "python",
    reflection_level: Literal["full", "full_with_precision"] = "full",
    reflect_not_null: bool = False,
    snapshot_pipeline: Optional[dlt.Pipeline] = None,
    hard_delete: bool = True,
) -> Iterable[Union[TDataItem, DataItemWithMeta]]: ...
```

It needs the `REPLICATION` attribute. Slots have no owner: any user with `REPLICATION` can read any slot. With `flush_slot=False`, dlt does not confirm the slot, which is useful for tests. See the [source code](https://github.com/dlt-hub/verified-sources/blob/master/sources/pg_replication/__init__.py) for all arguments.

## Upgrading from earlier versions

The changelog in the [source README](https://github.com/dlt-hub/verified-sources/blob/master/sources/pg_replication/README.md) lists all changes with upgrade steps. Version 1.0 requires dlt 1.22.0 and SQLAlchemy 2.0. Existing pipelines continue from their stored position and slot. No change is missed or loaded again. Breaking changes:

* `_dlt_lsn` orders the changes instead of `lsn`. `_dlt_deleted_ts` marks deletes instead of `deleted_ts`. In tables loaded before, `lsn` and `deleted_ts` stay empty in new rows.
* `replication_resource` loaded `real`, `time with time zone`, array and domain columns as `text`. Now these columns get the types in [Data types](#data-types). Existing destination columns keep `text`. Rows loaded before keep the old format until they change.
* New tables and columns get no length, no integer size and no `NOT NULL` except on the primary key. Existing destination columns keep their types.
* Tables without a primary key or a replica identity index raise `NoPrimaryKeyException` when the publication publishes updates or deletes.

`persist_snapshots=True` is deprecated. It still works, logs a warning and does the same as `initial_snapshot="table"`.

To load the tables again with the new types, run a [full refresh](#full-refresh).

## Alternative: change tracking with `xmin`

If logical replication does not fit, you can track changes with the `xmin` system column and the [`sql_database`](sql_database/index.md) source. Define a `query_adapter_callback` that selects `xmin` and filters on an incremental cursor:

```py
import sqlalchemy as sa


def query_adapter_callback(query, table, incremental=None, _engine=None) -> sa.TextClause:
    """Selects the rows of `table` with `xmin` at or after the incremental start value."""
    select_clause = f"SELECT {table.fullname}.*, xmin::text::bigint as xmin FROM {table.fullname}"

    if incremental:
        where_clause = (
            f" WHERE {incremental.cursor_path}::text::bigint >= "
            f"({incremental.start_value}::int8)"
        )
        return sa.text(select_clause + where_clause)

    return sa.text(select_clause)
```

This method does not need a timestamp or serial column. It loads new and updated rows, but not deletes. Also, `xmin` wraps around.
