---
title: Postgres replication
description: dlt verified source for Postgres replication
keywords: [postgres, postgres replication, database replication]
---
# Postgres replication

import Header from './_source-info-header.md';

<Header/>

[Postgres](https://www.postgresql.org/) is one of the most popular relational database management systems. This verified source uses Postgres replication functionality to efficiently process tables (a process often referred to as *Change Data Capture* or CDC). It uses [logical decoding](https://www.postgresql.org/docs/current/logicaldecoding.html) and the standard built-in `pgoutput` [output plugin](https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html).

Resources that can be loaded using this verified source are:

| Name                 | Description                                                                               |
| -------------------- | ----------------------------------------------------------------------------------------- |
| replication_resource | Load published messages from a replication slot                                           |
| init_replication     | Initialize replication and optionally return snapshot resources for the initial data load |

:::info
The Postgres replication source currently **does not** support the [scd2 merge strategy](../../general-usage/merge-loading.md#scd2-strategy).
:::

## Setup guide

### Set up user

To set up a Postgres user for replication, follow these steps:

1. Create a user with the `LOGIN` and `REPLICATION` attributes:

    ```sql
    CREATE ROLE replication_user WITH LOGIN REPLICATION;
    ```

2. Grant the `CREATE` privilege on the database:

    ```sql
    GRANT CREATE ON DATABASE dlt_data TO replication_user;
    ```

3. Grant ownership of the tables you want to replicate:

    ```sql
    ALTER TABLE your_table OWNER TO replication_user;  
    ```

:::note
The minimum required privileges may differ depending on your replication configuration. For example, replicating entire schemas requires superuser privileges. Check the [Sources and resources](#sources-and-resources) section for more detailed information.
:::

### Set up RDS

To set up a Postgres user on RDS, follow these steps:

1. Enable replication for your RDS Postgres instance via a [Parameter Group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PostgreSQL.Replication.ReadReplicas.html).

2. `WITH LOGIN REPLICATION;` does not work on RDS; instead, do:

    ```sql
    GRANT rds_replication TO replication_user;
    ```

3. Use the following connection parameters to enforce SSL:

   ```toml
   sources.pg_replication.credentials="postgresql://loader:password@host.rds.amazonaws.com:5432/dlt_data?sslmode=require&connect_timeout=300"
   ```

### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Run the following command:

   ```sh
   dlt init pg_replication duckdb
   ```

   This command initializes [pipeline examples](https://github.com/dlt-hub/verified-sources/blob/master/sources/pg_replication_pipeline.py) with Postgres replication as the [source](../../general-usage/source) and [DuckDB](../../dlt-ecosystem/destinations/duckdb) as the [destination](../../dlt-ecosystem/destinations).

2. If you'd like to use a different destination, simply replace `duckdb` with the name of your preferred [destination](../../dlt-ecosystem/destinations). For example:

   ```sh
   dlt init pg_replication bigquery
   ```

3. After running the command, a new directory will be created with the necessary files and configuration settings to get started.

### Add credentials

1. In the `.dlt` folder, there's a file called `secrets.toml`. It's where you store sensitive information securely, like access tokens. Keep this file safe.

   Here's what the `secrets.toml` looks like:

   ```toml
   [sources.pg_replication.credentials]
   drivername = "postgresql" # please set me up!
   database = "database" # please set me up!
   password = "password" # please set me up!
   username = "username" # please set me up!
   host = "host" # please set me up!
   port = 0 # please set me up! 
   ```

2. Credentials can be set as shown above. Alternatively, you can provide credentials in the `secrets.toml` file as follows:

   ```toml
   sources.pg_replication.credentials="postgresql://username@password.host:port/database"
   ```

3. Finally, follow the instructions in the [Destinations section](../../dlt-ecosystem/destinations/) to add credentials for your chosen destination.

For more information, read the [Configuration section.](../../general-usage/credentials)

## Run the pipeline

1. Ensure that you have installed all the necessary dependencies by running:

   ```sh
   pip install -r requirements.txt
   ```

2. After carrying out the necessary customization to your pipeline script, you can run the pipeline with the following command:

   ```sh
   python pg_replication_pipeline.py
   ```

3. Once the pipeline has finished running, you can verify that everything loaded correctly with:

   ```sh
   dlt pipeline <pipeline_name> show
   ```

   For example, the `pipeline_name` for the above pipeline example is `pg_replication_pipeline`, you may also use any custom name instead.

   For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).

## Sources and resources

### Snapshot resources from `init_replication`

The `init_replication` function serves two main purposes:

1. Sets up Postgres replication by creating the necessary replication slot and publication if they don't already exist.
2. Optionally captures an initial snapshot when `persist_snapshots=True` and returns snapshot resources for loading existing data.

```py notype
def init_replication(
    slot_name: str = dlt.config.value,
    pub_name: str = dlt.config.value,
    schema_name: str = dlt.config.value,
    table_names: Optional[Union[str, Sequence[str]]] = dlt.config.value,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    publish: str = "insert, update, delete",
    persist_snapshots: bool = False,
    include_columns: Optional[Mapping[str, Sequence[str]]] = None,
    columns: Optional[Mapping[str, TTableSchemaColumns]] = None,
    reset: bool = False,
) -> Optional[Union[DltResource, list[DltResource]]]:
    ...
```

Depending on how you configure `init_replication`, the minimum required privileges for the Postgres user may differ:

| Configuration                                                         | Description                                                                                                                                     | Minimum required privileges                                                                                                                                                                                                                              |
| --------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `table_names=None`                                                    | Replicates the entire schema. The publication includes all current and future tables in the schema.                                             | Superuser                                                                                                                                                                                                                                                |
| `table_names=[...]`<br />`reset=False`<br />`persist_snapshots=False` | Replicates specific tables. Creates or updates an existing publication/slot without dropping. No snapshot tables are created.                   | REPLICATION attribute,<br />CREATE on the database if the publication does not yet exist,<br />Publication ownership if the publication already exists,<br />Table ownership (for each table)                                                            |
| `table_names=[...]`<br />`reset=False`<br />`persist_snapshots=True`  | Replicates specific tables. Creates or updates an existing publication/slot without dropping. Snapshot tables are created for the initial load. | REPLICATION attribute,<br />CREATE on the database if the publication does not yet exist,<br />Publication ownership if the publication already exists,<br />Table ownership (for each table),<br />CREATE privilege in the schema (for snapshot tables) |
| `table_names=[...]`<br />`reset=True`<br />`persist_snapshots=False`  | Replicates specific tables. Drops existing publication/slot before recreating. No snapshot tables are created.                                  | REPLICATION attribute,<br />CREATE on the database,<br />Table ownership (for each table),<br />Slot/publication ownership if they already exist                                                                                                         |
| `table_names=[...]`<br />`reset=True`<br />`persist_snapshots=True`   | Replicates specific tables. Drops existing publication/slot before recreating. Snapshot tables are created for the initial load.                | REPLICATION attribute,<br />CREATE on the database,<br />Table ownership (for each table),<br />Slot/publication ownership if they already exist,<br />CREATE privilege in the schema (for snapshot tables)                                              |

For detailed information about all arguments, see the [source code](https://github.com/dlt-hub/verified-sources/blob/master/sources/pg_replication/helpers.py).

### Resource `replication_resource`

This resource yields data items for changes in one or more Postgres tables. It consumes messages from an existing replication slot and publication that must be set up beforehand (e.g., using `init_replication`).

```py
from collections.abc import Sequence
from typing import Iterable
from dlt.common.typing import TDataItem
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.extract.items import DataItemWithMeta

@dlt.resource(
    name=lambda args: args["slot_name"] + "_" + args["pub_name"],
)
def replication_resource(
    slot_name: str,
    pub_name: str,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    include_columns: dict[str, Sequence[str]] | None = None,
    columns: dict[str, TTableSchemaColumns] | None = None,
    target_batch_size: int = 1000,
    flush_slot: bool = True,
) -> Iterable[TDataItem | DataItemWithMeta]:
    ...
```

The minimum required privileges for using `replication_resource` are straightforward:

- REPLICATION attribute (required for logical replication connections)
- Slot ownership (required to consume messages from the replication slot)
- Read access to publication metadata (to query the `pg_publication` system catalog)

For detailed information about the arguments, refer to the [source code](https://github.com/dlt-hub/verified-sources/blob/master/sources/pg_replication/__init__.py).

## Customization

The [pipeline examples](https://github.com/dlt-hub/verified-sources/blob/master/sources/pg_replication_pipeline.py) include demos that simulate changes in a Postgres source to demonstrate replication. The simulation uses a simple pipeline defined as:

   ```py
   # Simulation pipeline
   sim_pl = dlt.pipeline(
       pipeline_name="simulation_pipeline",
       destination="postgres",
       dataset_name="source_dataset",
       dev_mode=True,
   )
   ```

This pipeline is configured in the `get_postgres_pipeline()` function.
It’s meant for local testing, so you can freely modify it to simulate different replication scenarios.

:::note
In production, you don’t need a simulation pipeline. Replication runs against an actual Postgres database that changes independently.
:::

The general workflow for setting up replication is:

1. Define the replication pipeline that will load replicated data in your chosen destination:

   ```py
   repl_pl = dlt.pipeline(
       pipeline_name="pg_replication_pipeline",
       destination='duckdb',
       dataset_name="replicate_single_table",
       dev_mode=True,
   )
   ```

2. Initialize replication (if needed) with `init_replication`, and capture a snapshot of the source:

      ```py notype
      snapshot = init_replication(  
         slot_name="my_slot",
         pub_name="my_pub",
         schema_name="my_schema",
         table_names="my_source_table",
         persist_snapshots=True,
         reset=True,
      )
      ```

3. Load the initial snapshot, so the destination contains all existing data before replication begins:

   ```py notype
   repl_pl.run(snapshot)
   ```

4. Apply ongoing changes by creating a `replication_resource` to capture updates and keep the destination in sync:

   ```py notype
   # Create a resource that generates items for each change in the source table
   changes = replication_resource("my_slot", "my_pub")
 
   repl_pl.run(changes)
   ```

## Initial snapshots and point-in-time backfilling

The initial state you can load is the one Postgres exports at the moment the replication slot is created. There is no argument that takes a timestamp or an LSN: you cannot snapshot a table as it looked yesterday, and you cannot replay changes committed before the slot existed, because Postgres starts retaining WAL for a slot only when that slot is created.

### How the snapshot lines up with the stream

When you call `init_replication` with `persist_snapshots=True`, and the slot does not exist yet, dlt:

1. creates the replication slot, which makes Postgres export a snapshot taken at the slot's consistent point.
2. opens a second session, puts it in `REPEATABLE READ` with `SET TRANSACTION SNAPSHOT` on the exported snapshot, and copies each included table into a `_dlt_<table>_s_<snapshot_name>` table in the same schema.
3. returns one resource per copied table, which you load before you start consuming changes.

The snapshot and the stream therefore meet exactly: the snapshot resources hold the rows that were visible at the consistent point, and `replication_resource` yields every change committed after it. Writes to the source are not blocked while the copy runs. They arrive through the stream instead. Snapshot resources are created with the `merge` write disposition and the source table's primary key (`append` when the publication publishes inserts only), so rows that appear in both the snapshot and the first batch of changes are not counted twice.

:::note
Snapshot tables are ordinary tables in your source database. They need the CREATE privilege in the schema and as much storage as the data they hold, and dlt does not drop them. Remove them yourself once the initial load has succeeded. Their `_dlt` prefix is what keeps them out of the replicated data: when you replicate an entire schema, the snapshot tables are published too, and dlt skips tables whose name starts with `_dlt`.
:::

### A snapshot is only taken when the slot is created

`persist_snapshots=True` has an effect only on the call that creates the slot. If the slot already exists, `init_replication` raises:

```text
RuntimeError: Cannot create snapshots because slot my_slot is already created.
```

To get a new snapshot, recreate the slot with `reset=True`. The publication and the slot are dropped and created again, so the new stream starts at a new consistent point, everything the old slot was still retaining is discarded, and you load the new snapshot into your destination from scratch.

Reset the pipeline state together with the slot. `replication_resource` keeps the LSN of the last consumed message in its [resource state](../../general-usage/state) and, with `flush_slot=True` (the default), advances the slot to that LSN at the start of the next run. After a reset, the stored LSN lies behind the position of the freshly created slot, and Postgres refuses the advance:

```text
ERROR:  cannot advance replication slot to 0/1574000, minimum is 0/15741C0
```

Run the reset against a fresh pipeline (the demo pipelines use `dev_mode=True`), or drop the state of the replication resource first.

### Changes that were already consumed cannot be replayed

Each run resumes at the last commit LSN in the resource state and reads up to the latest message in the slot. Once messages are flushed, Postgres is free to discard the WAL that held them, so there is no way to rewind the stream to an earlier LSN. To load a table again, recreate the slot with `reset=True` and load the new snapshot, or backfill the table with the `sql_database` source as described below.

### Backfill history with the `sql_database` source

Use the [sql_database](./sql_database/index.md) source when you need rows that predate the slot, or when copying a large table into a snapshot table is too expensive. Create the slot first, so that no change can slip through between the backfill and the start of replication:

1. Create the slot and publication without snapshots:

   ```py notype
   init_replication(
       slot_name="my_slot",
       pub_name="my_pub",
       schema_name="my_schema",
       table_names="my_source_table",
       persist_snapshots=False,
   )
   ```

2. Backfill the table, merging on the same primary key that the replicated table uses. [Backfilling in chunks](../../examples/backfill_in_chunks) shows how to split a long backfill into ranges:

   ```py
   import dlt
   from dlt.sources.sql_database import sql_table

   backfill = sql_table(
       credentials="postgresql://username:password@host:5432/database",
       table="my_source_table",
       schema="my_schema",
   )

   repl_pl = dlt.pipeline(
       pipeline_name="pg_replication_pipeline",
       destination="duckdb",
       dataset_name="replicate_single_table",
   )
   repl_pl.run(backfill, write_disposition="merge", primary_key="id")
   ```

3. Run `replication_resource` afterwards. Changes committed during the backfill are still in the slot, and re-applying the ones the backfill already read is harmless because they merge on the primary key.

:::caution
This ordering is safe because the changes are merged. If the publication publishes inserts only (`publish="insert"`), replicated rows are appended instead, and rows the backfill already loaded can arrive a second time.
:::

## Views and materialized views

Postgres logical replication publishes ordinary tables only: [regular views and materialized views cannot be part of a publication](https://www.postgresql.org/docs/current/sql-createpublication.html), so this source cannot replicate them. Passing a view name in `table_names` fails when dlt adds it to the publication:

```text
ERROR:  cannot add relation "orders_v" to publication
DETAIL:  This operation is not supported for views.
```

A materialized view fails in the same way, with `DETAIL:  This operation is not supported for materialized views.`

Replicating an entire schema with `table_names=None` does not include them either: `ALTER PUBLICATION ... ADD TABLES IN SCHEMA` covers the schema's tables and leaves views and materialized views out, and the list of tables to snapshot is reflected with the `sql_database` source, which does not reflect views unless you ask it to.

### Load a view with the `sql_database` source

Both views and materialized views can be read as if they were tables, so load them with the [sql_database](./sql_database/index.md) source, in the same pipeline that runs your replication resources:

```py
import dlt
from dlt.sources.sql_database import sql_table

orders_view = sql_table(
    credentials="postgresql://username:password@host:5432/database",
    table="orders_v",
    schema="public",
)

repl_pl = dlt.pipeline(
    pipeline_name="pg_replication_pipeline",
    destination="duckdb",
    dataset_name="replicate_single_table",
)
repl_pl.run(orders_view, write_disposition="replace")
```

A view has no primary key to reflect, so `sql_table` appends by default. Use `write_disposition="replace"` as above for a full refresh, or set `primary_key` together with `write_disposition="merge"`. If the view exposes a column that only moves forward, add an [incremental cursor](../../general-usage/incremental/cursor) so that each run reads new rows only. A materialized view changes only when it is refreshed, and the refresh never reaches the replication stream, so schedule the load after your `REFRESH MATERIALIZED VIEW`.

To load several views, use the `sql_database` source with `include_views=True`. View names listed in `table_names` are reflected regardless of that setting.

### Rebuild the view in the destination

The option that keeps the CDC guarantees is to replicate the tables the view is built on and to recreate its logic in the destination, with [SQL transformations](../transformations/sql) or [dbt](../transformations/dbt/dbt). The underlying tables stay in sync through the replication stream, and the view is recomputed where the data lands.

## Alternative: Using `xmin` for Change Data Capture (CDC)

If logical replication doesn't fit your needs, you can use the built-in `xmin` system column of Postgres for change tracking with dlt's `sql_database` source instead of the `pg_replication` source.

To do this, define a `query_adapter_callback` that extracts the `xmin` value from the source table and filters based on an incremental cursor:

```py
import sqlalchemy as sa

def query_adapter_callback(query, table, incremental=None, _engine=None) -> sa.TextClause:
    """Generate a SQLAlchemy text clause for querying a table with optional incremental filtering."""
    select_clause = (
        f"SELECT {table.fullname}.*, xmin::text::bigint as xmin FROM {table.fullname}"
    )

    if incremental:
        where_clause = (
            f" WHERE {incremental.cursor_path}::text::bigint >= "
            f"({incremental.start_value}::int8)"
        )
        return sa.text(select_clause + where_clause)

    return sa.text(select_clause)
```

This approach enables you to track changes based on the `xmin` value instead of a manually defined column, which is especially useful in cases where mutation tracking is needed but a timestamp or serial column is not available.

## Alternative: CDC with Debezium

If you need CDC from other databases, Debezium can be the CDC source and dlt can load the change events into your destination.

In this setup, Debezium runs in-process (via `pydbzengine`). It reads the database's transaction log (for example WAL or binlog) and produces a JSON record for each change. A dlt pipeline loads those records into your destination.

See the [Debezium + dlt demo](https://github.com/dlt-hub/dlt_demos/tree/main/dlt-debezium-demo) for a complete example.
