---
title: Postgres replication
description: dlt verified source for Postgres replication
keywords: [postgres, postgres replication, database replication]
---
import Header from './_source-info-header.md';

# Postgres replication

<Header/>

[Postgres](https://www.postgresql.org/) is one of the most popular relational database management systems. This verified source uses Postgres replication functionality to efficiently process tables (a process often referred to as *Change Data Capture* or CDC). It uses [logical decoding](https://www.postgresql.org/docs/current/logicaldecoding.html) and the standard built-in `pgoutput` [output plugin](https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html).

Resources that can be loaded using this verified source are:

| Name                 | Description                                     |
| -------------------- | ----------------------------------------------- |
| replication_resource | Load published messages from a replication slot |

:::info
The Postgres replication source currently **does not** support the [scd2 merge strategy](../../general-usage/incremental-loading#scd2-strategy). 
:::

## Setup guide

### Setup user
To set up a Postgres user, follow these steps:

1. The Postgres user needs to have the `LOGIN` and `REPLICATION` attributes assigned:
    
    ```sql
    CREATE ROLE replication_user WITH LOGIN REPLICATION;
    ```
    
2. It also needs `GRANT` privilege on the database:
    
    ```sql
    GRANT CREATE ON DATABASE dlt_data TO replication_user;
    ```
    

### Set up RDS
To set up a Postgres user on RDS, follow these steps:

1. You must enable replication for the RDS Postgres instance via [Parameter Group](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PostgreSQL.Replication.ReadReplicas.html).

2. `WITH LOGIN REPLICATION;` does not work on RDS; instead, do:
    
    ```sql
    GRANT rds_replication TO replication_user;
    ```
    
3. Do not fallback to a non-SSL connection by setting connection parameters:
    
   ```toml
   sources.pg_replication.credentials="postgresql://loader:password@host.rds.amazonaws.com:5432/dlt_data?sslmode=require&connect_timeout=300"
   ```
### Initialize the verified source

To get started with your data pipeline, follow these steps:

1. Enter the following command:
    
   ```sh
   dlt init pg_replication duckdb
   ```
    
   It will initialize [the pipeline example](https://github.com/dlt-hub/verified-sources/blob/master/sources/pg_replication_pipeline.py) with a Postgres replication as the [source](../../general-usage/source) and [DuckDB](../../dlt-ecosystem/destinations/duckdb) as the [destination](../../dlt-ecosystem/destinations).
    
    
2. If you'd like to use a different destination, simply replace `duckdb` with the name of your preferred [destination](../../dlt-ecosystem/destinations).
    
3. This source uses the `sql_database` source; you can initialize it as follows:
    
   ```sh
   dlt init sql_database duckdb
   ```
   :::note
   It is important to note that it is now only required if a user performs an initial load, specifically when `persist_snapshots` is set to `True`.
   :::
    
4. After running these two commands, a new directory will be created with the necessary files and configuration settings to get started.
   
   For more information, read the guide on [how to add a verified source](../../walkthroughs/add-a-verified-source).

   :::note
   You can omit the `[sql.sources.credentials]` section in `secrets.toml` as it is not required.
   :::


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

3. Finally, follow the instructions in [Destinations](../../dlt-ecosystem/destinations/) to add credentials for your chosen destination. This will ensure that your data is properly routed.

For more information, read the [Configuration section.](../../general-usage/credentials)

## Run the pipeline

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by running the command:
   ```sh
   pip install -r requirements.txt
   ```
2. You're now ready to run the pipeline! To get started, run the following command:
   ```sh
   python pg_replication_pipeline.py
   ```
3. Once the pipeline has finished running, you can verify that everything loaded correctly by using the following command:
   ```sh
   dlt pipeline <pipeline_name> show
   ```
   For example, the `pipeline_name` for the above pipeline example is `pg_replication_pipeline`, you may also use any custom name instead.


   For more information, read the guide on [how to run a pipeline](../../walkthroughs/run-a-pipeline).
    

## Sources and resources

`dlt` works on the principle of [sources](../../general-usage/source) and [resources](../../general-usage/resource).

### Resource `replication_resource`

This resource yields data items for changes in one or more Postgres tables.

```py
@dlt.resource(
    name=lambda args: args["slot_name"] + "_" + args["pub_name"],
    standalone=True,
)
def replication_resource(
    slot_name: str,
    pub_name: str,
    credentials: ConnectionStringCredentials = dlt.secrets.value,
    include_columns: Optional[Dict[str, Sequence[str]]] = None,
    columns: Optional[Dict[str, TTableSchemaColumns]] = None,
    target_batch_size: int = 1000,
    flush_slot: bool = True,
) -> Iterable[Union[TDataItem, DataItemWithMeta]]:
    ...
```

`slot_name`: Replication slot name to consume messages.

`pub_name`: Publication slot name to publish messages.

`include_columns`: Maps table name(s) to a sequence of names of columns to include in the generated data items. Any column not in the sequence is excluded. If not provided, all columns are included.

`columns`:  Maps table name(s) to column hints to apply on the replicated table(s).

`target_batch_size`: Desired number of data items yielded in a batch. Can be used to limit the data items in memory.

`flush_slot`:  Whether processed messages are discarded from the replication slot. The recommended value is "True".

## Customization

If you wish to create your own pipelines, you can leverage source and resource methods from this verified source.

1. Define the source pipeline as:
    
   ```py
   # Defining source pipeline
   src_pl = dlt.pipeline(
       pipeline_name="source_pipeline",
       destination="postgres",
       dataset_name="source_dataset",
       dev_mode=True,
   )
   ```

   You can configure and use the `get_postgres_pipeline()` function available in the `pg_replication_pipeline.py` file to achieve the same functionality. 

   :::note IMPORTANT
    When working with large datasets from a Postgres database, it's important to consider the relevance of the source pipeline. For testing purposes, using the source pipeline can be beneficial to try out the data flow. However, in production use cases, there will likely be another process that mutates the Postgres database. In such cases, the user generally only needs to define a destination pipeline.
   :::

    
2. Similarly, define the destination pipeline.
    
   ```py
   dest_pl = dlt.pipeline(
       pipeline_name="pg_replication_pipeline",
       destination='duckdb',
       dataset_name="replicate_single_table",
       dev_mode=True,
   )
   ```
    
3. Define the slot and publication names as:
    
   ```py
   slot_name = "example_slot"
   pub_name = "example_pub"
   ```
    
4. To initialize replication, you can use the `init_replication` function. A user can use this function to let `dlt` configure Postgres and make it ready for replication.
    
   ```py
   # requires the Postgres user to have the REPLICATION attribute assigned
   init_replication(  
       slot_name=slot_name,
       pub_name=pub_name,
       schema_name=src_pl.dataset_name,
       table_names="my_source_table",
       reset=True,
   )
   ```
    
   :::note
   To replicate the entire schema, you can omit the `table_names` argument from the `init_replication` function.
   :::

5. To snapshot the data to the destination during the initial load, you can use the `persist_snapshots=True` argument as follows:
   ```py
   snapshot = init_replication(  # requires the Postgres user to have the REPLICATION attribute assigned
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=src_pl.dataset_name,
        table_names="my_source_table",
        persist_snapshots=True,  # persist snapshot table(s) and let function return resource(s) for initial load
        reset=True,
    )
   ```

6. To load this snapshot to the destination, run the destination pipeline as:
    
   ```py
   dest_pl.run(snapshot)
   ```
    
7. After changes are made to the source, you can replicate the changes to the destination using the `replication_resource`, and run the pipeline as:
    
   ```py
   # Create a resource that generates items for each change in the source table
   changes = replication_resource(slot_name, pub_name)
 
   # Run the pipeline as
   dest_pl.run(changes)
   ```
    
8. To replicate tables with selected columns, you can use the `include_columns` argument as follows:
    
   ```py
   # requires the Postgres user to have the REPLICATION attribute assigned
   initial_load = init_replication(  
       slot_name=slot_name,
       pub_name=pub_name,
       schema_name=src_pl.dataset_name,
       table_names="my_source_table",
       include_columns={
           "my_source_table": ("column1", "column2")
       },
       reset=True,
   )
   ```
    
   Similarly, to replicate changes from selected columns, you can use the `table_names` and `include_columns` arguments in the `replication_resource` function.

