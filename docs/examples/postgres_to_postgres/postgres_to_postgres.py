"""
---
title: Load from Postgres to Postgres with ConnectorX & Arrow export as Parquet, normalizing and exporting as DuckDB, and attaching it to Postgres
description: Load data fast from Postgres to Postgres with ConnectorX and DuckDB for bigger Postgres tables (GBs)
keywords: [connector x, pyarrow, zero copy, duckdb, postgres, initial load]
---

This examples shows you how to export and import data from postgres to postgres in a fast way with ConnectorX and DuckDB as the default export will
generate `Insert_statement` in the normalization phase, which is super slow for large tables.

As it's an initial load, we create a separate schema with timestamp initially and then replace the existing schema with the new one.

:::note
This approach is tested and works well for an initial load (`--replace`), the incremental load (`--merge`) might need some adjustments (loading of load-tables of dlt, setting up first run after an intial
load, etc.).
:::

We'll learn:

- How to get arrow tables from [connector X](https://github.com/sfu-db/connector-x) and yield them in chunks.
- That merge and incremental loads work with arrow tables.
- How to use DuckDB for a speedy normalization.
- How to use `argparse` to turn your pipeline script into CLI.


Be aware that you need to define the database credentials in `.dlt/secrets.toml` and adjust the tables names (table1 and table2):

Install `dlt` with `duckdb` as extra, also `connectorx`, Postgres adapter and progress bar tool:

```sh
pip install duckdb connectorx psycopg2-binary alive-progress
```

Run the example:
```sh
python postgres_to_postgres.py --replace
```

:::warn
Attention: There were problems with data type TIME that includes nano seconds. More details in
[Slack](https://dlthub-community.slack.com/archives/C04DQA7JJN6/p1711579390028279?thread_ts=1711477727.553279&cid=C04DQA7JJN60)

As well as with installing DuckDB (see [issue
here](https://github.com/duckdb/duckdb/issues/8035#issuecomment-2020803032)), that's why I manually the `postgres_scanner.duckdb_extension` in my Dockerfile to load the data into Postgres.
:::

"""

import argparse
import os
from datetime import datetime

import connectorx as cx
import duckdb
import psycopg2
import dlt
from dlt.sources.credentials import ConnectionStringCredentials

CHUNKSIZE = int(
    os.getenv("CHUNKSIZE", 1000000)
)  # 1 mio rows works well with 1GiB RAM memory (if no parallelism)
dataset_name = "lzn"
pipeline_name = "loading_postgres_to_postgres"
schema_name = "example_data_1"
connection_timeout = 15


def read_sql_x_chunked(conn_str: str, query: str, chunk_size: int = CHUNKSIZE):
    offset = 0
    while True:
        chunk_query = f"{query} LIMIT {chunk_size} OFFSET {offset}"
        data_chunk = cx.read_sql(
            conn_str,
            chunk_query,
            return_type="arrow2",
            protocol="binary",
        )
        yield data_chunk
        if data_chunk.num_rows < chunk_size:
            break  # No more data to read
        offset += chunk_size


@dlt.source(max_table_nesting=0)
def pg_resource_chunked(
    name: str,
    primary_key: list[str],
    table_name: str,
    order_date: str,
    load_type: str = "merge",
    columns: str = "*",
    credentials: ConnectionStringCredentials = dlt.secrets[
        "sources.postgres.credentials"
    ],
):
    print(
        f"dlt.resource write_dispostion: `{load_type}` -- ",
        f"connection string: postgresql://{credentials.username}:*****@{credentials.host}:{credentials.host}/{credentials.database}"
    )

    query = f"SELECT {columns} FROM {table_name} ORDER BY {order_date}"  # Needed to have an idempotent query

    source = dlt.resource(
        name=name,
        table_name=table_name,
        write_disposition=load_type,  # use `replace` for initial load, `merge` for incremental
        primary_key=primary_key,
        standalone=True,
        parallelized=True,
    )(read_sql_x_chunked)(
        credentials.to_native_representation(),  # Pass the connection string directly
        query,
    )

    if load_type == "merge":
        # Retrieve the last value processed for incremental loading
        source.apply_hints(incremental=dlt.sources.incremental(order_date))

    return source


def table_desc(name, pk, table_name, order_date, columns="*"):
    return {
        "name": name,
        "pk": pk,
        "table_name": table_name,
        "order_date": order_date,
        "columns": columns,
    }


if __name__ == "__main__":
    # Input Handling
    parser = argparse.ArgumentParser(
        description="Run specific functions in the script."
    )
    parser.add_argument("--replace", action="store_true", help="Run initial load")
    parser.add_argument("--merge", action="store_true", help="Run delta load")
    args = parser.parse_args()

    tables = [
        table_desc("table_1", ["pk"], f"{schema_name}.table_1", "updated_at"),
        table_desc("table_2", ["pk"], f"{schema_name}.table_2", "updated_at"),
    ]

    if args.replace:
        load_type = "replace"
    else:
        # default is delta load
        load_type = "merge"

    print(f"LOAD-TYPE: {load_type}")

    resources = []
    for table in tables:
        resources.append(
            pg_resource_chunked(
                table["name"],
                table["pk"],
                table["table_name"],
                table["order_date"],
                load_type=load_type,
                columns=table["columns"],
            )
        )

    if load_type == "replace":
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination="duckdb",
            dataset_name=dataset_name,
            full_refresh=True,
            progress="alive_progress",
        )
    else:
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination="postgres",
            dataset_name=dataset_name,
            full_refresh=False,
        )  # full_refresh=False

    # start timer
    startTime = datetime.now()

    # 1. extract
    print("##################################### START EXTRACT ########")
    pipeline.extract(resources)
    print(f"--Time elapsed: {datetime.now() - startTime}")

    # 2. normalize
    print("##################################### START NORMALIZATION ########")
    if load_type == "replace":
        info = pipeline.normalize(workers=2, loader_file_format="parquet")  # https://dlthub.com/docs/blog/dlt-arrow-loading
    else:
        info = pipeline.normalize()

    print(info)
    print(pipeline.last_trace.last_normalize_info)
    print(f"--Time elapsed: {datetime.now() - startTime}")

    # 3. load
    print("##################################### START LOAD ########")
    info = pipeline.load()
    print(info)
    print(f"--Time elapsed: {datetime.now() - startTime}")

    if load_type == "replace":
        # 4. Load DuckDB local database into Postgres
        print("##################################### START DUCKDB LOAD ########")
        conn = duckdb.connect(f"{pipeline_name}.duckdb")
        conn.install_extension("./postgres_scanner.duckdb_extension")  # duckdb_extension is downloaded/installed manually here, only needed if `LOAD/INSTALL postgres` throws an error
        conn.sql("LOAD postgres;")
        # select generated timestamp schema
        timestamped_schema = conn.sql(f"""select distinct table_schema from information_schema.tables
                     where table_schema like '{dataset_name}%'
                     and table_schema NOT LIKE '%_staging'
                     order by table_schema desc""").fetchone()[0]  # type: ignore
        print(f"timestamped_schema: {timestamped_schema}")

        target_credentials = dlt.secrets["sources.postgres.credentials"]
        # connect to destination (timestamped schema)
        conn.sql(
            f"ATTACH 'dbname={target_credentials.database} user={target_credentials.username} password={target_credentials.password} host={target_credentials.host} port={target_credentials.port}' AS pg_db (TYPE postgres);")
        conn.sql(f"CREATE SCHEMA IF NOT EXISTS pg_db.{timestamped_schema};")

        for table in tables:
            print(f"LOAD DuckDB -> Postgres: table: {timestamped_schema}.{table['name']} TO Postgres {timestamped_schema}.{table['name']}")

            conn.sql(f"CREATE OR REPLACE TABLE pg_db.{timestamped_schema}.{table['name']} AS SELECT * FROM {timestamped_schema}.{table['name']};")
            conn.sql(f"SELECT count(*) as count FROM pg_db.{timestamped_schema}.{table['name']};").show()

        print(f"--Time elapsed: {datetime.now() - startTime}")
        print("##################################### FINISHED ########")

        # 5. Cleanup and rename Schema
        print("##################################### RENAME Schema and CLEANUP ########")
        try:
            con_hd = psycopg2.connect(
                dbname=target_credentials.database,
                user=target_credentials.username,
                password=target_credentials.password,
                host=target_credentials.host,
                port=target_credentials.port
            )
            con_hd.autocommit = True
            print("Connected to HD-DB: " + target_credentials.host + ", DB: " + target_credentials.username)
        except:
            print("Unable to connect to HD-database!")

        with con_hd.cursor() as cur:
            # Drop existing lzn
            print(f"Drop existing lzn")
            cur.execute(f"DROP SCHEMA IF EXISTS lzn CASCADE;")
            # Rename timestamped-lzn to lzn
            print(f"Going to rename schema {timestamped_schema} to lzn")
            cur.execute(f"ALTER SCHEMA {timestamped_schema} RENAME TO lzn;")

        con_hd.close()
