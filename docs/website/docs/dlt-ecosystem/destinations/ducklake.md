---
title: DuckLake
description: DuckLake destination (DuckDB + ducklake extension)
keywords: [ducklake, duckdb, destination, data lake, lakehouse]
---

# DuckLake

[DuckLake](https://ducklake.select/) is a lakehouse-style destination that builds on the DuckDB engine with the [ducklake extension](https://ducklake.select/docs/stable/duckdb/introduction). It stores your `dlt` tables as files on a filesystem or object store while keeping table metadata in a separate SQL catalog.

In order to use ducklake you must provide the following infrastructure:
- **catalog**: a SQL database that stores table/partition metadata (sqlite, duckdb, postgres, mysql are supported)
- **storage**: a filesystem or object store holding table files (local files, s3, gcs, abfss, etc.)

If you are looking for a managed ducklake infra, check the [Motherduck Ducklake support](motherduck.md#ducklake-setup). `dlt` is also able to set-up a local ducklake with `sqlite` as catalog fully automatically.

<!--@@@DLT_DESTINATION_CAPABILITIES ducklake-->

## Quick start

- Install dlt with DuckDB dependencies:
```sh
pip install "dlt[ducklake]"
```

- Initialize new test pipeline
```sh
dlt init foo ducklake
```
`dlt init` will create a sample `secrets.toml` for **postgres** catalog and **s3** bucket storage. For local automatic setup comment out catalog and storage entries:
```toml
[destination.ducklake.credentials]
ducklake_name="ducklake"  # we recommend explicit ducklake name
```

- Run a test pipeline that writes to a local DuckLake:
```py
import dlt

pipeline = dlt.pipeline(
    pipeline_name="foo",
    destination="ducklake",
    dataset_name="lake_schema",
    dev_mode=True,
)

info = pipeline.run(
    [{"foo": 1}, {"foo": 2}],
    table_name="table_foo",
)
print(info)
print(pipeline.dataset().table_foo["foo"].df())
```
The console output will point you to where `sqlite` catalog database and data store were created:
- `lake_catalog.sqlite` catalog in current working directory
- `lake_catalog.files` folder with `lake_schema` subfolder for the dataset.

## Configure Ducklake
Pick your `ducklake_name` as described above. This name is the used:
- as attach name for the ducklake - each ducklake connection starts with **:memory:** connection to which we `ATTACH` the ducklake
- to set default folder name of the local filesystem storage and database file name for `sqlite` and `duckdb` (if no explicit configuration is provided)
- as **postgres** schema name where catalog tables will be created (if postgres configured)

### Configure catalog
You have the following options when configuring the catalog
- **sqlite**: very fast local catalog. You can set it up as follows:
```toml
[destination.ducklake.credentials]
catalog="sqlite:///catalog_x.db"
```
Snippet above stores catalog in `catalog_x.db` in cwd. Refer to [sqlite](sqlalchemy.md) configuration in `sqlalchemy` destination which reused the same configuration structure.
Note that we are not able to setup **sqlite** to write in parallel, even with `WAL` journaling.
Parallel writes produce conflicts on practically every catalog transactions so we had to put loader in **sequential mode**.

- **duckdb**: pretty fast and working **only in sequential mode** like **sqlite**. Parallel loads generate page faults and corrupt the catalog database.
```toml
[destination.ducklake.credentials]
catalog="duckdb:///catalog_y.duckdb"
```
Refer to [duckdb](duckdb.md) configuration for more options.

- **postgres**: currently the only catalog that can be considered production-grade with full parallelism support.
```toml
[destination.ducklake.credentials]
catalog="postgres://loader:pass@localhost:5432/dlt_data"
```
`ducklake` will use postgres schema with the name of `ducklake_name` config option and create required tables automatically.

- 🧪 **mysql**: uses the same code path as for **postgres** but we never tested it

- 🧪 **motherduck**: theoretically you could use Motherduck as catalog database. We were able to establish connection but unfortunately ducklake 1.2 segfaults when catalog is being attached.
```toml
[destination.ducklake.credentials]
catalog="md:///dlt_data"
```
Make sure that you have Motherduck token in your environment. Hopefully situation improves when duckdb 1.4 is supported.

### Configure storage
**storage** config reuses configuration of [filesystem](filesystem.md) destination. You can pick the following options:

- Local files: file:///path or a plain relative path
- S3: s3://bucket/prefix
- GCS: gs://bucket/prefix or gcs://bucket/prefix (**uses fsspec fallback**)
- Azure ADLS Gen2: abfss://container@account.dfs.core.windows.net/prefix (**uses fsspec fallback**)

Example s3 configuration:
```toml
[destination.ducklake.credentials]
ducklake_name="lakehouse"
catalog="postgres://loader:pass@localhost:5432/dlt_data"

[destination.ducklake.credentials.storage]
bucket_url="s3://dlt-ci-bucket"

[destination.ducklake.credentials.storage.credentials]
aws_access_key_id = "<configure me>" # fill this in!
aws_secret_access_key = "<configure me>" # fill this in!
```

### Configure additional connection options, pragmas and extensions
You can set additional connection options, pragmas and extensions - `ducklake` configuration reuses [duckdb configuration](duckdb.md#additional-configuration)
```toml
[destination.ducklake.credentials.global_config]
ducklake_max_retry_count=100
```

### Configure in code
You can create ducklake destination instance and configure it in code. In most cases you will just set additional options while still using the configuration:
```py
import dlt

# force parallel loads on sqlite
ducklake = dlt.destinations.ducklake(loader_parallelism_strategy="parallel")
pipeline = dlt.pipeline("test_factory", destination=ducklake, dataset_name="foo")
```
Above we force parallel loading on (default) sqlite catalog.

`DuckLakeCredentials` have friendly constructor where you can pass [catalog and storage credentials](../../general-usage/credentials/complex_types.md)
as shorthand strings and objects:
```py
import dlt
from dlt.destinations.impl.ducklake.configuration import DuckDbBaseCredentials

# set ducklake credentials using shorthands, s3 bucket requires secrets in config
credentials = DuckLakeCredentials(
    "lake_catalog",
    catalog="postgresql://loader:pass@localhost:5432/dlt_data",
    storage="s3://dlt-ci-test-bucket/lake",
)
destination = dlt.destinations.ducklake(credentials=credentials)
```

```py
import dlt
from dlt.sources.credentials import ConnectionStringCredentials

# set catalog name using connection string credentials
catalog_credentials = ConnectionStringCredentials()
# use duckdb with the default name
catalog_credentials.drivername = "duckdb"
credentials = DuckLakeCredentials(
    "lake_catalog",
    catalog=catalog_credentials,
)
```

As mentioned above, `filesystem` and `ducklake` share the same configuration object. Configuration for the `filesystem` can be reused:
```py

# `filesystem` below is a pipeline with configured filesystem destination

destination = dlt.destinations.ducklake(
    credentials=DuckLakeCredentials(
        "lake_catalog",
        storage=filesystem.destination_client().config
    )
)
```

## Maintain ducklake

### Data access
You have read and write access to the data in ducklake. You can take native duckdb connection with attached catalog and authenticated
storage using `sql_client`. This is demonstrated in examples below.

[dataset access](../../general-usage/dataset-access/) and **ibis** handover are fully supported.

### Set catalog options
Certain **ducklake** options are persisted in the catalog and are set differently than [connection options](#configure-additional-connection-options-pragmas-and-extensions). You can do that from code:

```py
import dlt
import duckdb

pipeline = dlt.pipeline(pipeline_name="foo", destination="ducklake", dataset_name="lake_schema")

# set per thread output option before pipeline runs so options are applied
with pipeline.sql_client() as client:
    con: duckdb.DuckDBPyConnection = client.native_connection
    # set option on `lake_catalog` we configured above
    con.sql("CALL lake_catalog.set_option('per_thread_output', true)")
```
Above we set `per_thread_output` (1.4.x only) before pipeline runs.

### Table maintenance
`dlt` has a standard interface to access open tables and catalogs but this is not implemented for ducklake (yet). However in case of
`ducklake` you just need configured and authorized connection which you can get after pipeline runs to do the maintenance.
```py

# pipeline.run(...)

with pipeline.sql_client() as client:
    print(client.execute_sql("CALL bucket_cat.merge_adjacent_files()"))
```

## Write disposition
All write dispositions are supported. `upsert` is supported on **duckdb 1.4.x** (without hard deletes for now)

## Data loading
By default, Parquet files and the `COPY` command are used to move local files to the remote storage,

The **INSERT** format is also supported and will execute large INSERT queries directly into the remote database. This method is significantly slower and may exceed the maximum query size, so it is not advised.

**partition** hint on a column is supported and works on **duckdb 1.4.x**. Simple identity partitions are created. Partition evolution is not supported.

**parallel** loading is supported via thread pool for postgres catalog (and probably mysql). We could not use [recommended method](https://duckdb.org/docs/stable/guides/python/multiple_threads.html) because the threads were (dead)locking. We open separate in-memory database for each thread to which we attach the catalog.

## dbt support
Not supported. We'd need to handover secrets and `ATTACH` command which is not planned at this moment.

## Syncing of `dlt` state
This destination fully supports [dlt state sync](../../general-usage/state#syncing-state-with-destination).

## ToDo
* open table interface for table maintenance like we have for iceberg and delta.
* better partitioning support.
* Motherduck as catalog if possible.
* support additional `ATTACH` options like `OVERRIDE_DATA_PATH`
* implement callbacks that will be called on creation of :memory: database and `ATTACH` command so those can be fully customized.
