---
title: DuckLake
description: DuckLake destination (DuckDB + ducklake extension)
keywords: [ducklake, duckdb, destination, data lake, lakehouse]
---

# DuckLake

DuckLake is a lakehouse-style destination that builds on the DuckDB engine via the ducklake extension. It stores your `dlt` tables as files on a filesystem or object store while keeping table metadata in a separate SQL catalog.

It has three building blocks (see the in-code docs in dlt/destinations/impl/ducklake/ducklake.py):
- ducklake client: a DuckDB process with the ducklake extension loaded
- catalog: a SQL database that stores table/partition metadata (sqlite, duckdb, postgres, mysql are supported)
- storage: a filesystem or object store holding table files (local files, s3, gcs, abfss, etc.)

For general DuckDB behavior (SQL semantics, naming, dbt, etc.), see the DuckDB destination page. DuckLake reuses DuckDB client code and capabilities. Refer to docs/dlt-ecosystem/destinations/duckdb.md.

```sh
dlt init chess ducklake
```
will create a sample `secrets.toml` for postgres catalog and s3 bucket storage.

## Quick start

- Install dlt with DuckDB dependencies:
```sh
pip install "dlt[ducklake]"
```

- Run a pipeline that writes Parquet to a local DuckLake (defaults shown below):
```py
import dlt
from dlt.destinations import ducklake

pipe = dlt.pipeline(
    pipeline_name="destination_defaults",
    destination=ducklake(),
    dataset_name="lake_schema",
    dev_mode=True,
)

info = pipe.run(
    [{"foo": 1}, {"foo": 2}],
    table_name="table_foo",
    loader_file_format="parquet",
)
print(info)
print(pipe.dataset().table_foo["foo"].arrow())
```

## What DuckLake configures for you

DuckLake’s configuration surface is centered on the catalog and storage. The destination class and credential types live in:
- dlt/destinations/impl/ducklake/factory.py
- dlt/destinations/impl/ducklake/configuration.py
- dlt/destinations/impl/ducklake/sql_client.py
- dlt/destinations/impl/ducklake/ducklake.py

Key defaults and behaviors from the code:
- preferred loader file format is Parquet and is used by default.
- when using a local catalog (sqlite or duckdb), loads run sequentially to avoid catalog contention. With postgres/mysql catalogs, parallel loading is enabled.
- each SQL connection attaches the DuckLake with an ATTACH ... (DATA_PATH ...) statement and detaches it on close to keep the catalog consistent.
- for non-local storage, credentials are registered as DuckDB secrets; abfss and gcs/gs fall back to fsspec for access, which may affect scan performance.
- job metrics expose a remote_url that points to the table’s directory in the storage (see DuckLakeCopyJob in ducklake.py).

## Catalog configuration

The catalog holds table metadata. You can point it at:
- sqlite: sqlite:///catalog.sqlite (default if you do not specify a catalog)
- duckdb: duckdb:///catalog.duckdb
- postgres: postgres://user:pass@host:5432/dbname
- mysql: mysql://user:pass@host:3306/dbname

Notes based on sql_client.py:
- sqlite/duckdb catalogs are attached with WAL and reasonable busy-timeout/synchronous settings.
- postgres/mysql catalogs are attached via a ducklake:postgres:… URL.

Examples

Python
```py
from dlt.destinations import ducklake
from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials

# explicit sqlite catalog file
dest = ducklake(credentials=DuckLakeCredentials(catalog="sqlite:///catalog.sqlite"))
pipe = dlt.pipeline("my_pipe", destination=dest, dataset_name="lake_schema", dev_mode=True)
```

TOML (secrets/config)
```toml
destination.ducklake.credentials = "ducklake:///my_lake"
# pick a catalog backend
destination.ducklake.credentials.catalog = "sqlite:///catalog.sqlite"
# or
# destination.ducklake.credentials.catalog = "duckdb:///catalog.duckdb"
# or
# destination.ducklake.credentials.catalog = "postgres://loader:loader@localhost:5432/dlt_data"
```

Behavior and performance
- with sqlite/duckdb catalogs, loader parallelism is set to sequential.
- with postgres/mysql catalogs, parallel loading is enabled.

## Storage configuration

Storage is where your table files live. Provide a bucket URL (or a local path). Supported schemes are handled either natively by DuckDB or via fsspec:
- Local files: file:///path or a plain relative path
- S3: s3://bucket/prefix
- GCS: gs://bucket/prefix or gcs://bucket/prefix (uses fsspec fallback)
- Azure ADLS Gen2: abfss://container@account.dfs.core.windows.net/prefix (uses fsspec fallback)

Defaults and layout
- if you do not specify storage, a local folder is created using the pattern "<ducklake_name>.files" under the pipeline working directory (see DUCKLAKE_STORAGE_PATTERN in configuration.py).
- dlt writes data under <storage>/<dataset_name>/<table_name>/… so each table has its own directory.
- metrics.remote_url points to <bucket_url>/<dataset_name>/<table_name>.

Examples

Python
```py
from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials

# use an explicit lake name and S3 storage
dest = ducklake(credentials=DuckLakeCredentials("my_lake", storage="s3://my-bucket/prefix"))
pipe = dlt.pipeline("s3_example", destination=dest, dataset_name="lake_schema", dev_mode=True)
pipe.run([{"foo": 1}, {"foo": 2}], table_name="table_foo", loader_file_format="parquet")
```

TOML
```toml
destination.ducklake.credentials = "ducklake:///my_lake"
destination.ducklake.credentials.storage = "s3://my-bucket/prefix"
```

Cloud-specific notes (from sql_client.py)
- abfss/az: registered via fsspec; expect lower scanning performance than native connectors.
- gs/gcs: also uses fsspec fallback if a native secret cannot be created.

## End-to-end examples (mirroring tests)

Using different catalogs (see tests/load/ducklake/test_ducklake_pipeline.py::test_all_catalogs)
```py
import dlt
from dlt.destinations import ducklake
from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials

for cat in (None, "sqlite:///catalog.sqlite", "duckdb:///catalog.duckdb", "postgres://loader:loader@localhost:5432/dlt_data"):
    dest = ducklake() if cat is None else ducklake(credentials=DuckLakeCredentials(catalog=cat))
    p = dlt.pipeline("destination_defaults", destination=dest, dataset_name="lake_schema", dev_mode=True)
    info = p.run([{"foo": 1}, {"foo": 2}], table_name="table_foo", loader_file_format="parquet")
    assert p.dataset().table_foo["foo"].fetchall() == [(1,), (2,)]
```

Using different object stores (see tests/load/ducklake/test_ducklake_pipeline.py::test_all_buckets)
```py
# assuming you already have appropriate cloud credentials configured in your environment
from dlt.destinations import ducklake
from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials

for bucket_url in ("gs://your-bucket", "abfss://container@account.dfs.core.windows.net/prefix", "s3://your-bucket/prefix"):
    dest = ducklake(credentials=DuckLakeCredentials("bucket_cat", storage=bucket_url))
    p = dlt.pipeline("destination_defaults", destination=dest, dataset_name="lake_schema", dev_mode=True)
    info = p.run([{"foo": 1}, {"foo": 2}], table_name="table_foo", loader_file_format="parquet")
    # verify data
    assert p.dataset().table_foo["foo"].fetchall() == [(1,), (2,)]
    # each job metric reports where files were written
    metrics = info.metrics[info.loads_ids[0]][0]["job_metrics"]
    for job_id, m in metrics.items():
        assert m.remote_url.startswith(bucket_url)
```

Tip: you can list files produced for a given table using the DuckLake SQL helper, for example:
```sql
FROM ducklake_list_files('bucket_cat', 'table_foo');
```
when run via pipeline.sql_client(). This mirrors the check performed in the tests.

## How it works under the hood (selected details)

- Each connection loads the ducklake extension, attaches the catalog and storage with:
  ATTACH IF NOT EXISTS 'ducklake:<catalog>' AS <ducklake_name> (DATA_PATH '<storage_url>' ...)
  and then sets the search_path to the dlt dataset. See DuckLakeSqlClient.open_connection and build_attach_statement.
- Connections always detach the DuckLake cleanly on close to avoid catalog corruption.
- Insert vs Copy: if insert-values is selected, dlt uses INSERT VALUES; otherwise a COPY-backed job writes Parquet and reports remote_url (see DuckLakeClient.create_load_job and DuckLakeCopyJob).

## See also

- DuckDB destination (shared concepts, configuration and caveats): ./duckdb.md
- Source code for this destination:
  - dlt/destinations/impl/ducklake/ducklake.py
  - dlt/destinations/impl/ducklake/sql_client.py
  - dlt/destinations/impl/ducklake/configuration.py
  - dlt/destinations/impl/ducklake/factory.py

