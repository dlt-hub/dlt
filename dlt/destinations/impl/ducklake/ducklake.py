"""
# DuckDB object hierarchy
Here are short definitions and relationships between DuckDB objects.
This should help disambiguate names used in Duckdb, DuckLake, and dlt.

TL;DR:
- scalar < column < table < schema (dataset) < database = catalog
- Typically, in duckdb, you have one catalog = one database = one file
- When using `ATTACH`, you're adding `Catalog` to your `Database`
    - Though if you do `SHOW ALL TABLES`, the result column "database" should be "catalog" to be precise

Hierarchy:
- A `Table` can have many `Column`
- A `Schema` can have many `Table`
- A `Database` can have many `Schema` (corresponds to dataset in dlt)
- A `Database` is a single physical file (e.g., `db.duckdb`)
- A `Database` has a single `Catalog`
- A `Catalog` is the internal metadata structure of everything found in the database
- Using `ATTACH` adds a `Catalog` to the

In dlt:
- dlt creates a duckdb `Database` per pipeline when using `dlt.pipeline(..., destination="duckdb")`
- dlt stores the data inside a `Schema` that matches the name of the `dlt.Dataset`
- when setting the pipeline destination to a specific duckdb `Database`, you can store multiple
`dlt.Dataset` inside the same instance (each with its own duckdb `Schema`).


# DuckLake object hierarchy

TL;DR:
- scalar < column < table < schema < snapshot < database = catalog

Hierarchy:
- A `Catalog` is an SQL database to store metadata
    - In duckdb terms, it's a duckdb `Database` that implements the duckdb `Catalog` for the DuckLake
- A `Catalog` has many Schemas (namespaces if you compare it to Iceberg) that correspond to dlt.Dataset
- A `Storage` is a file system or object store that can store parquet files
- A `Snapshot` references to the `Catalog` at a particular point in time
    - This places `Snapshot` at the top of the hierarchy because it scopes other constructs

Using the `ducklake` extension, the following command in duckdb

```sql
ATTACH 'ducklake:{catalog_database}' (DATA_PATH '{data_storage}');
```

adds the ducklake `Catalog` to your duckdb database
"""
from __future__ import annotations

import os
import pathlib
from packaging.version import Version
from typing import Iterable, List, Optional, Sequence

import dlt
from dlt.common import logger
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.client import LoadJob
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.metrics import LoadJobMetrics
from dlt.common.schema.typing import TColumnSchema

from dlt.destinations.impl.duckdb.duck import DuckDbClient, DuckDbCopyJob
from dlt.destinations.impl.ducklake.sql_client import DuckLakeSqlClient
from dlt.destinations.impl.ducklake.configuration import DuckLakeClientConfiguration
from dlt.destinations.insert_job_client import InsertValuesJobClient


class DuckLakeCopyJob(DuckDbCopyJob):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
        self._job_client: DuckLakeClient = None

    def metrics(self) -> Optional[LoadJobMetrics]:
        """Generate remote url metrics which point to the table in storage"""
        m = super().metrics()
        # job client not available before run_managed called
        if not self._job_client:
            return m
        # TODO: read location from catalog. ducklake supports customized table layouts
        return m._replace(
            remote_url=str(
                pathlib.Path().joinpath(
                    self._job_client.config.credentials.storage.bucket_url,
                    self._job_client.sql_client.dataset_name,
                    self.load_table_name,
                )
            )
        )


class DuckLakeClient(DuckDbClient):
    """Destination client to interact with a DuckLake

    A DuckLake has 3 components:
        - ducklake client: this is a `duckdb` instance with the `ducklake` extension
        - catalog: this is an SQL database storing metadata. It can be a duckdb instance
            (typically the ducklake client) or a remote database (sqlite, postgres, mysql)
        - storage: this is a filesystem where data is stored in files

    The dlt DuckLake destination gives access to the "ducklake client".
    You never have to manage the catalog and storage directly;
    this is done through the ducklake client.
    """

    def __init__(
        self,
        schema: dlt.Schema,
        config: DuckLakeClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)  # type: ignore[arg-type]
        self.config: DuckLakeClientConfiguration = config  # type: ignore[assignment]
        self.sql_client: DuckLakeSqlClient = DuckLakeSqlClient(
            dataset_name=config.normalize_dataset_name(schema),
            staging_dataset_name=config.normalize_staging_dataset_name(schema),
            credentials=config.credentials,
            capabilities=capabilities,
        )

    def initialize_storage(self, truncate_tables: Iterable[str] = None) -> None:
        super().initialize_storage(truncate_tables)
        # create local storage
        if self.config.credentials.storage.is_local_filesystem:
            os.makedirs(self.config.credentials.storage_url, exist_ok=True)

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        job = InsertValuesJobClient.create_load_job(self, table, file_path, load_id, restore)
        if not job:
            job = DuckLakeCopyJob(file_path)
        return job

    def _get_table_update_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        sql = super()._get_table_update_sql(table_name, new_columns, generate_alter)

        # TODO: evolve partitioning here. we should change the scheme if there's actual change vs.
        #  partition in schema
        if not generate_alter:
            partition_list = [
                self.sql_client.escape_column_name(c["name"])
                for c in new_columns
                if c.get("partition")
            ]

            if partition_list:
                import duckdb

                if Version(duckdb.__version__) < Version("1.4.0"):
                    logger.warning(
                        "Partitioning is disabled for duckdb < 1.4.0 due to malformed catalog"
                        " update statements."
                    )
                else:
                    qualified_name = self.sql_client.make_qualified_table_name(table_name)
                    sql.append(
                        f"ALTER TABLE {qualified_name} SET PARTITIONED BY ("
                        + ",".join(partition_list)
                        + ")"
                    )
        return sql
