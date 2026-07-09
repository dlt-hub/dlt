"""Query hotdata managed databases using the DuckDB SQL client.

The hotdata API exposes no SQL endpoint that duckdb can scan directly.
Tables are fetched as Arrow through the hotdata SDK, snapshotted to local
parquet files, and exposed as duckdb views. This makes hotdata compatible
with the `dlt.Dataset` interface.
"""
from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import Optional, Tuple, TYPE_CHECKING

from dlt.common.schema.schema import Schema
from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.destinations.impl.duckdb.sql_client import WithTableScanners
from dlt.destinations.impl.hotdata.contracts import TableContract
from dlt.destinations.impl.hotdata.errors import HotdataTerminalError, HotdataTransientError
from dlt.destinations.impl.hotdata.hotdata import _hotdata_api
from dlt.destinations.impl.hotdata.parquet import write_table_parquet

if TYPE_CHECKING:
    from dlt.common.destination.typing import PreparedTableSchema
    from dlt.destinations.impl.hotdata.hotdata import HotdataClient


class HotdataSqlClient(WithTableScanners):
    def __init__(self, hotdata_client: HotdataClient) -> None:
        self.hotdata_client = hotdata_client
        self._snapshot_dir = os.path.join(tempfile.gettempdir(), "hotdata_snapshots_" + uniq_id())
        super().__init__(
            remote_client=hotdata_client,
            dataset_name=hotdata_client.config.schema,
        )

    def can_create_view(self, table_schema: PreparedTableSchema) -> bool:
        return True

    def should_replace_view(self, view_name: str, table_schema: PreparedTableSchema) -> bool:
        return self.hotdata_client.config.always_refresh_views

    def create_view_select(
        self, table_schema: PreparedTableSchema, schema: Schema = None
    ) -> Optional[Tuple[str, str]]:
        config = self.hotdata_client.config
        contract = TableContract.from_table_schema(
            table_schema, database_name=config.database_name, schema=config.schema
        )
        snapshot_path = os.path.join(self._snapshot_dir, f"{contract.table_name}.parquet")
        # runs on every query before the existing-view short circuit, so snapshots must be cached
        if config.always_refresh_views or not os.path.exists(snapshot_path):
            with _hotdata_api(config) as api:
                arrow_table = api.fetch_table(
                    database=contract.database_name,
                    schema=contract.schema,
                    table=contract.table_name,
                )
            if arrow_table is not None:
                os.makedirs(self._snapshot_dir, exist_ok=True)
                write_table_parquet(arrow_table, snapshot_path)
            elif not os.path.exists(snapshot_path):
                return None
        return (
            contract.qualified_target,
            f"SELECT * FROM read_parquet('{Path(snapshot_path).as_posix()}')",
        )

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if isinstance(ex, KeyError):
            return DatabaseUndefinedRelation(ex)
        if isinstance(ex, HotdataTransientError):
            return DatabaseTransientException(ex)
        if isinstance(ex, HotdataTerminalError):
            return DatabaseTerminalException(ex)
        return super()._make_database_exception(ex)

    def __del__(self) -> None:
        if getattr(self, "_snapshot_dir", None):
            shutil.rmtree(self._snapshot_dir, ignore_errors=True)
        super().__del__()
