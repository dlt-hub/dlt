import os
from typing import Any, Tuple
from unittest.mock import MagicMock

import pytest

from dlt.common.libs.pyarrow import pyarrow
from dlt.destinations._adbc_jobs import AdbcParquetCopyJob


def _write_parquet_with_row_groups(path: str, num_row_groups: int, rows_per_group: int) -> None:
    """Write a parquet file with exactly `num_row_groups` row groups of equal size."""
    schema = pyarrow.schema([("id", pyarrow.int64())])
    with pyarrow.parquet.ParquetWriter(path, schema) as writer:
        for g in range(num_row_groups):
            tbl = pyarrow.table(
                {"id": list(range(g * rows_per_group, (g + 1) * rows_per_group))},
                schema=schema,
            )
            writer.write_table(tbl, row_group_size=rows_per_group)


class _StubAdbcParquetCopyJob(AdbcParquetCopyJob):
    """Minimal concrete subclass to exercise the base `run()` contract.

    Skips the heavy `RunnableLoadJob.__init__` (which parses filenames and
    requires a job client) so the test can focus on the ingest/commit pattern.
    """

    def __init__(self, file_path: str, conn_cm: Any, table_name: str = "stub_table") -> None:
        # bypass parent init to avoid pulling in the loader/file-name parsing surface
        self._file_path = file_path
        self._file_name = os.path.basename(file_path)
        self._conn_cm = conn_cm
        self._load_table = {"name": table_name}

    def _connect(self) -> Any:
        return self._conn_cm

    def _set_catalog_and_schema(self) -> Tuple[str, str]:
        return "cat", "sch"


def _make_mock_conn(rows_per_ingest: int) -> Tuple[MagicMock, MagicMock, MagicMock]:
    """Build a context-manager-shaped mock connection.

    Returns the outer `with`-target, the inner connection mock (for asserting
    `commit()` calls), and the cursor mock (for asserting `adbc_ingest` calls).
    """
    cur = MagicMock()
    cur.adbc_ingest = MagicMock(return_value=rows_per_ingest)
    cur_cm = MagicMock()
    cur_cm.__enter__ = MagicMock(return_value=cur)
    cur_cm.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.commit = MagicMock()
    conn.cursor = MagicMock(return_value=cur_cm)

    conn_cm = MagicMock()
    conn_cm.__enter__ = MagicMock(return_value=conn)
    conn_cm.__exit__ = MagicMock(return_value=False)

    return conn_cm, conn, cur


def test_default_ingest_streams_all_batches_in_single_call(tmp_path) -> None:
    file_path = str(tmp_path / "default.parquet")
    _write_parquet_with_row_groups(file_path, num_row_groups=3, rows_per_group=4)

    conn_cm, conn, cur = _make_mock_conn(rows_per_ingest=12)
    job = _StubAdbcParquetCopyJob(file_path, conn_cm)

    job.run()

    # default behaviour: one `adbc_ingest` call covering every batch
    assert cur.adbc_ingest.call_count == 1
    assert conn.commit.call_count == 1


def test_per_rowgroup_ingest_calls_adbc_ingest_per_rowgroup(tmp_path) -> None:
    file_path = str(tmp_path / "per_rg.parquet")
    _write_parquet_with_row_groups(file_path, num_row_groups=3, rows_per_group=4)

    conn_cm, conn, cur = _make_mock_conn(rows_per_ingest=4)
    job = _StubAdbcParquetCopyJob(file_path, conn_cm)
    job._ingest_per_rowgroup = True

    job.run()

    # one ingest call per row-group (bounds driver memory), but a single
    # commit for the whole file so atomicity is preserved
    assert cur.adbc_ingest.call_count == 3
    assert conn.commit.call_count == 1

    # every ingest call must receive a pyarrow.Table, not an iterator,
    # because the driver memory bound only holds when the call is bounded too
    for call in cur.adbc_ingest.call_args_list:
        positional = call.args
        assert isinstance(positional[1], pyarrow.Table)
        # mode must stay `append` so repeated calls accumulate rows
        assert call.kwargs.get("mode") == "append"
        assert call.kwargs.get("catalog_name") == "cat"
        assert call.kwargs.get("db_schema_name") == "sch"


def test_per_rowgroup_ingest_handles_single_rowgroup(tmp_path) -> None:
    file_path = str(tmp_path / "single_rg.parquet")
    _write_parquet_with_row_groups(file_path, num_row_groups=1, rows_per_group=10)

    conn_cm, conn, cur = _make_mock_conn(rows_per_ingest=10)
    job = _StubAdbcParquetCopyJob(file_path, conn_cm)
    job._ingest_per_rowgroup = True

    job.run()

    # single row-group still goes through the per-rowgroup path: one ingest, one commit
    assert cur.adbc_ingest.call_count == 1
    assert conn.commit.call_count == 1


def test_per_rowgroup_ingest_drops_none_catalog_and_schema(tmp_path) -> None:
    file_path = str(tmp_path / "none_cat.parquet")
    _write_parquet_with_row_groups(file_path, num_row_groups=2, rows_per_group=2)

    conn_cm, _, cur = _make_mock_conn(rows_per_ingest=2)
    job = _StubAdbcParquetCopyJob(file_path, conn_cm)
    job._ingest_per_rowgroup = True
    # simulate a destination that disables both catalog and schema
    job._set_catalog_and_schema = lambda: (None, None)  # type: ignore[method-assign]

    job.run()

    # `without_none` must strip both kwargs so the driver doesn't receive Nones
    for call in cur.adbc_ingest.call_args_list:
        assert "catalog_name" not in call.kwargs
        assert "db_schema_name" not in call.kwargs


def test_mssql_copy_job_opts_into_per_rowgroup_ingest() -> None:
    """`MssqlParquetCopyJob` must set the per-rowgroup flag so the bug fix sticks."""
    pytest.importorskip("pyodbc")
    from dlt.destinations.impl.mssql.mssql import MssqlParquetCopyJob

    assert MssqlParquetCopyJob._ingest_per_rowgroup is True


def test_postgres_copy_job_keeps_streaming_ingest() -> None:
    """sibling drivers stay on the streaming path — fix must not regress them."""
    pytest.importorskip("psycopg2")
    from dlt.destinations.impl.postgres.postgres import PostgresParquetCopyJob

    assert PostgresParquetCopyJob._ingest_per_rowgroup is False
