import os
from typing import Any, Tuple
from unittest.mock import MagicMock

import pytest

from dlt.common.libs.pyarrow import pyarrow
from dlt.destinations._adbc_jobs import AdbcParquetCopyJob


def _write_parquet_with_row_groups(path: str, num_row_groups: int, rows_per_group: int) -> None:
    schema = pyarrow.schema([("id", pyarrow.int64())])
    with pyarrow.parquet.ParquetWriter(path, schema) as writer:
        for g in range(num_row_groups):
            tbl = pyarrow.table(
                {"id": list(range(g * rows_per_group, (g + 1) * rows_per_group))},
                schema=schema,
            )
            writer.write_table(tbl, row_group_size=rows_per_group)


# concrete subclass that bypasses RunnableLoadJob.__init__ so tests can drive run()
# without a real job client / filename parsing
class _StubAdbcParquetCopyJob(AdbcParquetCopyJob):
    def __init__(self, file_path: str, conn_cm: Any, table_name: str = "stub_table") -> None:
        self._file_path = file_path
        self._file_name = os.path.basename(file_path)
        self._conn_cm = conn_cm
        self._load_table = {"name": table_name}

    def _connect(self) -> Any:
        return self._conn_cm

    def _set_catalog_and_schema(self) -> Tuple[str, str]:
        return "cat", "sch"


def _make_mock_conn(rows_per_ingest: int) -> Tuple[MagicMock, MagicMock, MagicMock]:
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

    assert cur.adbc_ingest.call_count == 1
    assert conn.commit.call_count == 1


@pytest.mark.parametrize(
    "num_row_groups,rows_per_group",
    [(3, 4), (1, 10), (0, 0)],
    ids=["multi_rowgroup", "single_rowgroup", "empty"],
)
def test_per_rowgroup_ingest_calls_adbc_ingest_per_rowgroup(
    tmp_path, num_row_groups: int, rows_per_group: int
) -> None:
    file_path = str(tmp_path / "per_rg.parquet")
    _write_parquet_with_row_groups(file_path, num_row_groups, rows_per_group)

    conn_cm, conn, cur = _make_mock_conn(rows_per_ingest=rows_per_group)
    job = _StubAdbcParquetCopyJob(file_path, conn_cm)
    job._ingest_per_rowgroup = True

    job.run()

    # one ingest call per row-group bounds driver memory, but a single commit per file preserves atomicity
    assert cur.adbc_ingest.call_count == num_row_groups
    assert conn.commit.call_count == 1

    for call in cur.adbc_ingest.call_args_list:
        # each call gets a bounded pyarrow.Table (not an iterator) so driver memory stays capped
        assert isinstance(call.args[1], pyarrow.Table)
        assert call.kwargs.get("mode") == "append"
        assert call.kwargs.get("catalog_name") == "cat"
        assert call.kwargs.get("db_schema_name") == "sch"
