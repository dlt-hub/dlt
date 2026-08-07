"""Tests for the mssql_arrow backend."""

from typing import Any, Generator, List, cast

import pytest

pytest.importorskip("pyarrow")

import pyarrow as pa
from unittest.mock import MagicMock, patch

from dlt.sources.sql_database.helpers import TABLE_LOADER_REGISTRY, BaseTableLoader


def test_mssql_arrow_backend_registered() -> None:
    import dlt.sources.sql_database.mssql_arrow as mod  # noqa: F401

    assert "mssql_arrow" in TABLE_LOADER_REGISTRY
    assert issubclass(TABLE_LOADER_REGISTRY["mssql_arrow"], BaseTableLoader)


def test_mssql_arrow_loader_class_exists() -> None:
    from dlt.sources.sql_database.mssql_arrow import MssqlArrowTableLoader
    from dlt.sources.sql_database.helpers import TableLoader

    assert issubclass(MssqlArrowTableLoader, TableLoader)


class ClosableArrowReader:
    """Fakes the mssql-python v1.13 `_ArrowReader`: iterable, idempotently closable,
    and usable as a context manager -- unlike a bare iterator, it can tell us whether
    (and when, relative to other cleanup) it was closed.
    """

    def __init__(self, batches: List[pa.RecordBatch], events: List[str] = None) -> None:
        self._batches = iter(batches)
        self.events = events if events is not None else []
        self.close_count = 0
        self.closed = False

    def __iter__(self) -> "ClosableArrowReader":
        return self

    def __next__(self) -> pa.RecordBatch:
        return next(self._batches)

    def close(self) -> None:
        self.close_count += 1
        self.closed = True
        self.events.append("reader.close")

    def __enter__(self) -> "ClosableArrowReader":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()


def _sample_batch() -> pa.RecordBatch:
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("val", pa.string())])
    return pa.record_batch({"id": [1, 2], "val": ["a", "b"]}, schema=schema)


def _make_loader(reader: Any, events: List[str]) -> Any:
    from dlt.sources.sql_database.mssql_arrow import MssqlArrowTableLoader

    mock_cursor = MagicMock()
    mock_cursor.arrow_reader.return_value = reader

    mock_result = MagicMock()
    mock_result.cursor = mock_cursor
    mock_result.close.side_effect = lambda: events.append("result.close")

    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value = mock_result

    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_conn

    mock_table = MagicMock()
    mock_table.name = "test_table"
    mock_table.c = {}

    return MssqlArrowTableLoader(
        engine=mock_engine,
        backend="mssql_arrow",  # type: ignore[arg-type]
        table=mock_table,
        columns={},
        chunk_size=100,
    )


def test_mssql_arrow_uses_arrow_reader() -> None:
    events: List[str] = []
    reader = ClosableArrowReader([_sample_batch()], events)
    loader = _make_loader(reader, events)

    with patch.object(loader, "make_query", return_value=MagicMock()):
        rows = list(loader.load_rows())

    assert len(rows) == 1
    assert isinstance(rows[0], pa.Table)
    assert rows[0].num_rows == 2
    assert reader.close_count == 1
    # reader is owned by the driver, result by SQLAlchemy: reader must go first
    assert events == ["reader.close", "result.close"]


def test_mssql_arrow_closes_reader_on_early_generator_close() -> None:
    events: List[str] = []
    reader = ClosableArrowReader([_sample_batch(), _sample_batch()], events)
    loader = _make_loader(reader, events)

    with patch.object(loader, "make_query", return_value=MagicMock()):
        gen = loader.load_rows()
        first = next(gen)
        gen.close()

    assert isinstance(first, pa.Table)
    assert reader.close_count == 1
    assert events == ["reader.close", "result.close"]


def test_mssql_arrow_closes_reader_when_conversion_raises() -> None:
    events: List[str] = []
    reader = ClosableArrowReader([_sample_batch()], events)
    loader = _make_loader(reader, events)

    with (
        patch.object(loader, "make_query", return_value=MagicMock()),
        patch(
            "dlt.common.libs.pyarrow.cast_connectorx_temporal_columns",
            side_effect=ValueError("conversion failed"),
        ),
        pytest.raises(ValueError, match="conversion failed"),
    ):
        list(loader.load_rows())

    assert reader.close_count == 1
    assert events == ["reader.close", "result.close"]


def test_mssql_arrow_reader_close_failure_does_not_mask_conversion_error() -> None:
    class FailingCloseReader(ClosableArrowReader):
        def close(self) -> None:
            super().close()
            raise RuntimeError("driver close failed")

    events: List[str] = []
    reader = FailingCloseReader([_sample_batch()], events)
    loader = _make_loader(reader, events)

    with (
        patch.object(loader, "make_query", return_value=MagicMock()),
        patch(
            "dlt.common.libs.pyarrow.cast_connectorx_temporal_columns",
            side_effect=ValueError("conversion failed"),
        ),
        pytest.raises(ValueError, match="conversion failed"),
    ):
        list(loader.load_rows())

    # the reader cleanup still ran (and failed) but never replaced the original error
    assert reader.close_count == 1
    assert events == ["reader.close", "result.close"]


def test_mssql_arrow_reader_close_failure_does_not_break_normal_exhaustion() -> None:
    class FailingCloseReader(ClosableArrowReader):
        def close(self) -> None:
            super().close()
            raise RuntimeError("driver close failed")

    events: List[str] = []
    reader = FailingCloseReader([_sample_batch()], events)
    loader = _make_loader(reader, events)

    with patch.object(loader, "make_query", return_value=MagicMock()):
        rows = list(loader.load_rows())

    assert len(rows) == 1
    assert reader.close_count == 1
    assert events == ["reader.close", "result.close"]


class FakeArrowReader:
    """Fakes the closable reader while enforcing the v1.13 ownership contract on its
    owning `FakeMssqlCursor`: the cursor refuses new queries until the reader closes.
    """

    def __init__(self, batches: List[pa.RecordBatch], owner: "FakeMssqlCursor") -> None:
        self._batches = iter(batches)
        self._owner = owner
        self.close_count = 0

    def __iter__(self) -> "FakeArrowReader":
        return self

    def __next__(self) -> pa.RecordBatch:
        return next(self._batches)

    def close(self) -> None:
        self.close_count += 1
        self._owner.reader_closed = True

    def __enter__(self) -> "FakeArrowReader":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()


class FakeMssqlCursor:
    """Models the DBAPI cursor mssql-python's arrow reader wraps: `execute()` raises
    while an open reader still owns it, and succeeds again once the reader is closed.
    """

    def __init__(self, batches: List[pa.RecordBatch]) -> None:
        self._batches = batches
        self.reader_closed = True
        self.execute_count = 0

    def arrow_reader(self, batch_size: int) -> FakeArrowReader:
        self.reader_closed = False
        return FakeArrowReader(self._batches, owner=self)

    def execute(self, query: str) -> "FakeMssqlCursor":
        if not self.reader_closed:
            raise RuntimeError("cursor is still owned by an open arrow reader")
        self.execute_count += 1
        return self


def test_mssql_arrow_early_stop_leaves_cursor_reusable() -> None:
    """Stops extraction after the first batch, then re-executes on the same cursor.

    A live MSSQL server (mssql-python driver) is not available in this test
    environment, so this exercises the documented v1.13 close contract -- cursor
    reuse after an early reader close -- against a fake cursor rather than a real
    connection. See the `mssql` marker for the existing live-db test infrastructure.
    """
    from dlt.sources.sql_database.mssql_arrow import MssqlArrowTableLoader

    cursor = FakeMssqlCursor([_sample_batch(), _sample_batch()])

    mock_result = MagicMock()
    mock_result.cursor = cursor

    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value = mock_result

    mock_engine = MagicMock()
    mock_engine.connect.return_value = mock_conn

    mock_table = MagicMock()
    mock_table.name = "test_table"
    mock_table.c = {}

    loader = MssqlArrowTableLoader(
        engine=mock_engine,
        backend="mssql_arrow",  # type: ignore[arg-type]
        table=mock_table,
        columns={},
        chunk_size=100,
    )

    with patch.object(loader, "make_query", return_value=MagicMock()):
        gen = cast(Generator[Any, None, None], loader.load_rows())
        next(gen)
        gen.close()

    assert cursor.reader_closed
    # the parent cursor is usable again for an unrelated query, per the v1.13 contract
    cursor.execute("SELECT 1")
    assert cursor.execute_count == 1
