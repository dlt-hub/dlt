"""Tests for the mssql_arrow backend."""

import contextlib
from typing import Any, Generator, List, Optional, Tuple, cast

import pytest

pytest.importorskip("pyarrow")

import pyarrow as pa
from unittest.mock import MagicMock, patch

from dlt.sources.sql_database.helpers import TABLE_LOADER_REGISTRY, BaseTableLoader
from dlt.sources.sql_database.mssql_arrow import MssqlArrowTableLoader


def test_mssql_arrow_backend_registered() -> None:
    import dlt.sources.sql_database.mssql_arrow as mod  # noqa: F401

    assert "mssql_arrow" in TABLE_LOADER_REGISTRY
    assert issubclass(TABLE_LOADER_REGISTRY["mssql_arrow"], BaseTableLoader)


def test_mssql_arrow_loader_class_exists() -> None:
    from dlt.sources.sql_database.helpers import TableLoader

    assert issubclass(MssqlArrowTableLoader, TableLoader)


class ClosableArrowReader:
    """Fakes the mssql-python v1.13 `_ArrowReader`: iterable and closable -- unlike a bare
    iterator, it can tell us whether (and when, relative to other cleanup) it was closed.
    """

    def __init__(self, batches: List[pa.RecordBatch], events: List[str] = None) -> None:
        self._batches = iter(batches)
        self.events = events if events is not None else []
        self.close_count = 0

    def __iter__(self) -> "ClosableArrowReader":
        return self

    def __next__(self) -> pa.RecordBatch:
        return next(self._batches)

    def close(self) -> None:
        self.close_count += 1
        self.events.append("reader.close")


class FailingCloseReader(ClosableArrowReader):
    """A reader whose `close()` always raises, to probe cleanup-failure handling."""

    def close(self) -> None:
        super().close()
        raise RuntimeError("driver close failed")


def _sample_batch() -> pa.RecordBatch:
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("val", pa.string())])
    return pa.record_batch({"id": [1, 2], "val": ["a", "b"]}, schema=schema)


def _make_loader(reader: Any, events: List[str]) -> Tuple[MssqlArrowTableLoader, MagicMock]:
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

    loader = MssqlArrowTableLoader(
        engine=mock_engine,
        backend="mssql_arrow",  # type: ignore[arg-type]
        table=mock_table,
        columns={},
        chunk_size=100,
    )
    return loader, mock_cursor


def test_mssql_arrow_uses_arrow_reader() -> None:
    events: List[str] = []
    reader = ClosableArrowReader([_sample_batch()], events)
    loader, mock_cursor = _make_loader(reader, events)

    with patch.object(loader, "make_query", return_value=MagicMock()):
        rows = list(loader.load_rows())

    mock_cursor.arrow_reader.assert_called_once_with(batch_size=100)
    assert len(rows) == 1
    assert isinstance(rows[0], pa.Table)
    assert rows[0].num_rows == 2
    assert reader.close_count == 1
    # reader is owned by the driver, result by SQLAlchemy: reader must go first
    assert events == ["reader.close", "result.close"]


def test_mssql_arrow_closes_reader_on_early_generator_close() -> None:
    events: List[str] = []
    reader = ClosableArrowReader([_sample_batch(), _sample_batch()], events)
    loader, _ = _make_loader(reader, events)

    with patch.object(loader, "make_query", return_value=MagicMock()):
        gen = cast(Generator[Any, None, None], loader.load_rows())
        first = next(gen)
        gen.close()

    assert isinstance(first, pa.Table)
    assert reader.close_count == 1
    assert events == ["reader.close", "result.close"]


def test_mssql_arrow_closes_reader_when_conversion_raises() -> None:
    events: List[str] = []
    reader = ClosableArrowReader([_sample_batch()], events)
    loader, _ = _make_loader(reader, events)

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


@pytest.mark.parametrize(
    "conversion_error,expected_error",
    [
        pytest.param(ValueError("conversion failed"), ValueError, id="prior-error-wins"),
        pytest.param(None, RuntimeError, id="close-failure-propagates-without-prior-error"),
    ],
)
def test_mssql_arrow_reader_close_failure(
    conversion_error: Optional[BaseException], expected_error: type
) -> None:
    events: List[str] = []
    reader = FailingCloseReader([_sample_batch()], events)
    loader, _ = _make_loader(reader, events)

    convert_patch = (
        patch(
            "dlt.common.libs.pyarrow.cast_connectorx_temporal_columns",
            side_effect=conversion_error,
        )
        if conversion_error is not None
        else contextlib.nullcontext()
    )
    with (
        patch.object(loader, "make_query", return_value=MagicMock()),
        convert_patch,
        pytest.raises(expected_error),
    ):
        list(loader.load_rows())

    # cleanup still ran (and failed); only a prior error is protected from being masked
    assert reader.close_count == 1
    assert events == ["reader.close", "result.close"]
