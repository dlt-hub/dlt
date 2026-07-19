"""Tests for the mssql_arrow backend."""

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


def test_mssql_arrow_uses_arrow_reader() -> None:
    from dlt.sources.sql_database.mssql_arrow import MssqlArrowTableLoader

    schema = pa.schema([pa.field("id", pa.int64()), pa.field("val", pa.string())])
    batch = pa.record_batch({"id": [1, 2], "val": ["a", "b"]}, schema=schema)

    mock_cursor = MagicMock()
    mock_cursor.arrow_reader.return_value = iter([batch])

    mock_result = MagicMock()
    mock_result.cursor = mock_cursor

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
        backend="mssql_arrow",
        table=mock_table,
        columns={},
        chunk_size=100,
    )

    with patch.object(loader, "make_query", return_value=MagicMock()):
        rows = list(loader.load_rows())

    mock_cursor.arrow_reader.assert_called_once_with(batch_size=100)
    assert len(rows) == 1
    assert isinstance(rows[0], pa.Table)
    assert rows[0].num_rows == 2


def test_mssql_arrow_falls_back_without_arrow_reader() -> None:
    from dlt.sources.sql_database.mssql_arrow import MssqlArrowTableLoader

    mock_cursor = MagicMock(spec=[])  # no arrow_reader attribute

    mock_result = MagicMock()
    mock_result.cursor = mock_cursor

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
        backend="mssql_arrow",
        table=mock_table,
        columns={},
        chunk_size=100,
    )

    sentinel = object()

    with patch.object(loader, "make_query", return_value=MagicMock()), patch.object(
        loader, "_convert_result", return_value=iter([sentinel])
    ) as mock_convert:
        rows = list(loader.load_rows())

    mock_convert.assert_called_once()
    assert rows == [sentinel]
