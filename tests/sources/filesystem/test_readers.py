import pathlib
from typing import Any, Iterator

import pytest
import pandas as pd
import pyarrow
from fsspec import AbstractFileSystem

from dlt.common import pendulum, json
from dlt.common.storages import fsspec_filesystem
from dlt.common.storages.fsspec_filesystem import FileItem
from dlt.sources.filesystem import FileItemDict
from dlt.sources.filesystem.readers import _read_csv, _read_csv_duckdb, _read_jsonl, _read_parquet


@pytest.fixture(scope="module")
def data() -> list[dict[str, Any]]:
    return [
        {"id": 1, "name": "Al"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charle"},
        {"id": 4, "name": "Dave"},
        {"id": 5, "name": "Eve"},
    ]


def _fsspec_client(tmp_path: pathlib.Path) -> AbstractFileSystem:
    client, _ = fsspec_filesystem(
        protocol=str(tmp_path), credentials=None, kwargs={}, client_kwargs={}
    )
    return client


def _create_parquet_file(data: list[dict[str, Any]], tmp_path: pathlib.Path) -> FileItemDict:
    file_name = "data.parquet"
    full_file_path = tmp_path / file_name

    df = pd.DataFrame(data)
    df.to_parquet(full_file_path, engine="pyarrow")

    file_item = FileItem(
        file_name=file_name,
        relative_path=file_name,
        file_url=full_file_path.as_uri(),
        mime_type="application/parquet",
        modification_date=pendulum.DateTime(2025, 1, 1, 0, 0, 0, 0),
        size_in_bytes=111,
    )

    return FileItemDict(mapping=file_item, fsspec=_fsspec_client(tmp_path))


def _create_csv_file(data: list[dict[str, Any]], tmp_path: pathlib.Path) -> FileItemDict:
    file_name = "data.csv"
    full_file_path = tmp_path / file_name

    df = pd.DataFrame(data)
    df.to_csv(full_file_path, index=False)

    file_item = FileItem(
        file_name=file_name,
        relative_path=file_name,
        file_url=full_file_path.as_uri(),
        mime_type="text/csv",
        modification_date=pendulum.DateTime(2025, 1, 1, 0, 0, 0, 0),
        size_in_bytes=111,
    )
    return FileItemDict(mapping=file_item, fsspec=_fsspec_client(tmp_path))


def _create_jsonl_file(data: list[dict[str, Any]], tmp_path: pathlib.Path) -> FileItemDict:
    file_name = "data.jsonl"
    full_file_path = tmp_path / file_name

    with open(full_file_path, "w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item) + "\n")

    file_item = FileItem(
        file_name=file_name,
        relative_path=file_name,
        file_url=full_file_path.as_uri(),
        mime_type="text/jsonl",
        modification_date=pendulum.DateTime(2025, 1, 1, 0, 0, 0, 0),
        size_in_bytes=111,
    )

    return FileItemDict(mapping=file_item, fsspec=_fsspec_client(tmp_path))


# TODO rewrite the following tests as a parameterized test once `read_` functions
# have a unified interface
# see discussion for ibis: https://github.com/ibis-project/ibis/issues/11459
# see discussion for narwhals: https://github.com/narwhals-dev/narwhals/issues/2930
def test_read_parquet(tmp_path: pathlib.Path, data: list[dict[str, Any]]) -> None:
    file_ = _create_parquet_file(data=data, tmp_path=tmp_path)
    iterator = _read_parquet([file_])
    read_data = list(iterator)

    assert isinstance(iterator, Iterator)
    assert isinstance(read_data, list)  # list of batches
    assert isinstance(read_data[0], list)  # batch of records
    assert isinstance(read_data[0][0], dict)  # record
    assert read_data == [data]


def test_read_parquet_use_pyarrow(tmp_path: pathlib.Path, data: list[dict[str, Any]]) -> None:
    file_ = _create_parquet_file(data=data, tmp_path=tmp_path)
    iterator = _read_parquet([file_], use_pyarrow=True)
    read_data = list(iterator)

    assert isinstance(iterator, Iterator)
    assert isinstance(read_data, list)  # list of batches
    assert isinstance(read_data[0], pyarrow.RecordBatch)  # batch of records
    assert isinstance(read_data[0][0], pyarrow.Array)  # column
    assert read_data == [pyarrow.RecordBatch.from_pylist(data)]


def test_read_csv(tmp_path: pathlib.Path, data: list[dict[str, Any]]) -> None:
    file_ = _create_csv_file(data=data, tmp_path=tmp_path)
    iterator = _read_csv([file_])
    read_data = list(iterator)

    assert isinstance(iterator, Iterator)
    assert isinstance(read_data, list)  # list of batches
    assert isinstance(read_data[0], list)  # batch of records
    assert isinstance(read_data[0][0], dict)  # record
    assert read_data == [data]


def test_read_jsonl(tmp_path: pathlib.Path, data: list[dict[str, Any]]) -> None:
    file_ = _create_jsonl_file(data=data, tmp_path=tmp_path)
    iterator = _read_jsonl([file_])
    read_data = list(iterator)

    assert isinstance(iterator, Iterator)
    assert isinstance(read_data, list)  # list of batches
    assert isinstance(read_data[0], list)  # batch of records
    assert isinstance(read_data[0][0], dict)  # record
    assert read_data == [data]


def test_read_csv_duckdb(tmp_path: pathlib.Path, data: list[dict[str, Any]]) -> None:
    file_ = _create_csv_file(data=data, tmp_path=tmp_path)
    iterator = _read_csv_duckdb([file_])
    read_data = list(iterator)

    assert isinstance(iterator, Iterator)
    assert isinstance(read_data, list)  # list of batches
    assert isinstance(read_data[0], list)  # batch of records
    assert isinstance(read_data[0][0], dict)  # record
    assert read_data == [data]


def test_read_csv_duckdb_use_pyarrow(tmp_path: pathlib.Path, data: list[dict[str, Any]]) -> None:
    file_ = _create_csv_file(data=data, tmp_path=tmp_path)
    iterator = _read_csv_duckdb([file_], use_pyarrow=True)
    read_data = list(iterator)

    assert isinstance(iterator, Iterator)
    assert isinstance(read_data, list)  # list of batches
    assert isinstance(read_data[0], pyarrow.RecordBatch)  # batch of records
    assert isinstance(read_data[0][0], pyarrow.Array)  # column
    assert read_data == [pyarrow.RecordBatch.from_pylist(data)]
