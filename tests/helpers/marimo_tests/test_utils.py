import pickle
import json
import pathlib
import gzip
import unittest.mock

import pyarrow
import pyarrow.parquet
import pyarrow.csv
import pytest

import dlt.helpers.marimo.utils as marimo_utils


def test_load_pickle(tmp_path):
    file_path = tmp_path / "file.pickle"
    obj = ("foo", "bar")
    pickle.dump(obj, file_path.open("wb"))

    loaded_obj = marimo_utils._load_pickle(file_path)

    assert loaded_obj == obj


def test_load_json(tmp_path):
    file_path = tmp_path / "file.json"
    obj = {"foo": "bar"}
    json.dump(obj, file_path.open("w"))

    loaded_obj = marimo_utils._load_json(file_path)

    assert loaded_obj == obj


def test_load_jsonl(tmp_path):
    file_path = tmp_path / "file.jsonl"
    obj = [{"foo": "bar"}, {"foo": "baz"}]
    with file_path.open("w") as f:
        for record in obj:
            f.write(json.dumps(record) + "\n")

    loaded_obj = marimo_utils._load_jsonl(file_path)

    assert loaded_obj == obj


def test_load_parquet(tmp_path):
    file_path = tmp_path / "file.parquet"
    obj = pyarrow.table(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "score": [85.0, 90.5, 88.0]}
    )
    pyarrow.parquet.write_table(obj, file_path)

    loaded_obj = marimo_utils._load_parquet(file_path)

    assert loaded_obj == obj


def test_load_csv(tmp_path):
    file_path = tmp_path / "file.csv"
    obj = pyarrow.table(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "score": [85.0, 90.5, 88.0]}
    )
    pyarrow.csv.write_csv(obj, file_path)

    loaded_obj = marimo_utils._load_csv(file_path)

    assert loaded_obj == obj


def test_load_gzip(tmp_path):
    file_path = tmp_path / "file.gzip"
    obj = "foobar"
    with gzip.open(file_path, "w") as f:
        f.write(obj.encode())

    loaded_obj = marimo_utils._load_gzip(file_path)

    assert loaded_obj == obj


def test_load_inser_values_gzip(tmp_path):
    file_path = tmp_path / "file.gzip"
    obj = r"""
INSERT INTO {}("sku","name","type")
VALUES
(E'JAF-001',E'nutellaphone who dis?',E'jaffle',),
(E'JAF-002',E'doctor stew',E'jaffle'),
"""
    expected_obj = pyarrow.table(
        {
            "sku": ["JAF-001", "JAF-002"],
            "name": ["nutellaphone who dis?", "doctor stew"],
            "type": ["jaffle", "jaffle"],
        }
    )
    with gzip.open(file_path, "w") as f:
        f.write(obj.encode())

    loaded_obj = marimo_utils._load_insert_values_gzip(file_path)

    assert loaded_obj == expected_obj


@pytest.mark.parametrize(
    ("file_name", "expected_triggered_fn"),
    [
        ("file.pickle", "_load_pickle"),
        ("file.json", "_load_json"),
        ("file.jsonl", "_load_jsonl"),
        ("file.gzip", "_load_gzip"),
        ("file.gz", "_load_gzip"),
        ("file.insert_values.gzip", "_load_insert_values_gzip"),
        ("file.insert_values.gz", "_load_insert_values_gzip"),
        ("file.parquet", "_load_parquet"),
        ("file.csv", "_load_csv"),
        ("file.sql", "_load_raw_text"),
        ("file", "_load_raw_text"),
    ],
)
def test_load_file_dispatch(file_name: str, expected_triggered_fn: str):
    with unittest.mock.patch(f"dlt.helpers.marimo.utils.{expected_triggered_fn}") as mock_fn:
        SENTINEL = object()
        mock_fn.return_value = SENTINEL

        assert marimo_utils._load_file(pathlib.Path(file_name)) is SENTINEL
