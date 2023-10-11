from typing import List
from fsspec import AbstractFileSystem
import pandas
from pyarrow import parquet

from dlt.common import pendulum
from dlt.common.storages import FilesystemConfiguration
from dlt.common.storages.fsspec_filesystem import FileItem, FileItemDict


def assert_sample_files(all_file_items: List[FileItem], filesystem: AbstractFileSystem, config: FilesystemConfiguration, load_content: bool) -> None:
    for item in all_file_items:
        assert isinstance(item["file_name"], str)
        assert item["file_url"].endswith(item["file_name"])
        assert item["file_url"].startswith(config.protocol)
        assert isinstance(item["mime_type"], str)
        assert isinstance(item["size_in_bytes"], int)
        assert isinstance(item["modification_date"], pendulum.DateTime)
        content = filesystem.read_bytes(item["file_url"])
        assert len(content) == item["size_in_bytes"]
        if load_content:
            item["file_content"] = content

        # create file dict
        file_dict = FileItemDict(item, config.credentials)
        dict_content = file_dict.read_bytes()
        assert content == dict_content
        with file_dict.open() as f:
            assert content == f.read()
        # read via various readers
        if item["mime_type"] == "text/csv":
            with file_dict.open() as f:
                df = pandas.read_csv(f, header="infer")
                assert len(df.to_dict(orient="records")) > 0
        if item["mime_type"] == "application/parquet":
            with file_dict.open() as f:
                table = parquet.ParquetFile(f).read()
                assert len(table.to_pylist())
        if item["mime_type"].startswith("text"):
            with file_dict.open(mode="rt") as f_txt:
                lines = f_txt.readlines()
                assert len(lines) >= 1
                assert isinstance(lines[0], str)

    assert len(all_file_items) == 10
    assert set([item["file_name"] for item in all_file_items]) == {
        'csv/freshman_kgs.csv',
        'csv/freshman_lbs.csv',
        'csv/mlb_players.csv',
        'csv/mlb_teams_2012.csv',
        'jsonl/mlb_players.jsonl',
        'met_csv/A801/A881_20230920.csv',
        'met_csv/A803/A803_20230919.csv',
        'met_csv/A803/A803_20230920.csv',
        'parquet/mlb_players.parquet',
        'sample.txt'
    }