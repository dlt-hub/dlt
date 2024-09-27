import os
import glob
from pathlib import Path
from urllib.parse import urlparse
import pytest
import gzip
from typing import List, Sequence, Tuple
from fsspec import AbstractFileSystem

from dlt.common import pendulum, json
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.data_writers import DataWriter
from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.storages import (
    LoadStorageConfiguration,
    FilesystemConfiguration,
    LoadPackageInfo,
    TPackageJobState,
    LoadStorage,
)
from dlt.common.storages import DataItemStorage, FileStorage
from dlt.common.storages.fsspec_filesystem import FileItem, FileItemDict
from dlt.common.storages.schema_storage import SchemaStorage
from dlt.common.typing import StrAny, TDataItems
from dlt.common.utils import uniq_id

from tests.common.utils import load_yml_case

TEST_SAMPLE_FILES = "tests/common/storages/samples"
MINIMALLY_EXPECTED_RELATIVE_PATHS = {
    "csv/freshman_kgs.csv",
    "csv/freshman_lbs.csv",
    "csv/mlb_players.csv",
    "csv/mlb_teams_2012.csv",
    "jsonl/mlb_players.jsonl",
    "met_csv/A801/A881_20230920.csv",
    "met_csv/A803/A803_20230919.csv",
    "met_csv/A803/A803_20230920.csv",
    "parquet/mlb_players.parquet",
    "gzip/taxi.csv.gz",
    "sample.txt",
}


@pytest.fixture
def load_storage() -> LoadStorage:
    C = resolve_configuration(LoadStorageConfiguration())
    s = LoadStorage(True, LoadStorage.ALL_SUPPORTED_FILE_FORMATS, C)
    return s


def glob_local_case(glob_filter: str) -> List[str]:
    all_files = [
        os.path.relpath(p, TEST_SAMPLE_FILES)
        for p in glob.glob(os.path.join(TEST_SAMPLE_FILES, glob_filter), recursive=True)
        if os.path.isfile(p)
    ]
    return [Path(p).as_posix() for p in all_files]


def assert_sample_files(
    all_file_items: List[FileItem],
    filesystem: AbstractFileSystem,
    config: FilesystemConfiguration,
    load_content: bool,
    glob_filter: str = "**",
) -> None:
    # sanity checks: all expected files are in the fixtures
    assert MINIMALLY_EXPECTED_RELATIVE_PATHS == set(glob_local_case("**"))
    # filter expected files by glob filter
    expected_relative_paths = glob_local_case(glob_filter)
    expected_file_names = [path.split("/")[-1] for path in expected_relative_paths]

    assert len(all_file_items) == len(expected_relative_paths)

    for item in all_file_items:
        # only accept file items we know
        assert item["relative_path"] in expected_relative_paths

        # is valid url
        file_url_parsed = urlparse(item["file_url"])
        assert isinstance(item["file_name"], str)
        assert item["file_name"] in expected_file_names
        assert file_url_parsed.path.endswith(item["file_name"])
        assert item["file_url"].startswith(config.protocol)
        assert item["file_url"].endswith(item["relative_path"])
        assert isinstance(item["mime_type"], str)
        assert isinstance(item["size_in_bytes"], int)
        assert isinstance(item["modification_date"], pendulum.DateTime)

        # create file dict
        file_dict = FileItemDict(item, config.credentials)

        try:
            # try to load using local filesystem
            content = filesystem.read_bytes(file_dict.local_file_path)
        except ValueError:
            content = filesystem.read_bytes(file_dict["file_url"])
        assert len(content) == item["size_in_bytes"]
        if load_content:
            item["file_content"] = content
        dict_content = file_dict.read_bytes()
        assert content == dict_content
        with file_dict.open() as f:
            # content will be decompressed for gzip encoding
            if item["encoding"] == "gzip":
                content = gzip.decompress(content)
            open_content = f.read()
            assert content == open_content
        # read via various readers
        if item["mime_type"] == "text/csv":
            # parse csv
            with file_dict.open(mode="rt") as f:
                from csv import DictReader

                # fieldnames below are not really correct but allow to load first 3 columns
                # even if first row does not have header names
                elements = list(DictReader(f, fieldnames=["A", "B", "C"]))
                assert len(elements) > 0
        if item["mime_type"] == "application/parquet":
            # verify it is a real parquet
            with file_dict.open() as f:
                parquet: bytes = f.read()
                assert parquet.startswith(b"PAR1")
        if item["mime_type"].startswith("text"):
            with file_dict.open(mode="rt") as f_txt:
                lines = f_txt.readlines()
                assert len(lines) >= 1
                assert isinstance(lines[0], str)
        if item["file_name"].endswith(".gz"):
            assert item["encoding"] == "gzip"


def write_temp_job_file(
    item_storage: DataItemStorage,
    file_storage: FileStorage,
    load_id: str,
    table_name: str,
    table: TTableSchemaColumns,
    file_id: str,
    rows: TDataItems,
) -> str:
    """Writes new file into new packages "new_jobs". Intended for testing"""
    file_name = (
        item_storage._get_data_item_path_template(load_id, None, table_name) % file_id
        + "."
        + item_storage.writer_spec.file_extension
    )
    mode = "wb" if item_storage.writer_spec.is_binary_format else "w"
    with file_storage.open_file(file_name, mode=mode) as f:
        writer = DataWriter.from_file_format(
            item_storage.writer_spec.file_format, item_storage.writer_spec.data_item_format, f
        )
        writer.write_all(table, rows)
        writer.close()
    return Path(file_name).name


def start_loading_files(
    s: LoadStorage, content: Sequence[StrAny], start_job: bool = True, file_count: int = 1
) -> Tuple[str, List[str]]:
    load_id = uniq_id()
    s.new_packages.create_package(load_id)
    # write test file
    file_names: List[str] = []
    for _ in range(0, file_count):
        item_storage = s.create_item_storage(
            DataWriter.writer_spec_from_file_format("jsonl", "object")
        )
        file_name = write_temp_job_file(
            item_storage, s.storage, load_id, "mock_table", None, uniq_id(), content
        )
        file_names.append(file_name)
    # write schema and schema update
    s.new_packages.save_schema(load_id, Schema("mock"))
    s.new_packages.save_schema_updates(load_id, {})
    s.commit_new_load_package(load_id)
    assert_package_info(s, load_id, "normalized", "new_jobs", jobs_count=file_count)
    if start_job:
        for file_name in file_names:
            s.normalized_packages.start_job(load_id, file_name)
            assert_package_info(s, load_id, "normalized", "started_jobs")
    return load_id, file_names


def start_loading_file(
    s: LoadStorage, content: Sequence[StrAny], start_job: bool = True
) -> Tuple[str, str]:
    load_id, file_names = start_loading_files(s, content, start_job)
    return load_id, file_names[0]


def assert_package_info(
    storage: LoadStorage,
    load_id: str,
    package_state: str,
    job_state: TPackageJobState,
    jobs_count: int = 1,
) -> LoadPackageInfo:
    package_info = storage.get_load_package_info(load_id)
    # make sure it is serializable
    json.dumps(package_info)
    # generate str
    str(package_info)
    package_info.asstr()
    package_info.asstr(verbosity=1)
    assert package_info.state == package_state
    assert package_info.schema_name == "mock"
    assert len(package_info.jobs[job_state]) == jobs_count
    if package_state == "normalized":
        assert package_info.completed_at is None
    else:
        assert (pendulum.now().diff(package_info.completed_at).seconds) < 2
    # get dict
    package_info.asdict()
    return package_info


def prepare_eth_import_folder(storage: SchemaStorage) -> Schema:
    eth_V9 = load_yml_case("schemas/eth/ethereum_schema_v9")
    # remove processing hints before installing as import schema
    # ethereum schema is a "dirty" schema with processing hints
    eth = Schema.from_dict(eth_V9, remove_processing_hints=True)
    storage._export_schema(eth, storage.config.import_schema_path)
    return eth
