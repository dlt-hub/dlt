import pytest
from typing import List, Sequence, Tuple
from fsspec import AbstractFileSystem

from dlt.common import pendulum, json
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.schema import Schema
from dlt.common.storages import (
    LoadStorageConfiguration,
    FilesystemConfiguration,
    LoadPackageInfo,
    TJobState,
    LoadStorage,
)
from dlt.common.storages.fsspec_filesystem import FileItem, FileItemDict
from dlt.common.typing import StrAny
from dlt.common.utils import uniq_id


@pytest.fixture
def load_storage() -> LoadStorage:
    C = resolve_configuration(LoadStorageConfiguration())
    s = LoadStorage(True, "jsonl", LoadStorage.ALL_SUPPORTED_FILE_FORMATS, C)
    return s


def assert_sample_files(
    all_file_items: List[FileItem],
    filesystem: AbstractFileSystem,
    config: FilesystemConfiguration,
    load_content: bool,
) -> None:
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
            # parse csv
            with file_dict.open(mode="rt") as f:
                from csv import DictReader

                elements = list(DictReader(f))
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

    assert len(all_file_items) >= 10
    assert set([item["file_name"] for item in all_file_items]) >= {
        "csv/freshman_kgs.csv",
        "csv/freshman_lbs.csv",
        "csv/mlb_players.csv",
        "csv/mlb_teams_2012.csv",
        "jsonl/mlb_players.jsonl",
        "met_csv/A801/A881_20230920.csv",
        "met_csv/A803/A803_20230919.csv",
        "met_csv/A803/A803_20230920.csv",
        "parquet/mlb_players.parquet",
        "sample.txt",
    }


def start_loading_file(
    s: LoadStorage, content: Sequence[StrAny], start_job: bool = True
) -> Tuple[str, str]:
    load_id = uniq_id()
    s.new_packages.create_package(load_id)
    # write test file
    file_name = s._write_temp_job_file(load_id, "mock_table", None, uniq_id(), content)
    # write schema and schema update
    s.new_packages.save_schema(load_id, Schema("mock"))
    s.new_packages.save_schema_updates(load_id, {})
    s.commit_new_load_package(load_id)
    assert_package_info(s, load_id, "normalized", "new_jobs")
    if start_job:
        s.normalized_packages.start_job(load_id, file_name)
        assert_package_info(s, load_id, "normalized", "started_jobs")
    return load_id, file_name


def assert_package_info(
    storage: LoadStorage,
    load_id: str,
    package_state: str,
    job_state: TJobState,
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
        assert (pendulum.now() - package_info.completed_at).seconds < 2
    # get dict
    package_info.asdict()
    return package_info
