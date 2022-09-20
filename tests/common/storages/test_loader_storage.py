import os
import pytest
from pathlib import Path
from typing import Sequence, Tuple

from dlt.common import sleep
from dlt.common.schema import Schema
from dlt.common.storages.load_storage import LoadStorage, TParsedJobFileName
from dlt.common.configuration import LoadVolumeConfiguration, make_configuration
from dlt.common.storages.exceptions import NoMigrationPathException
from dlt.common.typing import StrAny
from dlt.common.utils import uniq_id

from tests.utils import TEST_STORAGE_ROOT, write_version, autouse_test_storage


@pytest.fixture
def storage() -> LoadStorage:
    C = make_configuration(LoadVolumeConfiguration, LoadVolumeConfiguration)
    s = LoadStorage(True, C, "jsonl", LoadStorage.ALL_SUPPORTED_FILE_FORMATS)
    return s


def test_complete_successful_package(storage: LoadStorage) -> None:
    # should delete package in full
    storage.delete_completed_jobs = True
    load_id, file_name = start_loading_file(storage, [{"content": "a"}, {"content": "b"}])
    assert storage.storage.has_folder(storage.get_package_path(load_id))
    storage.complete_job(load_id, file_name)
    storage.complete_load_package(load_id)
    # deleted from loading
    assert not storage.storage.has_folder(storage.get_package_path(load_id))
    # deleted from package
    assert not storage.storage.has_folder(storage.get_completed_package_path(load_id))

    # do not delete completed jobs
    storage.delete_completed_jobs = False
    load_id, file_name = start_loading_file(storage, [{"content": "a"}, {"content": "b"}])
    storage.complete_job(load_id, file_name)
    storage.complete_load_package(load_id)
    # deleted from loading
    assert not storage.storage.has_folder(storage.get_package_path(load_id))
    # has load preserved
    assert storage.storage.has_folder(storage.get_completed_package_path(load_id))


def test_complete_package_failed_jobs(storage: LoadStorage) -> None:
    # loads with failed jobs are always persisted
    storage.delete_completed_jobs = True
    load_id, file_name = start_loading_file(storage, [{"content": "a"}, {"content": "b"}])
    assert storage.storage.has_folder(storage.get_package_path(load_id))
    storage.fail_job(load_id, file_name, "EXCEPTION")
    storage.complete_load_package(load_id)
    # deleted from loading
    assert not storage.storage.has_folder(storage.get_package_path(load_id))
    # present in completed loads folder
    assert storage.storage.has_folder(storage.get_completed_package_path(load_id))


def test_save_load_schema(storage: LoadStorage) -> None:
    # mock schema version to some random number so we know we load what we save
    schema = Schema("event")
    schema._stored_version = 762171

    storage.create_temp_load_package("copy")
    saved_file_name = storage.save_temp_schema(schema, "copy")
    assert saved_file_name.endswith(os.path.join(storage.storage.storage_path, "copy", LoadStorage.SCHEMA_FILE_NAME))
    assert storage.storage.has_file(os.path.join("copy",LoadStorage.SCHEMA_FILE_NAME))
    schema_copy = storage.load_temp_schema("copy")
    assert schema.stored_version == schema_copy.stored_version


def test_job_elapsed_time_seconds(storage: LoadStorage) -> None:
    load_id, fn = start_loading_file(storage, "test file")
    fp = storage.storage.make_full_path(storage._get_job_file_path(load_id, "started_jobs", fn))
    elapsed = storage.job_elapsed_time_seconds(fp)
    sleep(0.3)
    # do not touch file
    elapsed_2 = storage.job_elapsed_time_seconds(fp)
    assert elapsed_2 - elapsed >= 0.3
    # rename the file
    fp = storage.retry_job(load_id, fn)
    # retry_job increases retry number in file name so the line below does not work
    # fp = storage.storage._make_path(storage._get_job_file_path(load_id, "new_jobs", fn))
    elapsed_2 = storage.job_elapsed_time_seconds(fp)
    # it should keep its mod original date after rename
    assert elapsed_2 - elapsed >= 0.3


def test_retry_job(storage: LoadStorage) -> None:
    load_id, fn = start_loading_file(storage, "test file")
    job_fn_t = LoadStorage.parse_job_file_name(fn)
    assert job_fn_t.table_name == "mock_table"
    assert job_fn_t.retry_count == 0
    # now retry
    new_fp = storage.retry_job(load_id, fn)
    assert LoadStorage.parse_job_file_name(new_fp).retry_count == 1
    # try again
    fn = Path(new_fp).name
    storage.start_job(load_id, fn)
    new_fp = storage.retry_job(load_id, fn)
    assert LoadStorage.parse_job_file_name(new_fp).retry_count == 2


def test_build_parse_job_path(storage: LoadStorage) -> None:
    file_id = uniq_id(5)
    f_n_t = TParsedJobFileName("test_table", file_id, 0, "jsonl")
    job_f_n = storage.build_job_file_name(f_n_t.table_name, file_id, 0)
    # test the exact representation but we should probably not test for that
    assert job_f_n == f"test_table.{file_id}.0.jsonl"
    assert LoadStorage.parse_job_file_name(job_f_n) == f_n_t
    # also parses full paths correctly
    assert LoadStorage.parse_job_file_name("load_id/" + job_f_n) == f_n_t

    # parts cannot contain dots
    with pytest.raises(ValueError):
        storage.build_job_file_name("test.table", file_id, 0)
        storage.build_job_file_name("test_table", "f.id", 0)

    # parsing requires 4 parts and retry count
    with pytest.raises(ValueError):
        LoadStorage.parse_job_file_name(job_f_n + ".more")
        LoadStorage.parse_job_file_name("tab.id.wrong_retry.jsonl")
        # must know the file format
        LoadStorage.parse_job_file_name("tab.id.300.avr")


def test_process_schema_update(storage: LoadStorage) -> None:
    with pytest.raises(FileNotFoundError):
        storage.begin_schema_update("load_id")
    load_id, fn = start_loading_file(storage, "test file")
    assert storage.begin_schema_update(load_id) == [{}]
    assert storage.begin_schema_update(load_id) == [{}]
    storage.commit_schema_update(load_id)
    with pytest.raises(FileNotFoundError):
        storage.commit_schema_update(load_id)
    assert storage.begin_schema_update(load_id) is None
    # processed file exists
    assert storage.storage.has_file(os.path.join(storage.get_package_path(load_id), LoadStorage.PROCESSED_SCHEMA_UPDATES_FILE_NAME))


def test_full_migration_path() -> None:
    # create directory structure
    s = LoadStorage(True, LoadVolumeConfiguration, "jsonl", LoadStorage.ALL_SUPPORTED_FILE_FORMATS)
    # overwrite known initial version
    write_version(s.storage, "1.0.0")
    # must be able to migrate to current version
    s = LoadStorage(False, LoadVolumeConfiguration, "jsonl", LoadStorage.ALL_SUPPORTED_FILE_FORMATS)
    assert s.version == LoadStorage.STORAGE_VERSION


def test_unknown_migration_path() -> None:
    # create directory structure
    s = LoadStorage(True, LoadVolumeConfiguration, "jsonl", LoadStorage.ALL_SUPPORTED_FILE_FORMATS)
    # overwrite known initial version
    write_version(s.storage, "10.0.0")
    # must be able to migrate to current version
    with pytest.raises(NoMigrationPathException):
        LoadStorage(False, LoadVolumeConfiguration, "jsonl", LoadStorage.ALL_SUPPORTED_FILE_FORMATS)


def start_loading_file(s: LoadStorage, content: Sequence[StrAny]) -> Tuple[str, str]:
    load_id = uniq_id()
    s.create_temp_load_package(load_id)
    # write test file
    file_name = s.write_temp_job_file(load_id, "mock_table", None, uniq_id(), content)
    # write schema and schema update
    s.save_temp_schema(Schema("mock"), load_id)
    s.save_temp_schema_updates(load_id, [{}])
    s.commit_temp_load_package(load_id)
    s.start_job(load_id, file_name)
    return load_id, file_name
