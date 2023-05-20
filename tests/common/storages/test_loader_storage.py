import os
import pytest
from pathlib import Path
from typing import Sequence, Tuple

from dlt.common import sleep, json, pendulum
from dlt.common.schema import Schema, TSchemaTables
from dlt.common.storages.load_storage import LoadPackageInfo, LoadStorage, ParsedLoadJobFileName, TJobState
from dlt.common.configuration import resolve_configuration
from dlt.common.storages import LoadStorageConfiguration
from dlt.common.storages.exceptions import LoadPackageNotFound, NoMigrationPathException
from dlt.common.typing import StrAny
from dlt.common.utils import uniq_id

from tests.utils import TEST_STORAGE_ROOT, write_version, autouse_test_storage


@pytest.fixture
def storage() -> LoadStorage:
    C = resolve_configuration(LoadStorageConfiguration())
    s = LoadStorage(True, "jsonl", LoadStorage.ALL_SUPPORTED_FILE_FORMATS, C)
    return s


def test_complete_successful_package(storage: LoadStorage) -> None:
    # should delete package in full
    storage.config.delete_completed_jobs = True
    load_id, file_name = start_loading_file(storage, [{"content": "a"}, {"content": "b"}])
    assert storage.storage.has_folder(storage.get_package_path(load_id))
    storage.complete_job(load_id, file_name)
    assert_package_info(storage, load_id, "normalized", "completed_jobs")
    storage.complete_load_package(load_id, False)
    # deleted from loading
    assert not storage.storage.has_folder(storage.get_package_path(load_id))
    # has package
    assert storage.storage.has_folder(storage.get_completed_package_path(load_id))
    assert storage.storage.has_file(os.path.join(storage.get_completed_package_path(load_id), LoadStorage.PACKAGE_COMPLETED_FILE_NAME))
    # but completed packages are deleted
    assert not storage.storage.has_folder(storage._get_job_folder_completed_path(load_id, "completed_jobs"))
    assert_package_info(storage, load_id, "loaded", "completed_jobs", jobs_count=0)
    # delete completed package
    storage.delete_completed_package(load_id)
    assert not storage.storage.has_folder(storage.get_completed_package_path(load_id))
    # do not delete completed jobs
    storage.config.delete_completed_jobs = False
    load_id, file_name = start_loading_file(storage, [{"content": "a"}, {"content": "b"}])
    storage.complete_job(load_id, file_name)
    storage.complete_load_package(load_id, False)
    # deleted from loading
    assert not storage.storage.has_folder(storage.get_package_path(load_id))
    # has load preserved
    assert storage.storage.has_folder(storage.get_completed_package_path(load_id))
    assert storage.storage.has_file(os.path.join(storage.get_completed_package_path(load_id), LoadStorage.PACKAGE_COMPLETED_FILE_NAME))
    # has completed loads
    assert storage.storage.has_folder(storage._get_job_folder_completed_path(load_id, "completed_jobs"))
    storage.delete_completed_package(load_id)
    assert not storage.storage.has_folder(storage.get_completed_package_path(load_id))


def test_wipe_normalized_packages(storage: LoadStorage) -> None:
    load_id, file_name = start_loading_file(storage, [{"content": "a"}, {"content": "b"}])

    storage.wipe_normalized_packages()

    assert not storage.storage.has_folder(storage.NORMALIZED_FOLDER)


def test_complete_package_failed_jobs(storage: LoadStorage) -> None:
    # loads with failed jobs are always persisted
    storage.config.delete_completed_jobs = True
    load_id, file_name = start_loading_file(storage, [{"content": "a"}, {"content": "b"}])
    assert storage.storage.has_folder(storage.get_package_path(load_id))
    storage.fail_job(load_id, file_name, "EXCEPTION")
    assert_package_info(storage, load_id, "normalized", "failed_jobs")
    storage.complete_load_package(load_id, False)
    # deleted from loading
    assert not storage.storage.has_folder(storage.get_package_path(load_id))
    # present in completed loads folder
    assert storage.storage.has_folder(storage.get_completed_package_path(load_id))
    # has completed loads
    assert storage.storage.has_folder(storage._get_job_folder_completed_path(load_id, "completed_jobs"))
    assert_package_info(storage, load_id, "loaded", "failed_jobs")

    # get failed jobs info
    failed_files = sorted(storage.list_completed_failed_jobs(load_id))
    # job + message
    assert len(failed_files) == 2
    assert storage.storage.has_file(failed_files[0])
    failed_info = storage.list_failed_jobs_in_completed_package(load_id)
    assert failed_info[0].file_path == storage.storage.make_full_path(failed_files[0])
    assert failed_info[0].failed_message == "EXCEPTION"
    assert failed_info[0].job_file_info.table_name == "mock_table"
    # a few stats
    assert failed_info[0].file_size == 32
    assert (pendulum.now() - failed_info[0].created_at).seconds < 2
    assert failed_info[0].elapsed < 2

    package_info = storage.get_load_package_info(load_id)
    assert package_info.state == "loaded"
    assert package_info.schema_update == {}
    assert package_info.jobs["failed_jobs"] == failed_info


def test_abort_package(storage: LoadStorage) -> None:
    # loads with failed jobs are always persisted
    storage.config.delete_completed_jobs = True
    load_id, file_name = start_loading_file(storage, [{"content": "a"}, {"content": "b"}])
    assert storage.storage.has_folder(storage.get_package_path(load_id))
    storage.fail_job(load_id, file_name, "EXCEPTION")
    assert_package_info(storage, load_id, "normalized", "failed_jobs")
    storage.complete_load_package(load_id, True)
    assert storage.storage.has_folder(storage._get_job_folder_completed_path(load_id, "completed_jobs"))
    assert_package_info(storage, load_id, "aborted", "failed_jobs")


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
    assert_package_info(storage, load_id, "normalized", "new_jobs")
    assert LoadStorage.parse_job_file_name(new_fp).retry_count == 1
    # try again
    fn = Path(new_fp).name
    storage.start_job(load_id, fn)
    new_fp = storage.retry_job(load_id, fn)
    assert LoadStorage.parse_job_file_name(new_fp).retry_count == 2


def test_build_parse_job_path(storage: LoadStorage) -> None:
    file_id = uniq_id(5)
    f_n_t = ParsedLoadJobFileName("test_table", file_id, 0, "jsonl")
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
    assert storage.begin_schema_update(load_id) == {}
    assert storage.begin_schema_update(load_id) == {}
    # store the applied schema update
    applied_update: TSchemaTables = {"table": {"name": "table", "columns": {}}}
    storage.commit_schema_update(load_id, applied_update)
    with pytest.raises(FileNotFoundError):
        storage.commit_schema_update(load_id, applied_update)
    assert storage.begin_schema_update(load_id) is None
    # processed file exists
    applied_update_path = os.path.join(storage.get_package_path(load_id), LoadStorage.APPLIED_SCHEMA_UPDATES_FILE_NAME)
    assert storage.storage.has_file(applied_update_path) is True
    assert json.loads(storage.storage.load(applied_update_path)) == applied_update
    # verify info package
    package_info = assert_package_info(storage, load_id, "normalized", "started_jobs")
    # applied update is present
    assert package_info.schema_update == applied_update
    # should be in dict
    package_dict = package_info.asdict()
    assert len(package_dict["tables"]) == 1
    # commit package
    storage.complete_load_package(load_id, False)
    package_info = assert_package_info(storage, load_id, "loaded", "started_jobs")
    # applied update is present
    assert package_info.schema_update == applied_update


def test_get_unknown_package_info(storage: LoadStorage) -> None:
    with pytest.raises(LoadPackageNotFound):
        storage.get_load_package_info("UNKNOWN LOAD ID")


def test_full_migration_path() -> None:
    # create directory structure
    s = LoadStorage(True, "jsonl", LoadStorage.ALL_SUPPORTED_FILE_FORMATS)
    # overwrite known initial version
    write_version(s.storage, "1.0.0")
    # must be able to migrate to current version
    s = LoadStorage(False, "jsonl", LoadStorage.ALL_SUPPORTED_FILE_FORMATS)
    assert s.version == LoadStorage.STORAGE_VERSION


def test_unknown_migration_path() -> None:
    # create directory structure
    s = LoadStorage(True, "jsonl", LoadStorage.ALL_SUPPORTED_FILE_FORMATS)
    # overwrite known initial version
    write_version(s.storage, "10.0.0")
    # must be able to migrate to current version
    with pytest.raises(NoMigrationPathException):
        LoadStorage(False, "jsonl", LoadStorage.ALL_SUPPORTED_FILE_FORMATS)


def start_loading_file(s: LoadStorage, content: Sequence[StrAny]) -> Tuple[str, str]:
    load_id = uniq_id()
    s.create_temp_load_package(load_id)
    # write test file
    file_name = s.write_temp_job_file(load_id, "mock_table", None, uniq_id(), content)
    # write schema and schema update
    s.save_temp_schema(Schema("mock"), load_id)
    s.save_temp_schema_updates(load_id, {})
    s.commit_temp_load_package(load_id)
    assert_package_info(s, load_id, "normalized", "new_jobs")
    s.start_job(load_id, file_name)
    assert_package_info(s, load_id, "normalized", "started_jobs")
    return load_id, file_name


def assert_package_info(storage: LoadStorage, load_id: str, package_state: str, job_state: TJobState, jobs_count: int = 1) -> LoadPackageInfo:
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
