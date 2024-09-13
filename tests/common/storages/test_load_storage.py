import os
import pytest

from dlt.common import json, pendulum
from dlt.common.schema import TSchemaTables
from dlt.common.storages import PackageStorage, LoadStorage
from dlt.common.storages.exceptions import LoadPackageNotFound, NoMigrationPathException

from dlt.common.storages.file_storage import FileStorage
from dlt.common.storages.load_package import create_load_id
from tests.common.storages.utils import (
    start_loading_file,
    assert_package_info,
    load_storage,
    start_loading_files,
)
from tests.utils import write_version, autouse_test_storage


def test_complete_successful_package(load_storage: LoadStorage) -> None:
    # should delete package in full
    load_storage.config.delete_completed_jobs = True
    load_id, file_name = start_loading_file(load_storage, [{"content": "a"}, {"content": "b"}])
    assert load_storage.storage.has_folder(load_storage.get_normalized_package_path(load_id))
    load_storage.normalized_packages.complete_job(load_id, file_name)
    assert_package_info(load_storage, load_id, "normalized", "completed_jobs")
    load_storage.complete_load_package(load_id, False)
    # deleted from loading
    assert not load_storage.storage.has_folder(load_storage.get_normalized_package_path(load_id))
    # has package
    assert load_storage.storage.has_folder(load_storage.get_loaded_package_path(load_id))
    assert load_storage.storage.has_file(
        os.path.join(
            load_storage.get_loaded_package_path(load_id),
            PackageStorage.PACKAGE_COMPLETED_FILE_NAME,
        )
    )
    # but completed packages are deleted
    load_storage.maybe_remove_completed_jobs(load_id)
    assert not load_storage.loaded_packages.storage.has_folder(
        load_storage.loaded_packages.get_job_state_folder_path(load_id, "completed_jobs")
    )
    assert_package_info(load_storage, load_id, "loaded", "completed_jobs", jobs_count=0)
    # delete completed package
    load_storage.delete_loaded_package(load_id)
    assert not load_storage.storage.has_folder(load_storage.get_loaded_package_path(load_id))
    # do not delete completed jobs
    load_storage.config.delete_completed_jobs = False
    load_id, file_name = start_loading_file(load_storage, [{"content": "a"}, {"content": "b"}])
    load_storage.normalized_packages.complete_job(load_id, file_name)
    load_storage.complete_load_package(load_id, False)
    # deleted from loading
    assert not load_storage.storage.has_folder(load_storage.get_normalized_package_path(load_id))
    # has load preserved
    assert load_storage.storage.has_folder(load_storage.get_loaded_package_path(load_id))
    assert load_storage.storage.has_file(
        os.path.join(
            load_storage.get_loaded_package_path(load_id),
            PackageStorage.PACKAGE_COMPLETED_FILE_NAME,
        )
    )
    # has completed loads
    assert load_storage.loaded_packages.storage.has_folder(
        load_storage.loaded_packages.get_job_state_folder_path(load_id, "completed_jobs")
    )
    load_storage.delete_loaded_package(load_id)
    assert not load_storage.storage.has_folder(load_storage.get_loaded_package_path(load_id))


def test_wipe_normalized_packages(load_storage: LoadStorage) -> None:
    load_id, file_name = start_loading_file(load_storage, [{"content": "a"}, {"content": "b"}])
    load_storage.wipe_normalized_packages()
    assert not load_storage.storage.has_folder(load_storage.NORMALIZED_FOLDER)


def test_complete_package_failed_jobs(load_storage: LoadStorage) -> None:
    # loads with failed jobs are always persisted
    load_storage.config.delete_completed_jobs = True
    load_id, file_name = start_loading_file(load_storage, [{"content": "a"}, {"content": "b"}])
    assert load_storage.storage.has_folder(load_storage.get_normalized_package_path(load_id))
    load_storage.normalized_packages.fail_job(load_id, file_name, "EXCEPTION")
    assert_package_info(load_storage, load_id, "normalized", "failed_jobs")
    load_storage.complete_load_package(load_id, False)
    # deleted from loading
    assert not load_storage.storage.has_folder(load_storage.get_normalized_package_path(load_id))
    # present in completed loads folder
    assert load_storage.storage.has_folder(load_storage.get_loaded_package_path(load_id))
    # has completed loads
    assert load_storage.loaded_packages.storage.has_folder(
        load_storage.loaded_packages.get_job_state_folder_path(load_id, "completed_jobs")
    )
    assert_package_info(load_storage, load_id, "loaded", "failed_jobs")

    # get failed jobs info
    failed_files = sorted(load_storage.loaded_packages.list_failed_jobs(load_id))
    # only jobs
    assert len(failed_files) == 1
    assert load_storage.loaded_packages.storage.has_file(failed_files[0])
    failed_info = load_storage.list_failed_jobs_in_loaded_package(load_id)
    assert failed_info[0].file_path == load_storage.loaded_packages.storage.make_full_path(
        failed_files[0]
    )
    assert failed_info[0].failed_message == "EXCEPTION"
    assert failed_info[0].job_file_info.table_name == "mock_table"
    # a few stats
    assert failed_info[0].file_size == 32
    assert (pendulum.now().diff(failed_info[0].created_at)).seconds < 2
    assert failed_info[0].elapsed < 2

    package_info = load_storage.get_load_package_info(load_id)
    assert package_info.state == "loaded"
    assert package_info.schema_update == {}
    assert package_info.jobs["failed_jobs"] == failed_info


def test_abort_package(load_storage: LoadStorage) -> None:
    # loads with failed jobs are always persisted
    load_storage.config.delete_completed_jobs = True
    load_id, file_name = start_loading_file(load_storage, [{"content": "a"}, {"content": "b"}])
    assert load_storage.storage.has_folder(load_storage.get_normalized_package_path(load_id))
    load_storage.normalized_packages.fail_job(load_id, file_name, "EXCEPTION")
    assert_package_info(load_storage, load_id, "normalized", "failed_jobs")
    load_storage.complete_load_package(load_id, True)
    assert load_storage.loaded_packages.storage.has_folder(
        load_storage.loaded_packages.get_job_state_folder_path(load_id, "completed_jobs")
    )
    assert_package_info(load_storage, load_id, "aborted", "failed_jobs")


def test_process_schema_update(load_storage: LoadStorage) -> None:
    with pytest.raises(FileNotFoundError):
        load_storage.begin_schema_update("load_id")
    load_id, fn = start_loading_file(load_storage, "test file")  # type: ignore[arg-type]
    assert load_storage.begin_schema_update(load_id) == {}
    assert load_storage.begin_schema_update(load_id) == {}
    # store the applied schema update
    applied_update: TSchemaTables = {"table": {"name": "table", "columns": {}}}
    load_storage.commit_schema_update(load_id, applied_update)
    with pytest.raises(FileNotFoundError):
        load_storage.commit_schema_update(load_id, applied_update)
    assert load_storage.begin_schema_update(load_id) is None
    # processed file exists
    applied_update_path = os.path.join(
        load_storage.get_normalized_package_path(load_id),
        PackageStorage.APPLIED_SCHEMA_UPDATES_FILE_NAME,
    )
    assert load_storage.storage.has_file(applied_update_path) is True
    assert json.loads(load_storage.storage.load(applied_update_path)) == applied_update
    # verify info package
    package_info = assert_package_info(load_storage, load_id, "normalized", "started_jobs")
    # applied update is present
    assert package_info.schema_update == applied_update
    # should be in dict
    package_dict = package_info.asdict()
    assert len(package_dict["tables"]) == 1
    # commit package
    load_storage.complete_load_package(load_id, False)
    package_info = assert_package_info(load_storage, load_id, "loaded", "started_jobs")
    # applied update is present
    assert package_info.schema_update == applied_update


def test_get_unknown_package_info(load_storage: LoadStorage) -> None:
    with pytest.raises(LoadPackageNotFound):
        load_storage.get_load_package_info("UNKNOWN LOAD ID")


def test_import_extracted_package(load_storage: LoadStorage) -> None:
    # create extracted package
    extracted = PackageStorage(
        FileStorage(os.path.join(load_storage.config.load_volume_path, "extracted")), "new"
    )
    load_id = create_load_id()
    extracted.create_package(load_id)
    extracted_state = extracted.get_load_package_state(load_id)
    load_storage.import_extracted_package(load_id, extracted)
    # make sure state was imported
    assert extracted_state == load_storage.new_packages.get_load_package_state(load_id)
    # move to normalized
    load_storage.commit_new_load_package(load_id)
    assert extracted_state == load_storage.normalized_packages.get_load_package_state(load_id)
    # move to loaded
    load_storage.complete_load_package(load_id, aborted=False)
    assert extracted_state == load_storage.loaded_packages.get_load_package_state(load_id)


def test_full_migration_path() -> None:
    # create directory structure
    s = LoadStorage(True, LoadStorage.ALL_SUPPORTED_FILE_FORMATS)
    # overwrite known initial version
    write_version(s.storage, "1.0.0")
    # must be able to migrate to current version
    s = LoadStorage(False, LoadStorage.ALL_SUPPORTED_FILE_FORMATS)
    assert s.version == LoadStorage.STORAGE_VERSION


def test_unknown_migration_path() -> None:
    # create directory structure
    s = LoadStorage(True, LoadStorage.ALL_SUPPORTED_FILE_FORMATS)
    # overwrite known initial version
    write_version(s.storage, "10.0.0")
    # must be able to migrate to current version
    with pytest.raises(NoMigrationPathException):
        LoadStorage(False, LoadStorage.ALL_SUPPORTED_FILE_FORMATS)
