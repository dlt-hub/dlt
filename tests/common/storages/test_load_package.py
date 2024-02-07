import os
import pytest
from pathlib import Path

from dlt.common import sleep
from dlt.common.schema import Schema
from dlt.common.storages import PackageStorage, LoadStorage, ParsedLoadJobFileName
from dlt.common.utils import uniq_id

from tests.common.storages.utils import start_loading_file, assert_package_info, load_storage
from tests.utils import autouse_test_storage
from dlt.common.pendulum import pendulum
from dlt.common.configuration.container import Container
from dlt.common.pipeline import (
    LoadPackageStateInjectableContext,
    load_package_destination_state,
    load_package_state,
    commit_load_package_state,
    clear_loadpackage_destination_state,
)


def test_is_partially_loaded(load_storage: LoadStorage) -> None:
    load_id, file_name = start_loading_file(
        load_storage, [{"content": "a"}, {"content": "b"}], start_job=False
    )
    info = load_storage.get_load_package_info(load_id)
    # all jobs are new
    assert PackageStorage.is_package_partially_loaded(info) is False
    # start job
    load_storage.normalized_packages.start_job(load_id, file_name)
    info = load_storage.get_load_package_info(load_id)
    assert PackageStorage.is_package_partially_loaded(info) is True
    # complete job
    load_storage.normalized_packages.complete_job(load_id, file_name)
    info = load_storage.get_load_package_info(load_id)
    assert PackageStorage.is_package_partially_loaded(info) is True
    # must complete package
    load_storage.complete_load_package(load_id, False)
    info = load_storage.get_load_package_info(load_id)
    assert PackageStorage.is_package_partially_loaded(info) is False

    # abort package
    load_id, file_name = start_loading_file(load_storage, [{"content": "a"}, {"content": "b"}])
    load_storage.complete_load_package(load_id, True)
    info = load_storage.get_load_package_info(load_id)
    assert PackageStorage.is_package_partially_loaded(info) is True


def test_save_load_schema(load_storage: LoadStorage) -> None:
    # mock schema version to some random number so we know we load what we save
    schema = Schema("event")
    schema._stored_version = 762171

    load_storage.new_packages.create_package("copy")
    saved_file_name = load_storage.new_packages.save_schema("copy", schema)
    assert saved_file_name.endswith(
        os.path.join(
            load_storage.new_packages.storage.storage_path, "copy", PackageStorage.SCHEMA_FILE_NAME
        )
    )
    assert load_storage.new_packages.storage.has_file(
        os.path.join("copy", PackageStorage.SCHEMA_FILE_NAME)
    )
    schema_copy = load_storage.new_packages.load_schema("copy")
    assert schema.stored_version == schema_copy.stored_version


def test_create_and_update_loadpackage_state(load_storage: LoadStorage) -> None:
    load_storage.new_packages.create_package("copy")
    state = load_storage.new_packages.get_load_package_state("copy")
    assert state["_state_version"] == 0
    assert state["_version_hash"] is not None
    assert state["created"] is not None
    old_state = state.copy()

    state["new_key"] = "new_value"  # type: ignore
    load_storage.new_packages.save_load_package_state("copy", state)

    state = load_storage.new_packages.get_load_package_state("copy")
    assert state["new_key"] == "new_value"  # type: ignore
    assert state["_state_version"] == 1
    assert state["_version_hash"] != old_state["_version_hash"]
    # created timestamp should be conserved
    assert state["created"] == old_state["created"]

    # check timestamp
    time = pendulum.from_timestamp(state["created"])
    now = pendulum.now()
    assert (now - time).in_seconds() < 2


def test_loadpackage_state_injectable_context(load_storage: LoadStorage) -> None:
    load_storage.new_packages.create_package("copy")

    container = Container()
    with container.injectable_context(
        LoadPackageStateInjectableContext(
            storage=load_storage.new_packages,
            load_id="copy",
            destination_name="some_destination_name",
        )
    ):
        # test general load package state
        injected_state = load_package_state()
        assert injected_state["_state_version"] == 0
        injected_state["new_key"] = "new_value"  # type: ignore

        # not persisted yet
        assert load_storage.new_packages.get_load_package_state("copy").get("new_key") is None
        # commit
        commit_load_package_state()

        # now it should be persisted
        assert (
            load_storage.new_packages.get_load_package_state("copy").get("new_key") == "new_value"
        )
        assert load_storage.new_packages.get_load_package_state("copy").get("_state_version") == 1

        # check that second injection is the same as first
        second_injected_instance = load_package_state()
        assert second_injected_instance == injected_state

        # check scoped destination states
        assert load_storage.new_packages.get_load_package_state("copy").get("destinations") is None
        destination_state = load_package_destination_state()
        destination_state["new_key"] = "new_value"
        commit_load_package_state()
        assert load_storage.new_packages.get_load_package_state("copy").get("destinations") == {
            "some_destination_name": {"new_key": "new_value"}
        }

        # this also shows up on the previously injected state
        assert injected_state["destinations"]["some_destination_name"]["new_key"] == "new_value"

        # clear destination state
        clear_loadpackage_destination_state()
        assert load_storage.new_packages.get_load_package_state("copy").get("destinations") == {}


def test_job_elapsed_time_seconds(load_storage: LoadStorage) -> None:
    load_id, fn = start_loading_file(load_storage, "test file")  # type: ignore[arg-type]
    fp = load_storage.normalized_packages.storage.make_full_path(
        load_storage.normalized_packages.get_job_file_path(load_id, "started_jobs", fn)
    )
    elapsed = PackageStorage._job_elapsed_time_seconds(fp)
    sleep(0.3)
    # do not touch file
    elapsed_2 = PackageStorage._job_elapsed_time_seconds(fp)
    assert elapsed_2 - elapsed >= 0.3
    # rename the file
    fp = load_storage.normalized_packages.retry_job(load_id, fn)
    # retry_job increases retry number in file name so the line below does not work
    # fp = storage.storage._make_path(storage._get_job_file_path(load_id, "new_jobs", fn))
    elapsed_2 = PackageStorage._job_elapsed_time_seconds(fp)
    # it should keep its mod original date after rename
    assert elapsed_2 - elapsed >= 0.3


def test_retry_job(load_storage: LoadStorage) -> None:
    load_id, fn = start_loading_file(load_storage, "test file")  # type: ignore[arg-type]
    job_fn_t = ParsedLoadJobFileName.parse(fn)
    assert job_fn_t.table_name == "mock_table"
    assert job_fn_t.retry_count == 0
    # now retry
    new_fp = load_storage.normalized_packages.retry_job(load_id, fn)
    assert_package_info(load_storage, load_id, "normalized", "new_jobs")
    assert ParsedLoadJobFileName.parse(new_fp).retry_count == 1
    # try again
    fn = Path(new_fp).name
    load_storage.normalized_packages.start_job(load_id, fn)
    new_fp = load_storage.normalized_packages.retry_job(load_id, fn)
    assert ParsedLoadJobFileName.parse(new_fp).retry_count == 2


def test_build_parse_job_path(load_storage: LoadStorage) -> None:
    file_id = ParsedLoadJobFileName.new_file_id()
    f_n_t = ParsedLoadJobFileName("test_table", file_id, 0, "jsonl")
    job_f_n = PackageStorage.build_job_file_name(
        f_n_t.table_name, file_id, 0, loader_file_format=load_storage.loader_file_format
    )
    # test the exact representation but we should probably not test for that
    assert job_f_n == f"test_table.{file_id}.0.jsonl"
    assert ParsedLoadJobFileName.parse(job_f_n) == f_n_t
    # also parses full paths correctly
    assert ParsedLoadJobFileName.parse("load_id/" + job_f_n) == f_n_t

    # parts cannot contain dots
    with pytest.raises(ValueError):
        PackageStorage.build_job_file_name(
            "test.table", file_id, 0, loader_file_format=load_storage.loader_file_format
        )
        PackageStorage.build_job_file_name(
            "test_table", "f.id", 0, loader_file_format=load_storage.loader_file_format
        )

    # parsing requires 4 parts and retry count
    with pytest.raises(ValueError):
        ParsedLoadJobFileName.parse(job_f_n + ".more")

    with pytest.raises(ValueError):
        ParsedLoadJobFileName.parse("tab.id.wrong_retry.jsonl")
