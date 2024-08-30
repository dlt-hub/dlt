import os
import pytest
from pathlib import Path
from os.path import join

import dlt

from dlt.common import sleep
from dlt.common.schema import Schema
from dlt.common.storages import PackageStorage, LoadStorage, ParsedLoadJobFileName
from dlt.common.storages.exceptions import LoadPackageAlreadyCompleted, LoadPackageNotCompleted
from dlt.common.utils import uniq_id
from dlt.common.pendulum import pendulum
from dlt.common.configuration.container import Container
from dlt.common.storages.load_package import (
    LoadPackageStateInjectableContext,
    create_load_id,
    destination_state,
    load_package,
    commit_load_package_state,
    clear_destination_state,
)

from tests.common.storages.utils import (
    start_loading_file,
    assert_package_info,
    load_storage,
    start_loading_files,
)
from tests.utils import TEST_STORAGE_ROOT, autouse_test_storage


def test_is_partially_loaded(load_storage: LoadStorage) -> None:
    load_id, file_names = start_loading_files(
        load_storage, [{"content": "a"}, {"content": "b"}], start_job=False, file_count=2
    )
    info = load_storage.get_load_package_info(load_id)
    # all jobs are new
    assert PackageStorage.is_package_partially_loaded(info) is False
    # start one job
    load_storage.normalized_packages.start_job(load_id, file_names[0])
    info = load_storage.get_load_package_info(load_id)
    assert PackageStorage.is_package_partially_loaded(info) is False
    # complete job
    load_storage.normalized_packages.complete_job(load_id, file_names[0])
    info = load_storage.get_load_package_info(load_id)
    assert PackageStorage.is_package_partially_loaded(info) is True
    # start second job
    load_storage.normalized_packages.start_job(load_id, file_names[1])
    info = load_storage.get_load_package_info(load_id)
    assert PackageStorage.is_package_partially_loaded(info) is True
    # finish second job, now not partial anymore
    load_storage.normalized_packages.complete_job(load_id, file_names[1])
    info = load_storage.get_load_package_info(load_id)
    assert PackageStorage.is_package_partially_loaded(info) is False

    # must complete package
    load_storage.complete_load_package(load_id, False)
    info = load_storage.get_load_package_info(load_id)
    assert PackageStorage.is_package_partially_loaded(info) is False

    # abort package (will never be partially loaded)
    load_id, file_name = start_loading_file(load_storage, [{"content": "a"}, {"content": "b"}])
    load_storage.complete_load_package(load_id, True)
    info = load_storage.get_load_package_info(load_id)
    assert PackageStorage.is_package_partially_loaded(info) is False

    # abort partially loaded will stay partially loaded
    load_id, file_names = start_loading_files(
        load_storage, [{"content": "a"}, {"content": "b"}], start_job=False, file_count=2
    )
    load_storage.normalized_packages.start_job(load_id, file_names[0])
    load_storage.normalized_packages.complete_job(load_id, file_names[0])
    load_storage.complete_load_package(load_id, True)
    info = load_storage.get_load_package_info(load_id)
    assert PackageStorage.is_package_partially_loaded(info) is True

    # failed jobs will also result in partial loads, if one job is completed
    load_id, file_names = start_loading_files(
        load_storage, [{"content": "a"}, {"content": "b"}], start_job=False, file_count=2
    )
    load_storage.normalized_packages.start_job(load_id, file_names[0])
    load_storage.normalized_packages.complete_job(load_id, file_names[0])
    load_storage.normalized_packages.start_job(load_id, file_names[1])
    load_storage.normalized_packages.fail_job(load_id, file_names[1], "much broken, so bad")
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


def test_create_package(load_storage: LoadStorage) -> None:
    package_storage = load_storage.new_packages
    # create package without initial state
    load_id = create_load_id()
    package_storage.create_package(load_id)
    # get state, created at must be == load_id
    state = package_storage.get_load_package_state(load_id)
    assert state["created_at"] == pendulum.from_timestamp(float(load_id))
    # assume those few lines execute in less than a second
    assert pendulum.now().diff(state["created_at"]).total_seconds() < 1

    # create package with non timestamp load id
    load_id = uniq_id()
    package_storage.create_package(load_id)
    state = package_storage.get_load_package_state(load_id)
    # still valid created at is there
    # assume those few lines execute in less than a second
    assert pendulum.now().diff(state["created_at"]).total_seconds() < 1

    force_created_at = pendulum.now().subtract(days=1)
    state["destination_state"] = {"destination": "custom"}
    state["created_at"] = force_created_at
    load_id = uniq_id()
    package_storage.create_package(load_id, initial_state=state)
    state_2 = package_storage.get_load_package_state(load_id)
    assert state_2["created_at"] == force_created_at


def test_create_and_update_load_package_state(load_storage: LoadStorage) -> None:
    load_storage.new_packages.create_package("copy")
    state = load_storage.new_packages.get_load_package_state("copy")
    assert state["_state_version"] == 0
    assert state["_version_hash"] is not None
    assert state["created_at"] is not None
    old_state = state.copy()

    state["new_key"] = "new_value"  # type: ignore
    load_storage.new_packages.save_load_package_state("copy", state)

    state = load_storage.new_packages.get_load_package_state("copy")
    assert state["new_key"] == "new_value"  # type: ignore
    assert state["_state_version"] == 1
    assert state["_version_hash"] != old_state["_version_hash"]
    # created timestamp should be conserved
    assert state["created_at"] == old_state["created_at"]

    # check timestamp
    created_at = state["created_at"]
    now = pendulum.now()
    assert (now - created_at).in_seconds() < 2


def test_create_load_id() -> None:
    # must increase over time
    load_id_1 = create_load_id()
    sleep(0.1)
    load_id_2 = create_load_id()
    assert load_id_2 > load_id_1


def test_load_package_state_injectable_context(load_storage: LoadStorage) -> None:
    load_storage.new_packages.create_package("copy")

    container = Container()
    with container.injectable_context(
        LoadPackageStateInjectableContext(
            storage=load_storage.new_packages,
            load_id="copy",
        )
    ):
        # test general load package state
        injected_state = load_package()
        assert injected_state["state"]["_state_version"] == 0
        injected_state["state"]["new_key"] = "new_value"  # type: ignore

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
        second_injected_instance = load_package()
        assert second_injected_instance == injected_state

        # check scoped destination states
        assert (
            load_storage.new_packages.get_load_package_state("copy").get("destination_state")
            is None
        )
        dstate = destination_state()
        dstate["new_key"] = "new_value"
        commit_load_package_state()
        assert load_storage.new_packages.get_load_package_state("copy").get(
            "destination_state"
        ) == {"new_key": "new_value"}

        # this also shows up on the previously injected state
        assert injected_state["state"]["destination_state"]["new_key"] == "new_value"

        # clear destination state
        clear_destination_state()
        assert (
            load_storage.new_packages.get_load_package_state("copy").get("destination_state")
            is None
        )


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
        f_n_t.table_name, file_id, 0, loader_file_format="jsonl"
    )
    # test the exact representation but we should probably not test for that
    assert job_f_n == f"test_table.{file_id}.0.jsonl"
    assert ParsedLoadJobFileName.parse(job_f_n) == f_n_t
    # also parses full paths correctly
    assert ParsedLoadJobFileName.parse("load_id/" + job_f_n) == f_n_t

    # parts cannot contain dots
    with pytest.raises(ValueError):
        PackageStorage.build_job_file_name("test.table", file_id, 0, loader_file_format="jsonl")
        PackageStorage.build_job_file_name("test_table", "f.id", 0, loader_file_format="jsonl")

    # parsing requires 4 parts and retry count
    with pytest.raises(ValueError):
        ParsedLoadJobFileName.parse(job_f_n + ".more")

    with pytest.raises(ValueError):
        ParsedLoadJobFileName.parse("tab.id.wrong_retry.jsonl")


def test_load_package_listings(load_storage: LoadStorage) -> None:
    # 100 csv files
    load_id = create_load_package(load_storage.new_packages, 100)
    new_jobs = load_storage.new_packages.list_new_jobs(load_id)
    assert len(new_jobs) == 100
    assert len(load_storage.new_packages.list_job_with_states_for_table(load_id, "items_1")) == 100
    assert len(load_storage.new_packages.list_job_with_states_for_table(load_id, "items_2")) == 0
    assert len(load_storage.new_packages.list_all_jobs_with_states(load_id)) == 100
    assert len(load_storage.new_packages.list_started_jobs(load_id)) == 0
    assert len(load_storage.new_packages.list_failed_jobs(load_id)) == 0
    assert load_storage.new_packages.is_package_completed(load_id) is False
    with pytest.raises(LoadPackageNotCompleted):
        load_storage.new_packages.list_failed_jobs_infos(load_id)
    # add a few more files
    add_new_jobs(load_storage.new_packages, load_id, 7, "items_2")
    assert len(load_storage.new_packages.list_job_with_states_for_table(load_id, "items_1")) == 100
    assert len(load_storage.new_packages.list_job_with_states_for_table(load_id, "items_2")) == 7
    j_w_s = load_storage.new_packages.list_all_jobs_with_states(load_id)
    assert len(j_w_s) == 107
    assert all(job[0] == "new_jobs" for job in j_w_s)
    with pytest.raises(FileNotFoundError):
        load_storage.new_packages.get_job_failed_message(load_id, j_w_s[0][1])
    # get package infos
    package_jobs = load_storage.new_packages.get_load_package_jobs(load_id)
    assert len(package_jobs["new_jobs"]) == 107
    # other folders empty
    assert len(package_jobs["started_jobs"]) == 0
    package_info = load_storage.new_packages.get_load_package_info(load_id)
    assert len(package_info.jobs["new_jobs"]) == 107
    assert len(package_info.jobs["completed_jobs"]) == 0
    assert package_info.load_id == load_id
    # full path
    assert package_info.package_path == load_storage.new_packages.storage.make_full_path(load_id)
    assert package_info.state == "new"
    assert package_info.completed_at is None

    # move some files
    new_jobs = sorted(load_storage.new_packages.list_new_jobs(load_id))
    load_storage.new_packages.start_job(load_id, os.path.basename(new_jobs[0]))
    load_storage.new_packages.start_job(load_id, os.path.basename(new_jobs[1]))
    load_storage.new_packages.start_job(load_id, os.path.basename(new_jobs[-1]))
    load_storage.new_packages.start_job(load_id, os.path.basename(new_jobs[-2]))

    assert len(load_storage.new_packages.list_started_jobs(load_id)) == 4
    assert len(load_storage.new_packages.list_new_jobs(load_id)) == 103
    assert len(load_storage.new_packages.list_job_with_states_for_table(load_id, "items_1")) == 100
    assert len(load_storage.new_packages.list_job_with_states_for_table(load_id, "items_2")) == 7
    package_jobs = load_storage.new_packages.get_load_package_jobs(load_id)
    assert len(package_jobs["new_jobs"]) == 103
    assert len(package_jobs["started_jobs"]) == 4
    package_info = load_storage.new_packages.get_load_package_info(load_id)
    assert len(package_info.jobs["new_jobs"]) == 103
    assert len(package_info.jobs["started_jobs"]) == 4

    # complete and fail some
    load_storage.new_packages.complete_job(load_id, os.path.basename(new_jobs[0]))
    load_storage.new_packages.fail_job(load_id, os.path.basename(new_jobs[1]), None)
    load_storage.new_packages.fail_job(load_id, os.path.basename(new_jobs[-1]), "error!")
    path = load_storage.new_packages.retry_job(load_id, os.path.basename(new_jobs[-2]))
    assert ParsedLoadJobFileName.parse(path).retry_count == 1
    assert (
        load_storage.new_packages.get_job_failed_message(
            load_id, ParsedLoadJobFileName.parse(new_jobs[1])
        )
        is None
    )
    assert (
        load_storage.new_packages.get_job_failed_message(
            load_id, ParsedLoadJobFileName.parse(new_jobs[-1])
        )
        == "error!"
    )
    # can't move again
    with pytest.raises(FileNotFoundError):
        load_storage.new_packages.complete_job(load_id, os.path.basename(new_jobs[0]))
    assert len(load_storage.new_packages.list_started_jobs(load_id)) == 0
    # retry back in new
    assert len(load_storage.new_packages.list_new_jobs(load_id)) == 104
    package_jobs = load_storage.new_packages.get_load_package_jobs(load_id)
    assert len(package_jobs["new_jobs"]) == 104
    assert len(package_jobs["started_jobs"]) == 0
    assert len(package_jobs["completed_jobs"]) == 1
    assert len(package_jobs["failed_jobs"]) == 2
    assert len(load_storage.new_packages.list_failed_jobs(load_id)) == 2
    package_info = load_storage.new_packages.get_load_package_info(load_id)
    assert len(package_info.jobs["new_jobs"]) == 104
    assert len(package_info.jobs["started_jobs"]) == 0
    assert len(package_info.jobs["completed_jobs"]) == 1
    assert len(package_info.jobs["failed_jobs"]) == 2

    # complete package
    load_storage.new_packages.complete_loading_package(load_id, "aborted")
    assert load_storage.new_packages.is_package_completed(load_id)
    with pytest.raises(LoadPackageAlreadyCompleted):
        load_storage.new_packages.complete_loading_package(load_id, "aborted")

    for job in package_info.jobs["failed_jobs"] + load_storage.new_packages.list_failed_jobs_infos(  # type: ignore[operator]
        load_id
    ):
        if job.job_file_info.table_name == "items_1":
            assert job.failed_message is None
        elif job.job_file_info.table_name == "items_2":
            assert job.failed_message == "error!"
        else:
            raise AssertionError()
        assert job.created_at is not None
        assert job.elapsed is not None
        assert job.file_size > 0
        assert job.state == "failed_jobs"
        # must be abs path!
        assert os.path.isabs(job.file_path)


def test_get_load_package_info_perf(load_storage: LoadStorage) -> None:
    import time

    st_t = time.time()
    for _ in range(10000):
        load_storage.loaded_packages.storage.make_full_path("198291092.121/new/ABD.CX.gx")
        # os.path.basename("198291092.121/new/ABD.CX.gx")
    print(time.time() - st_t)

    st_t = time.time()
    load_id = create_load_package(load_storage.loaded_packages, 10000)
    print(time.time() - st_t)

    st_t = time.time()
    # move half of the files to failed
    for file_name in load_storage.loaded_packages.list_new_jobs(load_id)[:1000]:
        load_storage.loaded_packages.start_job(load_id, os.path.basename(file_name))
        load_storage.loaded_packages.fail_job(
            load_id, os.path.basename(file_name), f"FAILED {file_name}"
        )
    print(time.time() - st_t)

    st_t = time.time()
    load_storage.loaded_packages.get_load_package_info(load_id)
    print(time.time() - st_t)

    st_t = time.time()
    table_stat = {}
    for file in load_storage.loaded_packages.list_new_jobs(load_id):
        parsed = ParsedLoadJobFileName.parse(file)
        table_stat[parsed.table_name] = parsed
    print(time.time() - st_t)


def create_load_package(
    package_storage: PackageStorage, new_jobs: int, table_name="items_1"
) -> str:
    schema = Schema("test")
    load_id = create_load_id()
    package_storage.create_package(load_id)
    package_storage.save_schema(load_id, schema)
    add_new_jobs(package_storage, load_id, new_jobs, table_name)
    return load_id


def add_new_jobs(
    package_storage: PackageStorage, load_id: str, new_jobs: int, table_name="items_1"
) -> None:
    for _ in range(new_jobs):
        file_name = PackageStorage.build_job_file_name(
            table_name, ParsedLoadJobFileName.new_file_id(), 0, False, "csv"
        )
        file_path = os.path.join(TEST_STORAGE_ROOT, file_name)
        with open(file_path, "wt", encoding="utf-8") as f:
            f.write("a|b|c")
        package_storage.import_job(load_id, file_path)


def test_migrate_to_load_package_state() -> None:
    """
    Here we test that an existing load package without a state will not error
    when the user upgrades to a dlt version with the state. we simulate it by
    wiping the state after normalization and see wether anything breaks
    """
    from dlt.destinations import dummy

    p = dlt.pipeline(pipeline_name=uniq_id(), destination=dummy(completed_prob=1))

    p.extract([{"id": 1, "name": "dave"}], table_name="person")
    p.normalize()

    # delete load package after normalization
    storage = p._get_load_storage()
    packaged_id = p.list_normalized_load_packages()[0]
    state_path = storage.normalized_packages.get_load_package_state_path(packaged_id)
    storage.storage.delete(join(LoadStorage.NORMALIZED_FOLDER, state_path))

    p.load()
