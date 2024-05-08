import csv
import os
import posixpath
from pathlib import Path
from typing import Any, Callable, List, Dict, cast

from pytest_mock import MockerFixture
import dlt
import pytest

from dlt.common import json
from dlt.common import pendulum
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.common.utils import uniq_id
from dlt.common.storages.load_storage import LoadJobInfo
from dlt.destinations import filesystem
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient

from tests.cases import arrow_table_all_data_types
from tests.common.utils import load_json_case
from tests.utils import ALL_TEST_DATA_ITEM_FORMATS, TestDataItemFormat, skip_if_not_active
from dlt.destinations.path_utils import create_path
from tests.load.pipeline.utils import (
    destinations_configs,
    DestinationTestConfiguration,
)

from tests.pipeline.utils import load_table_counts


skip_if_not_active("filesystem")


def test_pipeline_merge_write_disposition(default_buckets_env: str) -> None:
    """Run pipeline twice with merge write disposition
    Regardless wether primary key is set or not, filesystem appends
    """
    import pyarrow.parquet as pq  # Module is evaluated by other tests

    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "True"

    pipeline = dlt.pipeline(
        pipeline_name="test_" + uniq_id(),
        destination="filesystem",
        dataset_name="test_" + uniq_id(),
    )

    @dlt.resource(primary_key="id")
    def some_data():
        yield [{"id": 1}, {"id": 2}, {"id": 3}]

    @dlt.resource
    def other_data():
        yield [1, 2, 3, 4, 5]

    @dlt.source
    def some_source():
        return [some_data(), other_data()]

    pipeline.run(some_source(), write_disposition="merge")
    assert load_table_counts(pipeline, "some_data", "other_data") == {
        "some_data": 3,
        "other_data": 5,
    }

    # second load shows that merge always appends on filesystem
    pipeline.run(some_source(), write_disposition="merge")
    assert load_table_counts(pipeline, "some_data", "other_data") == {
        "some_data": 6,
        "other_data": 10,
    }

    # Force replace, back to initial values
    pipeline.run(some_source(), write_disposition="replace")
    assert load_table_counts(pipeline, "some_data", "other_data") == {
        "some_data": 3,
        "other_data": 5,
    }


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_pipeline_csv_filesystem_destination(item_type: TestDataItemFormat) -> None:
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "True"
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    # store locally
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "_storage"

    pipeline = dlt.pipeline(
        pipeline_name="parquet_test_" + uniq_id(),
        destination="filesystem",
        dataset_name="parquet_test_" + uniq_id(),
    )

    item, rows, _ = arrow_table_all_data_types(item_type, include_json=False, include_time=True)
    info = pipeline.run(item, table_name="table", loader_file_format="csv")
    info.raise_on_failed_jobs()
    job = info.load_packages[0].jobs["completed_jobs"][0].file_path
    assert job.endswith("csv")
    with open(job, "r", encoding="utf-8", newline="") as f:
        csv_rows = list(csv.DictReader(f, dialect=csv.unix_dialect))
        # header + 3 data rows
        assert len(csv_rows) == 3


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_csv_options(item_type: TestDataItemFormat) -> None:
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "True"
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    # set delimiter and disable headers
    os.environ["NORMALIZE__DATA_WRITER__DELIMITER"] = "|"
    os.environ["NORMALIZE__DATA_WRITER__INCLUDE_HEADER"] = "False"
    # store locally
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "_storage"
    pipeline = dlt.pipeline(
        pipeline_name="parquet_test_" + uniq_id(),
        destination="filesystem",
        dataset_name="parquet_test_" + uniq_id(),
    )

    item, rows, _ = arrow_table_all_data_types(item_type, include_json=False, include_time=True)
    info = pipeline.run(item, table_name="table", loader_file_format="csv")
    info.raise_on_failed_jobs()
    job = info.load_packages[0].jobs["completed_jobs"][0].file_path
    assert job.endswith("csv")
    with open(job, "r", encoding="utf-8", newline="") as f:
        csv_rows = list(csv.reader(f, dialect=csv.unix_dialect, delimiter="|"))
        # no header
        assert len(csv_rows) == 3
    # object csv adds dlt columns
    dlt_columns = 2 if item_type == "object" else 0
    assert len(rows[0]) + dlt_columns == len(csv_rows[0])


@pytest.mark.parametrize("item_type", ALL_TEST_DATA_ITEM_FORMATS)
def test_csv_quoting_style(item_type: TestDataItemFormat) -> None:
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "True"
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    # set quotes to all
    os.environ["NORMALIZE__DATA_WRITER__QUOTING"] = "quote_all"
    os.environ["NORMALIZE__DATA_WRITER__INCLUDE_HEADER"] = "False"
    # store locally
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "_storage"
    pipeline = dlt.pipeline(
        pipeline_name="parquet_test_" + uniq_id(),
        destination="filesystem",
        dataset_name="parquet_test_" + uniq_id(),
    )

    item, _, _ = arrow_table_all_data_types(item_type, include_json=False, include_time=True)
    info = pipeline.run(item, table_name="table", loader_file_format="csv")
    info.raise_on_failed_jobs()
    job = info.load_packages[0].jobs["completed_jobs"][0].file_path
    assert job.endswith("csv")
    with open(job, "r", encoding="utf-8", newline="") as f:
        # we skip headers and every line of data has 3 physical lines (due to string value in arrow_table_all_data_types)
        for line in f:
            line += f.readline()
            line += f.readline()
            # all elements are quoted
            for elem in line.strip().split(","):
                # NULL values are not quoted on arrow writer
                assert (
                    elem.startswith('"')
                    and elem.endswith('"')
                    or (len(elem) == 0 and item_type != "object")
                )


def test_pipeline_parquet_filesystem_destination() -> None:
    import pyarrow.parquet as pq  # Module is evaluated by other tests

    # store locally
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "_storage"
    pipeline = dlt.pipeline(
        pipeline_name="parquet_test_" + uniq_id(),
        destination="filesystem",
        dataset_name="parquet_test_" + uniq_id(),
    )

    @dlt.resource(primary_key="id")
    def some_data():
        yield [{"id": 1}, {"id": 2}, {"id": 3}]

    @dlt.resource
    def other_data():
        yield [1, 2, 3, 4, 5]

    @dlt.source
    def some_source():
        return [some_data(), other_data()]

    info = pipeline.run(some_source(), loader_file_format="parquet")
    package_info = pipeline.get_load_package_info(info.loads_ids[0])
    assert package_info.state == "loaded"

    # all three jobs succeeded
    assert len(package_info.jobs["failed_jobs"]) == 0
    assert len(package_info.jobs["completed_jobs"]) == 3

    client: FilesystemClient = pipeline.destination_client()  # type: ignore[assignment]
    some_data_glob = posixpath.join(client.dataset_path, "some_data/*")
    other_data_glob = posixpath.join(client.dataset_path, "other_data/*")

    some_data_files = client.fs_client.glob(some_data_glob)
    other_data_files = client.fs_client.glob(other_data_glob)

    assert len(some_data_files) == 1
    assert len(other_data_files) == 1

    with open(some_data_files[0], "rb") as f:
        table = pq.read_table(f)
        assert table.column("id").to_pylist() == [1, 2, 3]

    with open(other_data_files[0], "rb") as f:
        table = pq.read_table(f)
        assert table.column("value").to_pylist() == [1, 2, 3, 4, 5]


TEST_LAYOUTS = (
    "{schema_name}/{table_name}/{load_id}.{file_id}.{ext}",
    "{schema_name}.{table_name}.{load_id}.{file_id}.{ext}",
    "{table_name}88{load_id}-u-{file_id}.{ext}",
    "{table_name}/{curr_date}/{load_id}.{file_id}.{ext}{timestamp}",
    "{table_name}/{YYYY}-{MM}-{DD}/{load_id}.{file_id}.{ext}",
    "{table_name}/{YYYY}-{MMM}-{D}/{load_id}.{file_id}.{ext}",
    "{table_name}/{DD}/{HH}/{m}/{load_id}.{file_id}.{ext}",
    "{table_name}/{D}/{HH}/{mm}/{load_id}.{file_id}.{ext}",
    "{table_name}/{timestamp}/{load_id}.{file_id}.{ext}",
    "{table_name}/{timestamp_ms}/{load_id}.{file_id}.{ext}",
    "{table_name}/{load_package_timestamp}/{d}/{load_id}.{file_id}.{ext}",
    "{table_name}/{load_package_timestamp_ms}/{d}/{load_id}.{file_id}.{ext}",
    (
        "{table_name}/{YYYY}/{YY}/{Y}/{MMMM}/{MMM}/{MM}/{M}/{DD}/{D}/"
        "{HH}/{H}/{ddd}/{dd}/{d}/{ss}/{s}/{Q}/{timestamp}/{curr_date}/{load_id}.{file_id}.{ext}"
    ),
    (
        "{table_name}/{YYYY}/{YY}/{Y}/{MMMM}/{MMM}/{MM}/{M}/{DD}/{D}/"
        "{SSSS}/{SSS}/{SS}/{S}/{Q}/{timestamp}/{curr_date}/{load_id}.{file_id}.{ext}"
    ),
)


@pytest.mark.parametrize("layout", TEST_LAYOUTS)
def test_filesystem_destination_extended_layout_placeholders(
    layout: str, default_buckets_env: str, mocker: MockerFixture
) -> None:
    data = load_json_case("simple_row")
    call_count = 0

    def counter(value: Any) -> Callable[..., Any]:
        def count(*args, **kwargs) -> Any:
            nonlocal call_count
            call_count += 1
            return value

        return count

    extra_placeholders = {
        "who": "marcin",
        "action": "says",
        "what": "no potato",
        "func": counter("lifting"),
        "woot": "woot-woot",
        "hiphip": counter("Hurraaaa"),
    }
    now = pendulum.now()
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "_storage"
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "TRUE"

    # the reason why we are patching pendulum.from_timestamp is that
    # we are checking if the load package under a given path exists
    # so we have to mock this out because there will difference in
    # calculated timestamps thus will make the test flaky due to
    # small differences in timestamp calculations
    mocker.patch("pendulum.from_timestamp", return_value=now)
    fs_destination = filesystem(
        layout=layout,
        extra_placeholders=extra_placeholders,
        current_datetime=counter(now),
    )
    pipeline = dlt.pipeline(
        pipeline_name="test_extended_layouts",
        destination=fs_destination,
    )
    load_info = pipeline.run(
        [
            dlt.resource(data, name="table_1"),
            dlt.resource(data * 2, name="table_2"),
            dlt.resource(data * 3, name="table_3"),
        ],
        write_disposition="append",
    )
    client = pipeline.destination_client()

    expected_files = set()
    known_files = set()
    for basedir, _dirs, files in client.fs_client.walk(client.dataset_path):  # type: ignore[attr-defined]
        # strip out special tables
        if "_dlt" in basedir:
            continue

        for file in files:
            if ".jsonl" in file:
                expected_files.add(posixpath.join(basedir, file))

    for load_package in load_info.load_packages:
        for load_info in load_package.jobs["completed_jobs"]:  # type: ignore[assignment]
            job_info = ParsedLoadJobFileName.parse(load_info.file_path)  # type: ignore[attr-defined]
            # state file gets loaded a differentn way
            if job_info.table_name == "_dlt_pipeline_state":
                continue
            path = create_path(
                layout,
                file_name=job_info.file_name(),
                schema_name="test_extended_layouts",
                load_id=load_package.load_id,
                current_datetime=now,
                load_package_timestamp=load_info.created_at.to_iso8601_string(),  # type: ignore[attr-defined]
                extra_placeholders=extra_placeholders,
            )
            full_path = posixpath.join(client.dataset_path, path)  # type: ignore[attr-defined]
            assert client.fs_client.exists(full_path)  # type: ignore[attr-defined]
            if ".jsonl" in full_path:
                known_files.add(full_path)

    assert expected_files == known_files
    assert known_files
    # 6 is because simple_row contains two rows
    # and in this test scenario we have 3 callbacks
    assert call_count >= 6

    # check that table separation works for every path
    # we cannot test when ext is not the last value
    if ".{ext}{timestamp}" not in layout:
        assert load_table_counts(pipeline, "table_1", "table_2", "table_3") == {
            "table_1": 2,
            "table_2": 4,
            "table_3": 6,
        }
    pipeline._fs_client().truncate_tables(["table_1", "table_3"])
    if ".{ext}{timestamp}" not in layout:
        assert load_table_counts(pipeline, "table_1", "table_2", "table_3") == {"table_2": 4}


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_buckets_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_state_files(destination_config: DestinationTestConfiguration) -> None:
    def _collect_files(p) -> List[str]:
        client = p.destination_client()
        found = []
        for basedir, _dirs, files in client.fs_client.walk(client.dataset_path):
            for file in files:
                found.append(os.path.join(basedir, file).replace(client.dataset_path, ""))
        return found

    def _collect_table_counts(p) -> Dict[str, int]:
        return load_table_counts(
            p, "items", "items2", "items3", "_dlt_loads", "_dlt_version", "_dlt_pipeline_state"
        )

    # generate 4 loads from 2 pipelines, store load ids
    p1 = destination_config.setup_pipeline("p1", dataset_name="layout_test")
    p2 = destination_config.setup_pipeline("p2", dataset_name="layout_test")
    c1 = cast(FilesystemClient, p1.destination_client())
    c2 = cast(FilesystemClient, p2.destination_client())

    # first two loads
    p1.run([1, 2, 3], table_name="items").loads_ids[0]
    load_id_2_1 = p2.run([4, 5, 6], table_name="items").loads_ids[0]
    assert _collect_table_counts(p1) == {
        "items": 6,
        "_dlt_loads": 2,
        "_dlt_pipeline_state": 2,
        "_dlt_version": 2,
    }
    sc1_old = c1.get_stored_schema()
    sc2_old = c2.get_stored_schema()
    s1_old = c1.get_stored_state("p1")
    s2_old = c1.get_stored_state("p2")

    created_files = _collect_files(p1)
    # 4 init files, 2 item files, 2 load files, 2 state files, 2 version files
    assert len(created_files) == 12

    # second two loads
    @dlt.resource(table_name="items2")
    def some_data():
        dlt.current.resource_state()["state"] = {"some": "state"}
        yield from [1, 2, 3]

    load_id_1_2 = p1.run(some_data(), table_name="items2").loads_ids[
        0
    ]  # force state and migration bump here
    p2.run([4, 5, 6], table_name="items").loads_ids[0]  # no migration here

    # 4 loads for 2 pipelines, one schema and state change on p2 changes so 3 versions and 3 states
    assert _collect_table_counts(p1) == {
        "items": 9,
        "items2": 3,
        "_dlt_loads": 4,
        "_dlt_pipeline_state": 3,
        "_dlt_version": 3,
    }

    # test accessors for state
    s1 = c1.get_stored_state("p1")
    s2 = c1.get_stored_state("p2")
    assert s1.dlt_load_id == load_id_1_2  # second load
    assert s2.dlt_load_id == load_id_2_1  # first load
    assert s1_old.version != s1.version
    assert s2_old.version == s2.version

    # test accessors for schema
    sc1 = c1.get_stored_schema()
    sc2 = c2.get_stored_schema()
    assert sc1.version_hash != sc1_old.version_hash
    assert sc2.version_hash == sc2_old.version_hash
    assert sc1.version_hash != sc2.version_hash

    assert not c1.get_stored_schema_by_hash("blah")
    assert c2.get_stored_schema_by_hash(sc1_old.version_hash)

    created_files = _collect_files(p1)
    # 4 init files, 4 item files, 4 load files, 3 state files, 3 version files
    assert len(created_files) == 18

    # drop it
    p1.destination_client().drop_storage()
    created_files = _collect_files(p1)
    assert len(created_files) == 0


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_buckets_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_knows_dataset_state(destination_config: DestinationTestConfiguration) -> None:
    # check if pipeline knows initializisation state of dataset
    p1 = destination_config.setup_pipeline("p1", dataset_name="layout_test")
    assert not p1.destination_client().is_storage_initialized()
    p1.run([1, 2, 3], table_name="items")
    assert p1.destination_client().is_storage_initialized()
    p1.destination_client().drop_storage()
    assert not p1.destination_client().is_storage_initialized()


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_buckets_filesystem_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("restore", [True, False])
@pytest.mark.parametrize(
    "layout",
    [
        "{table_name}/{load_id}.{file_id}.{ext}",
        "{schema_name}/other_folder/{table_name}-{load_id}.{file_id}.{ext}",
        "{table_name}/{load_package_timestamp}/{d}/{load_id}.{file_id}.{ext}",
    ],
)  # we need a layout where the table has its own folder and one where it does not
def test_state_with_simple_incremental(
    destination_config: DestinationTestConfiguration,
    restore: bool,
    layout: str,
) -> None:
    os.environ["RESTORE_FROM_DESTINATION"] = str(restore)
    os.environ["DESTINATION__FILESYSTEM__LAYOUT"] = layout

    p = destination_config.setup_pipeline("p1", dataset_name="incremental_test")

    @dlt.resource(name="items")
    def my_resource(prim_key=dlt.sources.incremental("id")):
        yield from [
            {"id": 1},
            {"id": 2},
        ]

    @dlt.resource(name="items")
    def my_resource_inc(prim_key=dlt.sources.incremental("id")):
        yield from [
            {"id": 1},
            {"id": 2},
            {"id": 3},
            {"id": 4},
        ]

    p.run(my_resource)
    p._wipe_working_folder()

    # check incremental
    p = destination_config.setup_pipeline("p1", dataset_name="incremental_test")
    p.run(my_resource_inc)
    assert load_table_counts(p, "items") == {"items": 4 if restore else 6}


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_buckets_filesystem_configs=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "layout",
    [
        "{table_name}/{load_id}.{file_id}.{ext}",
        "{schema_name}/other_folder/{table_name}-{load_id}.{file_id}.{ext}",
    ],
)  # we need a layout where the table has its own folder and one where it does not
def test_client_methods(
    destination_config: DestinationTestConfiguration,
    layout: str,
) -> None:
    p = destination_config.setup_pipeline("access", dataset_name="incremental_test")
    os.environ["DESTINATION__FILESYSTEM__LAYOUT"] = layout

    @dlt.resource()
    def table_1():
        yield [1, 2, 3, 4, 5]

    @dlt.resource()
    def table_2():
        yield [1, 2, 3, 4, 5, 6, 7]

    @dlt.resource()
    def table_3():
        yield [1, 2, 3, 4, 5, 6, 7, 8]

    # 3 files for t_1, 2 files for t_2
    p.run([table_1(), table_2()])
    p.run([table_1(), table_2()])
    p.run([table_1()])

    fs_client = p._fs_client()
    t1_files = fs_client.list_table_files("table_1")
    t2_files = fs_client.list_table_files("table_2")
    assert len(t1_files) == 3
    assert len(t2_files) == 2

    assert load_table_counts(p, "table_1", "table_2") == {"table_1": 15, "table_2": 14}

    # verify that files are in the same folder on the second layout
    folder = fs_client.get_table_dir("table_1")
    file_count = len(fs_client.fs_client.ls(folder))
    if "{table_name}/" in layout:
        print(fs_client.fs_client.ls(folder))
        assert file_count == 3
    else:
        assert file_count == 5

    # check opening of file
    values = []
    for line in fs_client.read_text(t1_files[0]).split("\n"):
        if line:
            values.append(json.loads(line)["value"])
    assert values == [1, 2, 3, 4, 5]

    # check binary read
    assert fs_client.read_bytes(t1_files[0]) == str.encode(fs_client.read_text(t1_files[0]))

    # check truncate
    fs_client.truncate_tables(["table_1"])
    assert load_table_counts(p, "table_1", "table_2") == {"table_2": 14}

    # load again
    p.run([table_1(), table_2(), table_3()])
    assert load_table_counts(p, "table_1", "table_2", "table_3") == {
        "table_1": 5,
        "table_2": 21,
        "table_3": 8,
    }

    # test truncate multiple
    fs_client.truncate_tables(["table_1", "table_3"])
    assert load_table_counts(p, "table_1", "table_2", "table_3") == {"table_2": 21}
