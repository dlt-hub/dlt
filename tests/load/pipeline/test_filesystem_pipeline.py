import csv
import os
import posixpath
from pathlib import Path
from typing import Any, Callable

import dlt
import pytest

from dlt.common import pendulum
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.common.utils import uniq_id
from dlt.common.storages.load_storage import LoadJobInfo
from dlt.destinations import filesystem
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
from dlt.common.schema.typing import LOADS_TABLE_NAME

from tests.cases import arrow_table_all_data_types
from tests.common.utils import load_json_case
from tests.utils import ALL_TEST_DATA_ITEM_FORMATS, TestDataItemFormat, skip_if_not_active
from dlt.destinations.path_utils import create_path
from tests.load.pipeline.utils import load_table_counts

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
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "file://_storage"

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


def test_pipeline_parquet_filesystem_destination() -> None:
    import pyarrow.parquet as pq  # Module is evaluated by other tests

    # store locally
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "file://_storage"
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
    "{table_name}/{load_package_timestamp}/{d}/{load_id}.{file_id}.{ext}",
    (
        "{table_name}/{YYYY}/{YY}/{Y}/{MMMM}/{MMM}/{MM}/{M}/{DD}/{D}/"
        "{HH}/{H}/{ddd}/{dd}/{d}/{Q}/{timestamp}/{curr_date}/{load_id}.{file_id}.{ext}"
    ),
)


@pytest.mark.parametrize("layout", TEST_LAYOUTS)
def test_filesystem_destination_extended_layout_placeholders(layout: str) -> None:
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
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "file://_storage"
    pipeline = dlt.pipeline(
        pipeline_name="test_extended_layouts",
        destination=filesystem(
            layout=layout,
            extra_placeholders=extra_placeholders,
            kwargs={"auto_mkdir": True},
            current_datetime=counter(now),
        ),
    )
    load_info = pipeline.run(
        dlt.resource(data, name="simple_rows"),
        write_disposition="append",
    )
    client = pipeline.destination_client()
    expected_files = set()
    known_files = set()
    for basedir, _dirs, files in client.fs_client.walk(client.dataset_path):  # type: ignore[attr-defined]
        for file in files:
            if file.endswith("jsonl"):
                expected_files.add(os.path.join(basedir, file))

    for load_package in load_info.load_packages:
        for load_info in load_package.jobs["completed_jobs"]:  # type: ignore[assignment]
            job_info = ParsedLoadJobFileName.parse(load_info.file_path)  # type: ignore[attr-defined]
            path = create_path(
                layout,
                file_name=job_info.file_name(),
                schema_name="test_extended_layouts",
                load_id=load_package.load_id,
                current_datetime=now,
                load_package_timestamp=load_info.created_at.to_iso8601_string(),  # type: ignore[attr-defined]
                extra_placeholders=extra_placeholders,
            )
            full_path = os.path.join(client.dataset_path, path)  # type: ignore[attr-defined]
            assert os.path.exists(full_path)
            if full_path.endswith("jsonl"):
                known_files.add(full_path)

    assert expected_files == known_files
    # 6 is because simple_row contains two rows
    # and in this test scenario we have 3 callbacks
    assert call_count >= 6
