import posixpath
from pathlib import Path
import os

import dlt
from dlt.common.utils import uniq_id
from dlt.common.storages.load_storage import LoadJobInfo
from dlt.destinations.filesystem.filesystem import FilesystemClient, LoadFilesystemJob
from dlt.destinations.bigquery.bigquery import BigQueryClient
from dlt.common.schema.typing import LOADS_TABLE_NAME
import pyarrow.parquet as pq


def test_pipeline_parquet_bigquery_destination() -> None:
    """Run pipeline twice with merge write disposition
    Resource with primary key falls back to append. Resource without keys falls back to replace.
    """
    pipeline = dlt.pipeline(pipeline_name='parquet_test_' + uniq_id(), destination="bigquery",  dataset_name='parquet_test_' + uniq_id())

    @dlt.resource(primary_key='id')
    def some_data():  # type: ignore[no-untyped-def]
        yield [{'id': 1}, {'id': 2}, {'id': 3}]

    @dlt.resource
    def other_data():  # type: ignore[no-untyped-def]
        yield [1, 2, 3, 4, 5]

    @dlt.source
    def some_source():  # type: ignore[no-untyped-def]
        return [some_data(), other_data()]

    info = pipeline.run(some_source())
    package_info = pipeline.get_load_package_info(info.loads_ids[0])
    assert package_info.state == "loaded"
    # all three jobs succeeded
    assert len(package_info.jobs["failed_jobs"]) == 0
    assert len(package_info.jobs["completed_jobs"]) == 3

    client: BigQueryClient = pipeline._destination_client()  # type: ignore[assignment]
    with client.sql_client as sql_client:
        assert [row[0] for row in sql_client.execute_sql("SELECT * FROM other_data")] == [1, 2, 3, 4, 5]
        assert [row[0] for row in sql_client.execute_sql("SELECT * FROM some_data")] == [1, 2, 3]


def test_pipeline_parquet_filesystem_destination() -> None:

    os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = "file://_storage"
    pipeline = dlt.pipeline(pipeline_name='parquet_test_' + uniq_id(), destination="filesystem",  dataset_name='parquet_test_' + uniq_id())

    @dlt.resource(primary_key='id')
    def some_data():  # type: ignore[no-untyped-def]
        yield [{'id': 1}, {'id': 2}, {'id': 3}]

    @dlt.resource
    def other_data():  # type: ignore[no-untyped-def]
        yield [1, 2, 3, 4, 5]

    @dlt.source
    def some_source():  # type: ignore[no-untyped-def]
        return [some_data(), other_data()]

    info = pipeline.run(some_source(), loader_file_format="parquet")
    package_info = pipeline.get_load_package_info(info.loads_ids[0])
    assert package_info.state == "loaded"

    # all three jobs succeeded
    assert len(package_info.jobs["failed_jobs"]) == 0
    assert len(package_info.jobs["completed_jobs"]) == 3

    client: FilesystemClient = pipeline._destination_client()  # type: ignore[assignment]
    some_data_glob = posixpath.join(client.dataset_path, 'some_source.some_data.*')
    other_data_glob = posixpath.join(client.dataset_path, 'some_source.other_data.*')

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
