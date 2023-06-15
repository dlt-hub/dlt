import posixpath
from pathlib import Path
import os

import dlt
from dlt.common.utils import uniq_id
from dlt.common.storages.load_storage import LoadJobInfo
from dlt.destinations.filesystem.filesystem import FilesystemClient, LoadFilesystemJob
from dlt.common.schema.typing import LOADS_TABLE_NAME

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
    # both failed - we wait till the current loop is completed and then raise
    assert len(package_info.jobs["failed_jobs"]) == 0
    assert len(package_info.jobs["completed_jobs"]) == 3


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

    info = pipeline.run(some_source())
    package_info = pipeline.get_load_package_info(info.loads_ids[0])
    assert package_info.state == "loaded"
    # both failed - we wait till the current loop is completed and then raise
    assert len(package_info.jobs["failed_jobs"]) == 0
    assert len(package_info.jobs["completed_jobs"]) == 3