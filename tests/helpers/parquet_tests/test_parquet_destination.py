import posixpath
from pathlib import Path

import dlt
from dlt.common.utils import uniq_id
from dlt.common.storages.load_storage import LoadJobInfo
from dlt.destinations.filesystem.filesystem import FilesystemClient, LoadFilesystemJob
from dlt.common.schema.typing import LOADS_TABLE_NAME

def test_pipeline_parquet_destination() -> None:
    """Run pipeline twice with merge write disposition
    Resource with primary key falls back to append. Resource without keys falls back to replace.
    """
    pipeline = dlt.pipeline(pipeline_name='parquet_test_' + uniq_id(), destination="bigquery", loader_file_format="parquet", dataset_name='parquet_test_' + uniq_id())

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
    assert info.loads_ids[0] is not None
    assert pipeline.list_failed_jobs_in_package(info.loads_ids[0]) == []