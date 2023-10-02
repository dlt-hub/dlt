import posixpath
from pathlib import Path

import dlt, os
from dlt.common.utils import uniq_id
from dlt.common.storages.load_storage import LoadJobInfo
from dlt.destinations.filesystem.filesystem import FilesystemClient, LoadFilesystemJob
from dlt.common.schema.typing import LOADS_TABLE_NAME

from tests.utils import skip_if_not_active

skip_if_not_active("filesystem")


def assert_file_matches(layout: str, job: LoadJobInfo, load_id: str, client: FilesystemClient) -> None:
    """Verify file contents of load job are identical to the corresponding file in destination"""
    local_path = Path(job.file_path)
    filename = local_path.name

    destination_fn = LoadFilesystemJob.make_destination_filename(layout, filename, client.schema.name, load_id)
    destination_path = posixpath.join(client.dataset_path, destination_fn)

    assert local_path.read_bytes() == client.fs_client.read_bytes(destination_path)


def test_pipeline_merge_write_disposition(all_buckets_env: str) -> None:
    """Run pipeline twice with merge write disposition
    Resource with primary key falls back to append. Resource without keys falls back to replace.
    """
    import pyarrow.parquet as pq  # Module is evaluated by other tests

    pipeline = dlt.pipeline(pipeline_name='test_' + uniq_id(), destination="filesystem", dataset_name='test_' + uniq_id())

    @dlt.resource(primary_key='id')
    def some_data():
        yield [{'id': 1}, {'id': 2}, {'id': 3}]

    @dlt.resource
    def other_data():
        yield [1, 2, 3, 4, 5]

    @dlt.source
    def some_source():
        return [some_data(), other_data()]

    info1 = pipeline.run(some_source(), write_disposition='merge')
    info2 = pipeline.run(some_source(), write_disposition='merge')

    client: FilesystemClient = pipeline.destination_client()  # type: ignore[assignment]
    layout = client.config.layout

    append_glob = list(client._get_table_dirs(["some_data"]))[0]
    replace_glob = list(client._get_table_dirs(["other_data"]))[0]

    append_files = client.fs_client.ls(append_glob, detail=False, refresh=True)
    replace_files = client.fs_client.ls(replace_glob, detail=False, refresh=True)

    load_id1 = info1.loads_ids[0]
    load_id2 = info2.loads_ids[0]

    # resource with pk is loaded with append and has 1 copy for each load
    assert len(append_files) == 2
    assert any(load_id1 in fn for fn in append_files)
    assert any(load_id2 in fn for fn in append_files)

    # resource without pk is treated as append disposition
    assert len(replace_files) == 2
    assert any(load_id1 in fn for fn in replace_files)
    assert any(load_id2 in fn for fn in replace_files)

    # Verify file contents
    assert info2.load_packages
    for pkg in info2.load_packages:
        assert pkg.jobs['completed_jobs']
        for job in pkg.jobs['completed_jobs']:
            assert_file_matches(layout, job, pkg.load_id,  client)


    complete_fn = f"{client.schema.name}.{LOADS_TABLE_NAME}.%s"

    # Test complete_load markers are saved
    assert client.fs_client.isfile(posixpath.join(client.dataset_path, complete_fn % load_id1))
    assert client.fs_client.isfile(posixpath.join(client.dataset_path, complete_fn % load_id2))

    # Force replace
    pipeline.run(some_source(), write_disposition='replace')
    append_files = client.fs_client.ls(append_glob, detail=False, refresh=True)
    replace_files = client.fs_client.ls(replace_glob, detail=False, refresh=True)
    assert len(append_files) == 1
    assert len(replace_files) == 1


def test_pipeline_parquet_filesystem_destination() -> None:

    import pyarrow.parquet as pq  # Module is evaluated by other tests

    # store locally
    os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = "file://_storage"
    pipeline = dlt.pipeline(pipeline_name='parquet_test_' + uniq_id(), destination="filesystem",  dataset_name='parquet_test_' + uniq_id())

    @dlt.resource(primary_key='id')
    def some_data():
        yield [{'id': 1}, {'id': 2}, {'id': 3}]

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
    some_data_glob = posixpath.join(client.dataset_path, 'some_data/*')
    other_data_glob = posixpath.join(client.dataset_path, 'other_data/*')

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
