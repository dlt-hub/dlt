import posixpath
from pathlib import Path

import dlt
from dlt.common.utils import uniq_id
from dlt.common.storages.load_storage import LoadJobInfo
from dlt.destinations.filesystem.filesystem import FilesystemClient, LoadFilesystemJob
from dlt.common.schema.typing import LOADS_TABLE_NAME

from tests.utils import clean_test_storage, init_test_logging, preserve_environ
from tests.load.pipeline.utils import drop_pipeline


def assert_file_matches(job: LoadJobInfo, load_id: str, client: FilesystemClient) -> None:
    """Verify file contents of load job are identical to the corresponding file in destination"""
    local_path = Path(job.file_path)
    filename = local_path.name

    destination_fn = LoadFilesystemJob.make_destination_filename(filename, client.schema.name, load_id)
    destination_path = posixpath.join(client.dataset_path, destination_fn)

    assert local_path.read_bytes() == client.fs_client.read_bytes(destination_path)


def test_pipeline_merge_write_disposition(all_buckets_env: str) -> None:
    """Run pipeline twice with merge write disposition
    Resource with primary key falls back to append. Resource without keys falls back to replace.
    """
    pipeline = dlt.pipeline(pipeline_name='test_' + uniq_id(), destination="filesystem", dataset_name='test_' + uniq_id())

    @dlt.resource(primary_key='id')
    def some_data():  # type: ignore[no-untyped-def]
        yield [{'id': 1}, {'id': 2}, {'id': 3}]

    @dlt.resource
    def other_data():  # type: ignore[no-untyped-def]
        yield [1, 2, 3, 4, 5]

    @dlt.source
    def some_source():  # type: ignore[no-untyped-def]
        return [some_data(), other_data()]

    info1 = pipeline.run(some_source(), write_disposition='merge')
    info2 = pipeline.run(some_source(), write_disposition='merge')

    client: FilesystemClient = pipeline._destination_client()  # type: ignore[assignment]

    append_glob = posixpath.join(client.dataset_path, 'some_source.some_data.*')
    replace_glob = posixpath.join(client.dataset_path, 'some_source.other_data.*')

    append_files = client.fs_client.glob(append_glob)
    replace_files = client.fs_client.glob(replace_glob)

    load_id1 = info1.loads_ids[0]
    load_id2 = info2.loads_ids[0]

    # resource with pk is loaded with append and has 1 copy for each load
    assert len(append_files) == 2
    assert any(load_id1 in fn for fn in append_files)
    assert any(load_id2 in fn for fn in append_files)

    # resource without pk is replaced and has 1 copy from second load
    assert len(replace_files) == 1
    assert load_id2 in replace_files[0]

    # Verify file contents
    assert info2.load_packages
    for pkg in info2.load_packages:
        assert pkg.jobs['completed_jobs']
        for job in pkg.jobs['completed_jobs']:
            assert_file_matches(job, pkg.load_id,  client)


    complete_fn = f"{client.schema.name}.{LOADS_TABLE_NAME}.%s"

    # Test complete_load markers are saved
    assert client.fs_client.isfile(posixpath.join(client.dataset_path, complete_fn % load_id1))
    assert client.fs_client.isfile(posixpath.join(client.dataset_path, complete_fn % load_id2))
