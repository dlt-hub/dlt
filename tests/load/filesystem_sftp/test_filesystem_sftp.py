import pytest
import os
import fsspec
from dlt.common.json import json
import os
import dlt
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
import logging


@pytest.fixture(scope="module")
def sftp_filesystem():
    # Determine the path to the SSH key relative to this Python file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    key_path = os.path.join(current_dir, "bootstrap/foo_rsa")

    # Set up the SFTP filesystem connection using the relative SSH key path
    fs = fsspec.filesystem(
        "sftp", host="localhost", port=2222, username="foo", key_filename=key_path
    )
    yield fs


def test_sftp_server(sftp_filesystem):
    fs = sftp_filesystem
    test_file = "/data/countries.json"
    json_data = {
        "countries": [
            {"name": "United States", "code": "US"},
            {"name": "Canada", "code": "CA"},
            {"name": "Mexico", "code": "MX"},
        ]
    }

    try:
        with fs.open(test_file, "w") as f:
            f.write(json.dumps(json_data))

        files = fs.ls("/data")
        assert test_file in files

        with fs.open(test_file, "r") as f:
            data = json.load(f)
        assert data == json_data

        info = fs.info(test_file)
        assert "mtime" in info
        assert info["type"] == "file"

    finally:
        fs.rm(test_file)


def test_pipeline_filesystem_sftp_destination(sftp_filesystem):
    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "sftp://localhost/data"
    os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__SFTP_PORT"] = "2222"
    os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__SFTP_USERNAME"] = "foo"
    os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__SFTP_PASSWORD"] = "pass"

    @dlt.resource()
    def states():
        yield [{"id": 1, "name": "DE"}, {"id": 2, "name": "AK"}, {"id": 3, "name": "CA"}]

    # f = filesystem(bucket_url="sftp://localhost/data")
    pipeline = dlt.pipeline(destination="filesystem", dataset_name="test")

    pipeline.run([states], loader_file_format="parquet")

    client: FilesystemClient = pipeline.destination_client()  # type: ignore[assignment]

    import posixpath

    data_glob = posixpath.join(client.dataset_path, "states/*")
    data_files = client.fs_client.glob(data_glob)
    assert len(data_files) > 0

    fs = sftp_filesystem
    with fs.open(data_files[0], "rb") as f:
        import pyarrow.parquet as pq

        rows = pq.read_table(f).to_pylist()

        result_states = [r["name"] for r in rows]

        expected_states = ["DE", "AK", "CA"]
        assert sorted(result_states) == sorted(expected_states)
