import os
import pytest
import fsspec
import dlt

from dlt.common.json import json
from dlt.common.configuration.inject import with_config
from dlt.common.storages import FilesystemConfiguration, fsspec_from_config
from dlt.common.storages.fsspec_filesystem import glob_files
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient

from tests.common.storages.utils import assert_sample_files


@with_config(spec=FilesystemConfiguration, sections=("sources", "filesystem"))
def get_config(config: FilesystemConfiguration = None) -> FilesystemConfiguration:
    return config


def get_key_path(user: str = "foo") -> str:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, f"bootstrap/{user}_rsa")


def files_are_equal(file1_path, file2_path):
    try:
        with open(file1_path, "r", encoding="utf-8") as f1, open(
            file2_path, "r", encoding="utf-8"
        ) as f2:
            return f1.read() == f2.read()
    except FileNotFoundError:
        return False


def is_ssh_agent_ready():
    try:
        # Never skip tests when running in CI
        if os.getenv("CI"):
            return True

        # Check if SSH agent is running
        ssh_agent_pid = os.getenv("SSH_AGENT_PID")
        if not ssh_agent_pid:
            return False

        # Check if the key is present and matches
        id_rsa_pub_path = os.path.expanduser("~/.ssh/id_rsa")
        bobby_rsa_pub_path = os.path.expanduser(get_key_path("bobby"))
        if not os.path.isfile(id_rsa_pub_path) or not os.path.isfile(bobby_rsa_pub_path):
            return False

        return files_are_equal(id_rsa_pub_path, bobby_rsa_pub_path)
    except Exception:
        return False


@pytest.fixture(scope="module")
def sftp_filesystem():
    fs = fsspec.filesystem(
        "sftp", host="localhost", port=2222, username="foo", key_filename=get_key_path()
    )
    yield fs


@pytest.mark.sftp
def test_filesystem_sftp_server(sftp_filesystem):
    test_file = "/data/countries.json"
    input_data = {
        "countries": [
            {"name": "United States", "code": "US"},
            {"name": "Canada", "code": "CA"},
            {"name": "Mexico", "code": "MX"},
        ]
    }

    fs = sftp_filesystem
    try:
        with fs.open(test_file, "w") as f:
            f.write(json.dumps(input_data))

        files = fs.ls("/data")
        assert test_file in files

        with fs.open(test_file, "r") as f:
            data = json.load(f)
        assert data == input_data

        info = fs.info(test_file)
        assert "mtime" in info

    finally:
        fs.rm(test_file)


@pytest.mark.sftp
def test_filesystem_sftp_write(sftp_filesystem):
    import posixpath
    import pyarrow.parquet as pq

    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "sftp://localhost/data"
    os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__SFTP_PORT"] = "2222"
    os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__SFTP_USERNAME"] = "foo"
    os.environ["DESTINATION__FILESYSTEM__CREDENTIALS__SFTP_PASSWORD"] = "pass"

    @dlt.resource()
    def states():
        yield [{"id": 1, "name": "DE"}, {"id": 2, "name": "AK"}, {"id": 3, "name": "CA"}]

    pipeline = dlt.pipeline(destination="filesystem", dataset_name="test")
    pipeline.run([states], loader_file_format="parquet")

    client: FilesystemClient = pipeline.destination_client()  # type: ignore[assignment]
    data_glob = posixpath.join(client.dataset_path, "states/*")
    data_files = client.fs_client.glob(data_glob)
    assert len(data_files) > 0

    fs = sftp_filesystem
    with fs.open(data_files[0], "rb") as f:
        rows = pq.read_table(f).to_pylist()
        result_states = [r["name"] for r in rows]

        expected_states = ["DE", "AK", "CA"]
        assert sorted(result_states) == sorted(expected_states)


@pytest.mark.sftp
@pytest.mark.parametrize("load_content", (True, False))
@pytest.mark.parametrize("glob_filter", ("**", "**/*.csv", "*.txt", "met_csv/A803/*.csv"))
def test_filesystem_sftp_read(load_content: bool, glob_filter: str) -> None:
    # docker volume mount on: /home/foo/sftp/data/samples but /data/samples is the path in the SFTP server
    os.environ["SOURCES__FILESYSTEM__BUCKET_URL"] = "sftp://localhost/data/samples"
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_PORT"] = "2222"
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_USERNAME"] = "foo"
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_KEY_FILENAME"] = get_key_path()

    config = get_config()
    fs, _ = fsspec_from_config(config)

    files = fs.ls("/data/samples")

    assert len(files) > 0
    # use glob to get data
    all_file_items = list(glob_files(fs, config.bucket_url, file_glob=glob_filter))

    print(all_file_items)
    assert_sample_files(all_file_items, fs, config, load_content, glob_filter)


@pytest.mark.sftp
def test_filesystem_sftp_auth_useranme_password():
    os.environ["SOURCES__FILESYSTEM__BUCKET_URL"] = "sftp://localhost/data/samples"
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_PORT"] = "2222"
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_USERNAME"] = "foo"
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_PASSWORD"] = "pass"

    config = get_config()
    fs, _ = fsspec_from_config(config)

    files = fs.ls("/data/samples")
    assert len(files) > 0


@pytest.mark.sftp
def test_filesystem_sftp_auth_private_key():
    os.environ["SOURCES__FILESYSTEM__BUCKET_URL"] = "sftp://localhost/data/samples"
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_PORT"] = "2222"
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_USERNAME"] = "foo"
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_KEY_FILENAME"] = get_key_path()

    config = get_config()
    fs, _ = fsspec_from_config(config)

    files = fs.ls("/data/samples")

    assert len(files) > 0


@pytest.mark.sftp
def test_filesystem_sftp_auth_private_key_protected():
    os.environ["SOURCES__FILESYSTEM__BUCKET_URL"] = "sftp://localhost/data/samples"
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_PORT"] = "2222"
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_USERNAME"] = "bobby"
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_KEY_FILENAME"] = get_key_path("bobby")
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_KEY_PASSPHRASE"] = "passphrase123"

    config = get_config()
    fs, _ = fsspec_from_config(config)

    files = fs.ls("/data/samples")

    assert len(files) > 0


# Test requires - ssh_agent with user's bobby key loaded. The commands required are:
# eval "$(ssh-agent -s)"
# cp /path/to/tests/load/filesystem_sftp/bobby_rsa* ~/.ssh/id_rsa
# cp /path/to/tests/load/filesystem_sftp/bobby_rsa.pub ~/.ssh/id_rsa.pub
@pytest.mark.sftp
@pytest.mark.skipif(
    not is_ssh_agent_ready(),
    reason="SSH agent is not running or bobby's private key isn't stored in ~/.ssh/id_rsa",
)
def test_filesystem_sftp_auth_private_ssh_agent():
    os.environ["SOURCES__FILESYSTEM__BUCKET_URL"] = "sftp://localhost/data/samples"
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_PORT"] = "2222"
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_USERNAME"] = "bobby"
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_PASSWORD"] = "passphrase123"

    config = get_config()
    fs, _ = fsspec_from_config(config)

    files = fs.ls("/data/samples")

    assert len(files) > 0


@pytest.mark.sftp
def test_filesystem_sftp_auth_ca_signed_pub_key():
    os.environ["SOURCES__FILESYSTEM__BUCKET_URL"] = "sftp://localhost/data/samples"
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_PORT"] = "2222"
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_USERNAME"] = "billy"
    os.environ["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_KEY_FILENAME"] = get_key_path(
        "billy"
    )  # billy_rsa-cert.pub is automatically loaded too

    config = get_config()
    fs, _ = fsspec_from_config(config)

    files = fs.ls("/data/samples")

    assert len(files) > 0
