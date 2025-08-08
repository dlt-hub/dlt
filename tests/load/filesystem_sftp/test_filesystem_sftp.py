import os
import pytest
import fsspec
import socket
import dlt

from dlt.common.json import json
from dlt.common.configuration.inject import with_config
from dlt.common.storages import FilesystemConfiguration, fsspec_from_config
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient

from tests.load.utils import ALL_FILESYSTEM_DRIVERS

from paramiko.auth_strategy import Password
from paramiko import RSAKey, Transport
from paramiko.ssh_exception import SSHException

if "sftp" not in ALL_FILESYSTEM_DRIVERS:
    pytest.skip("sftp filesystem driver not configured", allow_module_level=True)


@with_config(spec=FilesystemConfiguration, sections=("sources", "filesystem"))
def get_config(config: FilesystemConfiguration = None) -> FilesystemConfiguration:
    return config


def get_key_path(user: str = "foo") -> str:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, f"bootstrap/{user}_rsa")


def files_are_equal(file1_path, file2_path):
    try:
        with (
            open(file1_path, "r", encoding="utf-8") as f1,
            open(file2_path, "r", encoding="utf-8") as f2,
        ):
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


def test_filesystem_sftp_pipeline(sftp_filesystem):
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


def run_sftp_auth(
    user,
    password=None,
    pkey=None,
    key=None,
    passphrase=None,
    sock=None,
    disabled_algorithms=None,
    transport_factory=None,
    auth_strategy=None,
):
    env_vars = {
        "SOURCES__FILESYSTEM__BUCKET_URL": "sftp://localhost",
        "SOURCES__FILESYSTEM__CREDENTIALS__SFTP_PORT": "2222",
        "SOURCES__FILESYSTEM__CREDENTIALS__SFTP_USERNAME": user,
    }

    if password:
        env_vars["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_PASSWORD"] = password
    if key:
        env_vars["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_KEY_FILENAME"] = get_key_path(user)
    if passphrase:
        env_vars["SOURCES__FILESYSTEM__CREDENTIALS__SFTP_KEY_PASSPHRASE"] = passphrase

    os.environ.update(env_vars)

    config = get_config()

    if disabled_algorithms:
        config.credentials.sftp_disabled_algorithms = disabled_algorithms  # type: ignore[union-attr]
    if transport_factory:
        config.credentials.sftp_transport_factory = transport_factory  # type: ignore[union-attr]
    if auth_strategy:
        config.credentials.sftp_auth_strategy = auth_strategy  # type: ignore[union-attr]
    if pkey:
        config.credentials.sftp_pkey = pkey  # type: ignore[union-attr]
    if sock:
        config.credentials.sftp_sock = sock  # type: ignore[union-attr]

    fs, _ = fsspec_from_config(config)
    assert len(fs.ls("/data/standard_source/samples")) > 0


def test_filesystem_sftp_auth_useranme_password():
    run_sftp_auth("foo", "pass")


def test_filesystem_sftp_auth_private_key():
    run_sftp_auth("foo", key=get_key_path())


def test_filesystem_sftp_auth_private_key_protected():
    run_sftp_auth("bobby", key=get_key_path("bobby"), passphrase="passphrase123")


def test_filesystem_sftp_auth_pkey():
    run_sftp_auth("foo", pkey=RSAKey.from_private_key_file(get_key_path("foo")))


def test_filesystem_sftp_pkey_auth_pkey_protected():
    run_sftp_auth(
        "bobby",
        pkey=RSAKey.from_private_key_file(filename=get_key_path("bobby"), password="passphrase123"),
    )


# Test requires - ssh_agent with user's bobby key loaded. The commands and file names required are:
# eval "$(ssh-agent -s)"
# cp /path/to/tests/load/filesystem_sftp/bobby_rsa* ~/.ssh/id_rsa
# cp /path/to/tests/load/filesystem_sftp/bobby_rsa.pub ~/.ssh/id_rsa.pub
# ssh-add ~/.ssh/id_rsa
@pytest.mark.skipif(
    not is_ssh_agent_ready(),
    reason="SSH agent is not running or bobby's private key isn't stored in ~/.ssh/id_rsa",
)
def test_filesystem_sftp_auth_private_ssh_agent():
    run_sftp_auth("bobby", passphrase="passphrase123")


def test_filesystem_sftp_auth_ca_signed_pub_key():
    # billy_rsa-cert.pub is automatically loaded too
    run_sftp_auth("billy", key=get_key_path("billy"))


def test_filesystem_sftp_with_socket():
    # Create socket and use in combination with auth
    sock = socket.create_connection(("localhost", 2222), timeout=5)
    run_sftp_auth("billy", key=get_key_path("billy"), sock=sock)

    # Close socket now to ensure socket was being used
    sock.close()
    with pytest.raises(OSError):
        run_sftp_auth("billy", key=get_key_path("billy"), sock=sock)


class TaggedTransport(Transport):
    """A Transport class that tags itself so we can detect it in tests."""

    def __init__(self, sock, **kwargs):
        super().__init__(sock, **kwargs)
        # Add any custom state or markers you like:
        self.factory_tag = "used-tagged-transport"


def test_filesystem_sftp_with_tagged_transport():
    created = []

    def factory(sock, **kwargs):
        t = TaggedTransport(sock, **kwargs)
        created.append(t)
        return t

    run_sftp_auth("foo", key=get_key_path("foo"), transport_factory=factory)

    # Verify it was used
    assert len(created) == 1, "Custom transport factory never ran"
    transport = created[0]
    assert isinstance(transport, TaggedTransport)
    assert transport.factory_tag == "used-tagged-transport"

    # And ensure it's still functional
    sftp = transport.open_sftp_client()
    assert hasattr(sftp, "listdir")
    sftp.close()


def test_filesystem_sftp_disabled_algorithms():
    # we know foo’s server uses rsa keys
    with pytest.raises(SSHException):
        run_sftp_auth(
            "foo",
            key=get_key_path("foo"),
            disabled_algorithms={"pubkeys": ["ssh-rsa", "rsa-sha2-256", "rsa-sha2-512"]},
        )


def test_filesystem_sftp_auth_strategy():
    # Verify that passing an alternate auth_strategy makes it through config.
    run_sftp_auth(
        "foo",
        auth_strategy=Password("foo", lambda: "pass"),
    )
