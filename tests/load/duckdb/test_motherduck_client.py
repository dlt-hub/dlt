import os
import pytest

from dlt.common.configuration.resolve import resolve_configuration

from dlt.destinations.motherduck.configuration import MotherDuckCredentials, MotherDuckClientConfiguration

from tests.utils import patch_home_dir, preserve_environ, ALL_DESTINATIONS


if 'motherduck' not in ALL_DESTINATIONS:
    pytest.skip("motherduck is not configured", allow_module_level=True)


def test_motherduck_database() -> None:
    # set HOME env otherwise some internal components in ducdkb (HTTPS) do not initialize
    os.environ["HOME"] = "/tmp"
    # os.environ.pop("HOME", None)

    cred = MotherDuckCredentials("md:///?token=TOKEN")
    assert cred.password == "TOKEN"
    cred = MotherDuckCredentials()
    cred.parse_native_representation("md:///?token=TOKEN")
    assert cred.password == "TOKEN"

    config = resolve_configuration(MotherDuckClientConfiguration(dataset_name="test"), sections=("destination", "motherduck"))
    # connect
    con = config.credentials.borrow_conn(read_only=False)
    con.sql("SHOW DATABASES")
    config.credentials.return_conn(con)
