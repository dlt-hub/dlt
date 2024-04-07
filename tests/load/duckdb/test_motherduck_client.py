import os
import pytest

from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.resolve import resolve_configuration

from dlt.destinations.impl.motherduck.configuration import (
    MotherDuckCredentials,
    MotherDuckClientConfiguration,
)

from tests.utils import patch_home_dir, preserve_environ, skip_if_not_active

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential

skip_if_not_active("motherduck")


def test_motherduck_configuration() -> None:
    cred = MotherDuckCredentials("md:///dlt_data?token=TOKEN")
    # print(dict(cred))
    assert cred.password == "TOKEN"
    assert cred.database == "dlt_data"
    assert cred.is_partial() is False
    assert cred.is_resolved() is True

    cred = MotherDuckCredentials()
    cred.parse_native_representation("md:///?token=TOKEN")
    assert cred.password == "TOKEN"
    assert cred.database == ""
    assert cred.is_partial() is False
    assert cred.is_resolved() is False

    # password or token are mandatory
    with pytest.raises(ConfigFieldMissingException) as conf_ex:
        resolve_configuration(MotherDuckCredentials())
    assert conf_ex.value.fields == ["password"]

    os.environ["CREDENTIALS__PASSWORD"] = "pwd"
    config = resolve_configuration(MotherDuckCredentials())
    assert config.password == "pwd"

    del os.environ["CREDENTIALS__PASSWORD"]
    os.environ["CREDENTIALS__QUERY"] = '{"token": "tok"}'
    config = resolve_configuration(MotherDuckCredentials())
    assert config.password == "tok"


def test_motherduck_connect() -> None:
    # set HOME env otherwise some internal components in ducdkb (HTTPS) do not initialize
    os.environ["HOME"] = "/tmp"

    config = resolve_configuration(
        MotherDuckClientConfiguration()._bind_dataset_name(dataset_name="test"),
        sections=("destination", "motherduck"),
    )
    # connect
    con = config.credentials.borrow_conn(read_only=False)
    con.sql("SHOW DATABASES")
    config.credentials.return_conn(con)
