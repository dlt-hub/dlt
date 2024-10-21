import os
from typing import Optional

import duckdb
import pytest

from pytest_mock import MockerFixture
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.resolve import resolve_configuration

from dlt.destinations.impl.motherduck.configuration import (
    MOTHERDUCK_USER_AGENT,
    MotherDuckCredentials,
    MotherDuckClientConfiguration,
)

from tests.utils import patch_home_dir, skip_if_not_active

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential

skip_if_not_active("motherduck")


def test_motherduck_configuration() -> None:
    cred = MotherDuckCredentials("md:///dlt_data?token=TOKEN")
    # print(dict(cred))
    assert cred.password == "TOKEN"
    assert cred.database == "dlt_data"
    assert cred.is_partial() is False
    assert cred.is_resolved() is False

    cred = MotherDuckCredentials()
    cred.parse_native_representation("md:///?motherduck_token=TOKEN")
    assert cred.password == "TOKEN"
    assert cred.database == ""
    assert cred.is_partial() is False
    assert cred.is_resolved() is False

    cred = MotherDuckCredentials()
    cred.parse_native_representation("md:xdb?motherduck_token=TOKEN2")
    assert cred.password == "TOKEN2"
    assert cred.database == "xdb"
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


def test_motherduck_connect_default_token() -> None:
    import dlt

    credentials = dlt.secrets.get(
        "destination.motherduck.credentials", expected_type=MotherDuckCredentials
    )
    assert credentials.password
    os.environ["motherduck_token"] = credentials.password

    credentials = MotherDuckCredentials()
    assert credentials._has_default_token() is True
    credentials.on_partial()
    assert credentials.is_resolved()

    config = MotherDuckClientConfiguration(credentials=credentials)
    print(config.credentials._conn_str())
    # connect
    con = config.credentials.borrow_conn(read_only=False)
    con.sql("SHOW DATABASES")
    config.credentials.return_conn(con)


@pytest.mark.parametrize("custom_user_agent", [MOTHERDUCK_USER_AGENT, "patates", None, ""])
def test_motherduck_connect_with_user_agent_string(
    custom_user_agent: Optional[str], mocker: MockerFixture
) -> None:
    # set HOME env otherwise some internal components in ducdkb (HTTPS) do not initialize
    os.environ["HOME"] = "/tmp"

    connect_spy = mocker.spy(duckdb, "connect")
    config = resolve_configuration(
        MotherDuckClientConfiguration()._bind_dataset_name(dataset_name="test"),
        sections=("destination", "motherduck"),
    )

    config.credentials.custom_user_agent = custom_user_agent

    # connect
    con = config.credentials.borrow_conn(read_only=False)
    con.sql("SHOW DATABASES")
    config.credentials.return_conn(con)

    # check for the default user agent value
    connect_spy.assert_called()
    assert "config" in connect_spy.call_args.kwargs
    # If empty string "" was set then we should not include it
    if custom_user_agent == "":
        assert "custom_user_agent" not in connect_spy.call_args.kwargs["config"]
    # if it is not specified, we expect the default `MOTHERDUCK_USER_AGENT``
    elif custom_user_agent is None:
        assert connect_spy.call_args.kwargs["config"]["custom_user_agent"] == MOTHERDUCK_USER_AGENT
    # otherwise all other values should be present
    else:
        assert connect_spy.call_args.kwargs["config"]["custom_user_agent"] == custom_user_agent
