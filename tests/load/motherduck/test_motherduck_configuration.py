import pytest

from dlt.common.utils import digest128
from dlt.destinations import duckdb as duckdb_factory, motherduck as motherduck_factory
from dlt.destinations.impl.duckdb.configuration import (
    DuckDbClientConfiguration,
    DuckDbCredentials,
)
from dlt.destinations.impl.duckdb.sql_client import DuckDbSqlClient
from dlt.destinations.impl.motherduck.configuration import (
    MotherDuckClientConfiguration,
    MotherDuckCredentials,
)
from dlt.destinations.impl.motherduck.sql_client import MotherDuckSqlClient

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "connection_string,expected_fingerprint",
    [
        pytest.param("", "", id="empty"),
        pytest.param(
            "md:///dlt_data?token=TOKEN",
            digest128("TOKEN"),
            id="legacy_token_query_param",
        ),
        pytest.param(
            "md:///dlt_data?motherduck_token=TOKEN",
            digest128("TOKEN"),
            id="legacy_motherduck_token_query_param",
        ),
    ],
)
def test_motherduck_fingerprint(connection_string: str, expected_fingerprint: str) -> None:
    if connection_string:
        credentials = MotherDuckCredentials(connection_string)
        config = MotherDuckClientConfiguration(credentials=credentials)
    else:
        config = MotherDuckClientConfiguration()

    assert config.fingerprint() == expected_fingerprint


def test_motherduck_fingerprint_uses_token_not_physical_location() -> None:
    config = MotherDuckClientConfiguration(
        credentials=MotherDuckCredentials("md:///dlt_data?token=TOKEN")
    )

    assert config.physical_location() == ""
    assert config.fingerprint() == digest128("TOKEN")


def test_motherduck_get_attach() -> None:
    credentials = MotherDuckCredentials("md:my_db?motherduck_token=TOKEN")
    config = MotherDuckClientConfiguration(credentials=credentials)._bind_dataset_name("ds_a")
    sql_client = MotherDuckSqlClient(
        "ds_a", "ds_a_staging", credentials, motherduck_factory().capabilities(config)
    )

    info = sql_client.get_attach(alias="attach_ds_a")
    assert info["attach_type"] == "motherduck"
    assert info["alias"] == "attach_ds_a"
    assert info["physical_location"] == "md:my_db"
    # extension + ATTACH stay cleartext; only the token line is a secret (encrypted when persisted)
    assert info["statements"] == [
        {"sql": "INSTALL motherduck", "secret": False},
        {"sql": "LOAD motherduck", "secret": False},
        {"sql": "SET motherduck_token='TOKEN'", "secret": True},
        {"sql": "ATTACH IF NOT EXISTS 'md:my_db' AS \"attach_ds_a\"", "secret": False},
    ]


def test_motherduck_cannot_execute_motherduck_attach() -> None:
    """MotherDuck can be attached into duckdb, but not into another MotherDuck connection."""
    credentials = MotherDuckCredentials("md:my_db?motherduck_token=TOKEN")
    config = MotherDuckClientConfiguration(credentials=credentials)._bind_dataset_name("ds_a")
    md_client = MotherDuckSqlClient(
        "ds_a", "ds_a_staging", credentials, motherduck_factory().capabilities(config)
    )
    duck_credentials = DuckDbCredentials(":memory:")
    duck_config = DuckDbClientConfiguration(credentials=duck_credentials)._bind_dataset_name("ds_b")
    duck_client = DuckDbSqlClient(
        "ds_b", "ds_b_staging", duck_credentials, duckdb_factory().capabilities(duck_config)
    )

    # a token can only be set before a connection is initialized, and MotherDuck rejects catalog
    # aliases in workspace mode
    assert md_client.can_attach("motherduck") is False
    # local databases and scanner views are plain duckdb attaches and stay allowed
    assert md_client.can_attach("duckdb") is True
    assert duck_client.can_attach("motherduck") is True
    assert duck_client.can_attach("duckdb") is True
