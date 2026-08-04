import pytest

from dlt.common.utils import digest128
from dlt.common.destination.attach import attach_statement
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


def test_motherduck_data_location_digests_the_token() -> None:
    """The token is the only account identity, so the location carries a digest of the token."""
    config = MotherDuckClientConfiguration(
        credentials=MotherDuckCredentials("md:///dlt_data?token=TOKEN")
    )

    assert config.fingerprint() == digest128("TOKEN")
    assert config.data_location() == f"md://{digest128('TOKEN')}"
    assert "TOKEN" not in config.data_location()


def test_motherduck_attach_statements() -> None:
    credentials = MotherDuckCredentials("md:my_db?motherduck_token=TOKEN")
    config = MotherDuckClientConfiguration(credentials=credentials)._bind_dataset_name("ds_a")
    sql_client = MotherDuckSqlClient(
        "ds_a", "ds_a_staging", credentials, motherduck_factory().capabilities(config)
    )

    assert config.attach_type() == "motherduck"
    # `LOAD` installs the extension automatically, so dlt emits no `INSTALL`. the `LOAD` line and
    # the `ATTACH` line stay cleartext. only the token line is a secret, and dlt encrypts it in
    # the model file
    assert sql_client.attach_statements(alias="attach_ds_a") == [
        attach_statement("LOAD motherduck"),
        attach_statement("SET motherduck_token=E'TOKEN'", secret=True, key="attach_ds_a:token"),
        attach_statement("ATTACH IF NOT EXISTS E'md:my_db' AS \"attach_ds_a\""),
    ]

    # a quote in the token cannot terminate the string literal that holds it
    credentials.password = "TO'KEN"
    assert (
        sql_client.attach_statements(alias="attach_ds_a")[1]["sql"]
        == "SET motherduck_token=E'TO''KEN'"
    )


def test_motherduck_cannot_execute_motherduck_attach() -> None:
    """A duckdb connection can attach MotherDuck, but another MotherDuck connection cannot."""
    credentials = MotherDuckCredentials("md:my_db?motherduck_token=TOKEN")
    md_config = MotherDuckClientConfiguration(credentials=credentials)._bind_dataset_name("ds_a")
    duck_credentials = DuckDbCredentials(":memory:")
    duck_config = DuckDbClientConfiguration(credentials=duck_credentials)._bind_dataset_name("ds_b")

    # dlt can set a token only before it initializes a connection. MotherDuck also rejects catalog
    # aliases in workspace mode
    assert md_config.can_attach("motherduck") is False
    # a local database and a scanner view attach as plain duckdb databases, so dlt allows both
    assert md_config.can_attach("duckdb") is True
    assert duck_config.can_attach("motherduck") is True
    assert duck_config.can_attach("duckdb") is True
