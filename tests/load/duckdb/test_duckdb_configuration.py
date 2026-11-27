from pathlib import Path
from typing import Optional

import duckdb
import pytest

from dlt.common.configuration import resolve_configuration
from dlt.destinations.impl.duckdb.configuration import (
    DuckDbClientConfiguration,
    DuckDbCredentials,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "credentials,expected_fingerprint",
    [
        pytest.param(None, "", id="empty"),
        pytest.param(
            DuckDbCredentials(":memory:"),
            "",
            id="memory_database",
        ),
        pytest.param(
            DuckDbCredentials("local.duckdb"),
            "",
            id="database_path",
        ),
    ],
)
def test_duckdb_fingerprint(
    credentials: Optional[DuckDbCredentials], expected_fingerprint: str
) -> None:
    config = DuckDbClientConfiguration(credentials=credentials)

    assert config.fingerprint() == expected_fingerprint


def test_external_connection_data_location(tmp_path: Path) -> None:
    """Connection-passed credentials must identify the real database file. dlt then separates a
    dataset that it can join directly from a dataset that needs an attach.
    """
    conn_a = duckdb.connect(str(tmp_path / "a.duckdb"))
    conn_b = duckdb.connect(str(tmp_path / "b.duckdb"))
    conn_mem = duckdb.connect()
    try:
        config_a = resolve_configuration(
            DuckDbClientConfiguration(credentials=conn_a)._bind_dataset_name(dataset_name="ds")
        )
        config_a2 = DuckDbClientConfiguration(credentials=DuckDbCredentials(conn_a))
        config_b = DuckDbClientConfiguration(credentials=DuckDbCredentials(conn_b))
        config_mem = DuckDbClientConfiguration(credentials=DuckDbCredentials(conn_mem))

        # real file path survives config resolution (make_location keeps absolute paths)
        assert config_a.data_location() == str(tmp_path / "a.duckdb")
        # a connection reads the same database file directly and attaches another file to read it
        assert config_a.can_read_from(config_a2)
        assert config_a.can_read_from(config_b)
        assert config_mem.can_read_from(config_a)
        # an in-memory connection has no path to name, so the marker carries the connection
        # identity. without this identity, any two in-memory connections look like one database
        assert config_mem.data_location() == f":external:{hex(id(conn_mem))}"
        assert config_mem.can_read_from(
            DuckDbClientConfiguration(credentials=DuckDbCredentials(conn_mem))
        )
        # a connection cannot attach a database that lives inside another connection
        assert not config_a.can_read_from(config_mem)

        # two in-memory connections are two unrelated databases
        config_mem_2 = DuckDbClientConfiguration(credentials=DuckDbCredentials(duckdb.connect()))
        assert config_mem.data_location() != config_mem_2.data_location()
        assert not config_mem.can_read_from(config_mem_2)
        # nothing can attach a database that lives inside a connection
        assert config_mem.attach_type() is None
        assert config_a.attach_type() == "duckdb"
    finally:
        conn_a.close()
        conn_b.close()
        conn_mem.close()
