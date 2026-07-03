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


def test_external_connection_physical_location(tmp_path: Path) -> None:
    """Connection-passed credentials must identify the real database file so two
    different databases are not considered join-compatible.
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
        assert config_a.physical_location() == str(tmp_path / "a.duckdb")
        # same database file: joinable, different files or memory: not
        assert config_a.can_read_from(config_a2)
        assert not config_a.can_read_from(config_b)
        assert not config_mem.can_read_from(config_a)
        # in-memory connections keep the external marker and stay compatible when shared
        assert config_mem.physical_location() == ":external:"
        assert config_mem.can_read_from(
            DuckDbClientConfiguration(credentials=DuckDbCredentials(conn_mem))
        )
    finally:
        conn_a.close()
        conn_b.close()
        conn_mem.close()
