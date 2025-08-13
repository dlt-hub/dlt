from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any, Final, Optional, Union

from dlt.common.configuration import configspec
from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.utils import digest128
from dlt.destinations.impl.duckdb.configuration import DuckDbBaseCredentials

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection


@configspec(init=False)
class DuckLakeCredentials(DuckDbBaseCredentials):
    def __init__(
        self,
        conn_or_path: Optional[Union[str, DuckDBPyConnection]] = None,
        *,
        read_only: bool = False,
        extensions: Optional[list[str]] = None,
        global_config: Optional[dict[str, Any]] = None,
        pragmas: Optional[list[str]] = None,
        local_config: Optional[dict[str, Any]] = None,
    ) -> None:
        """Initialize DuckDB credentials with a connection or file path and connection settings.

        Args:
            conn_or_path: Either a DuckDB connection object or a path to a DuckDB database file.
                          Can also be special values like ':pipeline:' or ':memory:'.
            read_only: Open database in read-only mode if True, read-write mode if False
            extensions: List of DuckDB extensions to load on each newly opened connection
            global_config: Dictionary of global configuration settings applied once on each newly opened connection
            pragmas: List of PRAGMA statements to be applied to each cursor connection
            local_config: Dictionary of local configuration settings applied to each cursor connection
        """
        self._apply_init_value(conn_or_path)
        self.read_only = read_only
        self.global_config = global_config
        self.pragmas = pragmas
        self.local_config = local_config

        # ensure `ducklake` extension is included
        if extensions:
            _extensions = set(extensions)
            _extensions.add("ducklake")
            self.extensions = list(_extensions)
    


@configspec
class DuckLakeClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = dataclasses.field(
        default="ducklake",
        init=False,
        repr=False,
        compare=False,
    )
    credentials: Optional[DuckLakeCredentials] = None
    create_indexes: bool = False

    def fingerprint(self) -> str:
        """Returns a fingerprint of user access token"""
        if self.credentials and self.credentials.password:
            return digest128(self.credentials.password)
        return ""