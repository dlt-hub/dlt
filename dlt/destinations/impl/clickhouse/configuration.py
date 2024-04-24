import dataclasses
from typing import ClassVar, List, Any, Final, Literal, cast, Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.destination.reference import (
    DestinationClientDwhWithStagingConfiguration,
)
from dlt.common.libs.sql_alchemy import URL
from dlt.common.utils import digest128


TSecureConnection = Literal[0, 1]


@configspec(init=False)
class ClickHouseCredentials(ConnectionStringCredentials):
    drivername: str = "clickhouse"
    host: str = None
    """Host with running ClickHouse server."""
    port: int = 9440
    """Native port ClickHouse server is bound to. Defaults to 9440."""
    http_port: int = 8443
    """HTTP Port to connect to ClickHouse server's HTTP interface.
    The HTTP port is needed for non-staging pipelines.
     Defaults to 8123."""
    username: str = "default"
    """Database user. Defaults to 'default'."""
    database: str = "default"
    """database connect to. Defaults to 'default'."""
    secure: TSecureConnection = 1
    """Enables TLS encryption when connecting to ClickHouse Server. 0 means no encryption, 1 means encrypted."""
    connect_timeout: int = 15
    """Timeout for establishing connection. Defaults to 10 seconds."""
    send_receive_timeout: int = 300
    """Timeout for sending and receiving data. Defaults to 300 seconds."""
    dataset_table_separator: str = "___"
    """Separator for dataset table names, defaults to '___', i.e. 'database.dataset___table'."""
    dataset_sentinel_table_name: str = "dlt_sentinel_table"
    """Special table to mark dataset as existing"""
    gcp_access_key_id: Optional[str] = None
    """When loading from a gcp bucket, you need to provide gcp interoperable keys"""
    gcp_secret_access_key: Optional[str] = None
    """When loading from a gcp bucket, you need to provide gcp interoperable keys"""

    __config_gen_annotations__: ClassVar[List[str]] = [
        "host",
        "port",
        "http_port",
        "database",
        "username",
        "password",
    ]

    def parse_native_representation(self, native_value: Any) -> None:
        super().parse_native_representation(native_value)
        self.connect_timeout = int(self.query.get("connect_timeout", self.connect_timeout))
        self.send_receive_timeout = int(
            self.query.get("send_receive_timeout", self.send_receive_timeout)
        )
        self.secure = cast(TSecureConnection, int(self.query.get("secure", self.secure)))
        if not self.is_partial():
            self.resolve()

    def to_url(self) -> URL:
        url = super().to_url()
        url = url.update_query_pairs(
            [
                ("connect_timeout", str(self.connect_timeout)),
                ("send_receive_timeout", str(self.send_receive_timeout)),
                ("secure", str(1) if self.secure else str(0)),
                # Toggle experimental settings. These are necessary for certain datatypes and not optional.
                ("allow_experimental_lightweight_delete", "1"),
                # ("allow_experimental_object_type", "1"),
                ("enable_http_compression", "1"),
            ]
        )
        return url


@configspec
class ClickHouseClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = dataclasses.field(default="clickhouse", init=False, repr=False, compare=False)  # type: ignore[misc]
    credentials: ClickHouseCredentials = None

    # Primary key columns are used to build a sparse primary index which allows for efficient data retrieval,
    # but they do not enforce uniqueness constraints. It permits duplicate values even for the primary key
    # columns within the same granule.
    # See: https://clickhouse.com/docs/en/optimize/sparse-primary-indexes

    def fingerprint(self) -> str:
        """Returns a fingerprint of host part of a connection string."""
        if self.credentials and self.credentials.host:
            return digest128(self.credentials.host)
        return ""
