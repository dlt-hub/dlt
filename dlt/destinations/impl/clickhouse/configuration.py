import dataclasses
from typing import ClassVar, Dict, List, Any, Final, cast, Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.destination.reference import (
    DestinationClientDwhWithStagingConfiguration,
)
from dlt.common.utils import digest128
from dlt.destinations.impl.clickhouse.typing import TSecureConnection, TTableEngineType


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

    def get_query(self) -> Dict[str, Any]:
        query = dict(super().get_query())
        query.update(
            {
                "connect_timeout": str(self.connect_timeout),
                "send_receive_timeout": str(self.send_receive_timeout),
                "secure": 1 if self.secure else 0,
                "allow_experimental_lightweight_delete": 1,
                "enable_http_compression": 1,
                "date_time_input_format": "best_effort",
            }
        )
        return query


@configspec
class ClickHouseClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = dataclasses.field(  # type: ignore[misc]
        default="clickhouse", init=False, repr=False, compare=False
    )
    credentials: ClickHouseCredentials = None

    dataset_table_separator: str = "___"
    """Separator for dataset table names, defaults to '___', i.e. 'database.dataset___table'."""
    table_engine_type: Optional[TTableEngineType] = "merge_tree"
    """The default table engine to use. Defaults to 'merge_tree'. Other implemented options are 'shared_merge_tree' and 'replicated_merge_tree'."""
    dataset_sentinel_table_name: str = "dlt_sentinel_table"
    """Special table to mark dataset as existing"""

    __config_gen_annotations__: ClassVar[List[str]] = [
        "dataset_table_separator",
        "dataset_sentinel_table_name",
        "table_engine_type",
    ]

    def fingerprint(self) -> str:
        """Returns a fingerprint of the host part of a connection string."""
        if self.credentials and self.credentials.host:
            return digest128(self.credentials.host)
        return ""
