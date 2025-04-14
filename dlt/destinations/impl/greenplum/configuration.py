import dataclasses
from typing import Any, ClassVar, Dict, Final, List, Optional

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.data_writers.configuration import CsvFormatConfiguration
from dlt.common.destination.client import (
    DestinationClientDwhWithStagingConfiguration,
)
from dlt.common.typing import TSecretStrValue
from dlt.common.utils import digest128


@configspec(init=False)
@dataclasses.dataclass
class GreenplumCredentials(ConnectionStringCredentials):
    drivername: Final[str] = dataclasses.field(default="postgresql", init=False, repr=False, compare=False)  # type: ignore
    database: str = None
    username: str = None
    password: TSecretStrValue = None
    host: str = None
    port: int = 5432
    connect_timeout: int = 15
    client_encoding: Optional[str] = None

    __config_gen_annotations__: ClassVar[List[str]] = [
        "port",
        "connect_timeout",
    ]

    def parse_native_representation(self, native_value: Any) -> None:
        # First use parent method to parse the connection string
        super().parse_native_representation(native_value)
        
        # Then process additional parameters
        self.connect_timeout = int(
            self.query.get("connect_timeout", self.connect_timeout)
        )
        self.client_encoding = self.query.get(
            "client_encoding", self.client_encoding
        )

    def get_query(self) -> Dict[str, Any]:
        query = dict(super().get_query())
        query["connect_timeout"] = self.connect_timeout
        if self.client_encoding:
            query["client_encoding"] = self.client_encoding
        return query


@configspec
@dataclasses.dataclass
class GreenplumClientConfiguration(
    DestinationClientDwhWithStagingConfiguration
):
    destination_type: Final[str] = dataclasses.field(default="greenplum", init=False, repr=False, compare=False)  # type: ignore
    credentials: GreenplumCredentials = None

    create_indexes: bool = True
    
    # Greenplum specific table storage options
    appendonly: bool = True
    blocksize: int = 32768
    compresstype: str = "zstd"
    compresslevel: int = 4
    orientation: str = "column"
    distribution_key: str = "_dlt_id"  # Default distribution key

    csv_format: Optional[CsvFormatConfiguration] = None
    """Optional csv format configuration"""

    def fingerprint(self) -> str:
        """Returns a fingerprint of host part of a connection string"""
        if self.credentials and self.credentials.host:
            return digest128(self.credentials.host)
        return ""