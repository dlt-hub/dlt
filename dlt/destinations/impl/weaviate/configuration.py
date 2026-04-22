import dataclasses
from typing import Dict, Literal, Optional, Final
from typing_extensions import Annotated
from urllib.parse import urlparse

from dlt.common.configuration import configspec, NotResolved
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration
from dlt.common.destination.client import (
    DestinationClientConfiguration,
    DestinationClientDwhConfiguration,
)

TWeaviateBatchConsistency = Literal["ONE", "QUORUM", "ALL"]
TWeaviateConnectionType = Literal["cloud", "local", "custom"]


@configspec
class WeaviateCredentials(CredentialsConfiguration):
    url: str = "http://localhost:8080"
    api_key: Optional[str] = None
    additional_headers: Optional[Dict[str, str]] = None
    # Optional ports for custom/local connections
    http_port: Optional[int] = None
    grpc_port: Optional[int] = None

    def __str__(self) -> str:
        """Used to display user friendly data location"""
        # assuming no password in url scheme for Weaviate
        return self.url


@configspec
class WeaviateClientConfiguration(DestinationClientDwhConfiguration):
    destination_type: Final[str] = dataclasses.field(default="weaviate", init=False, repr=False, compare=False)  # type: ignore
    # make it optional so empty dataset is allowed
    dataset_name: Annotated[Optional[str], NotResolved()] = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )

    batch_size: int = 100
    batch_workers: int = 1
    batch_consistency: TWeaviateBatchConsistency = "ONE"
    batch_retries: int = 5

    conn_timeout: float = 10.0
    read_timeout: float = 3 * 60.0
    startup_period: int = 5

    dataset_separator: str = "_"

    # Connection type: "cloud" for Weaviate Cloud, "local" for Docker, "custom" for self-hosted
    # If None, auto-detected from URL pattern
    connection_type: Optional[TWeaviateConnectionType] = None

    credentials: WeaviateCredentials = None
    vectorizer: str = "text2vec-openai"
    module_config: Dict[str, Dict[str, str]] = dataclasses.field(
        default_factory=lambda: {
            "text2vec-openai": {
                "model": "ada",
                "modelVersion": "002",
                "type": "text",
            }
        }
    )

    def physical_destination(self) -> str:
        """Returns the host part of the connection URL."""
        if self.credentials and self.credentials.url:
            return urlparse(self.credentials.url).hostname or ""
        return ""

    def can_join_with(self, other: DestinationClientConfiguration) -> bool:
        """Weaviate does not support dlt SQL joins."""
        return False
