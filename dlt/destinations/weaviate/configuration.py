from typing import Dict, Literal, Optional, Final
from dataclasses import field
from urllib.parse import urlparse

from dlt.common.configuration import configspec
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration
from dlt.common.destination.reference import DestinationClientDwhConfiguration
from dlt.common.utils import digest128

TWeaviateBatchConsistency = Literal["ONE", "QUORUM", "ALL"]


@configspec
class WeaviateCredentials(CredentialsConfiguration):
    url: str
    api_key: str
    additional_headers: Optional[Dict[str, str]] = None

    def __str__(self) -> str:
        """Used to display user friendly data location"""
        # assuming no password in url scheme for Weaviate
        return self.url


@configspec
class WeaviateClientConfiguration(DestinationClientDwhConfiguration):
    destination_name: Final[str] = "weaviate"  # type: ignore
    dataset_name: Optional[str] = None  # make it optional do empty dataset is allowed

    batch_size: int = 100
    batch_workers: int = 1
    batch_consistency: TWeaviateBatchConsistency = "ONE"
    batch_retries: int = 5
    conn_timeout: int = 10
    read_timeout: int = 3*60
    dataset_separator: str = "_"

    credentials: WeaviateCredentials
    vectorizer: str = "text2vec-openai"
    module_config: Dict[str, Dict[str, str]] = field(default_factory=lambda: {
        "text2vec-openai": {
            "model": "ada",
            "modelVersion": "002",
            "type": "text",
        }
    })

    def fingerprint(self) -> str:
        """Returns a fingerprint of host part of a connection string"""

        if self.credentials and self.credentials.url:
            hostname = urlparse(self.credentials.url).hostname
            return digest128(hostname)
        return ""
