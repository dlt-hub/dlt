import dataclasses
from typing import Dict, Literal, Optional, Final
from typing_extensions import Annotated
from urllib.parse import urlparse

from dlt.common.configuration import configspec, NotResolved
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration
from dlt.common.destination.reference import DestinationClientDwhConfiguration
from dlt.common.utils import digest128

TWeaviateBatchConsistency = Literal["ONE", "QUORUM", "ALL"]


@configspec
class WeaviateCredentials(CredentialsConfiguration):
    url: str = "http://localhost:8080"
    api_key: Optional[str] = None
    additional_headers: Optional[Dict[str, str]] = None

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

    def fingerprint(self) -> str:
        """Returns a fingerprint of host part of a connection string"""

        if self.credentials and self.credentials.url:
            hostname = urlparse(self.credentials.url).hostname
            return digest128(hostname)
        return ""
