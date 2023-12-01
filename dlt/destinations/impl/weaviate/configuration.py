from typing import Dict, Literal, Optional, Final, TYPE_CHECKING
from dataclasses import field
from urllib.parse import urlparse

from dlt.common.configuration import configspec
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration
from dlt.common.destination.reference import DestinationClientDwhConfiguration
from dlt.common.utils import digest128

TWeaviateBatchConsistency = Literal["ONE", "QUORUM", "ALL"]


@configspec
class WeaviateCredentials(CredentialsConfiguration):
    url: str = "http://localhost:8080"
    api_key: Optional[str]
    additional_headers: Optional[Dict[str, str]] = None

    def __str__(self) -> str:
        """Used to display user friendly data location"""
        # assuming no password in url scheme for Weaviate
        return self.url


@configspec
class WeaviateClientConfiguration(DestinationClientDwhConfiguration):
    destination_type: Final[str] = "weaviate"  # type: ignore
    # make it optional so empty dataset is allowed
    dataset_name: Optional[str] = None  # type: ignore[misc]

    batch_size: int = 100
    batch_workers: int = 1
    batch_consistency: TWeaviateBatchConsistency = "ONE"
    batch_retries: int = 5

    conn_timeout: float = 10.0
    read_timeout: float = 3 * 60.0
    startup_period: int = 5

    dataset_separator: str = "_"

    credentials: WeaviateCredentials
    vectorizer: str = "text2vec-openai"
    module_config: Dict[str, Dict[str, str]] = field(
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

    if TYPE_CHECKING:

        def __init__(
            self,
            *,
            destination_type: str = None,
            credentials: WeaviateCredentials = None,
            name: str = None,
            environment: str = None,
            dataset_name: str = None,
            default_schema_name: str = None,
            batch_size: int = None,
            batch_workers: int = None,
            batch_consistency: TWeaviateBatchConsistency = None,
            batch_retries: int = None,
            conn_timeout: float = None,
            read_timeout: float = None,
            startup_period: int = None,
            dataset_separator: str = None,
            vectorizer: str = None,
            module_config: Dict[str, Dict[str, str]] = None,
        ) -> None: ...
