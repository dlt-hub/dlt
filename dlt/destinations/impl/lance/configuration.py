import os
import dataclasses
from typing import Any, Dict, Optional, Final, Literal, ClassVar, List

from dlt.common import logger
from dlt.common.configuration import configspec
from dlt.common.configuration.specs.base_configuration import (
    BaseConfiguration,
    CredentialsConfiguration,
)
from dlt.common.destination.client import DestinationClientDwhConfiguration
from dlt.common.storages.configuration import FilesystemConfiguration, WithLocalFiles
from dlt.common.utils import digest128

from dlt.destinations.impl.lance.warnings import uri_on_credentials_deprecated


@configspec
class LanceCredentials(CredentialsConfiguration):
    uri: Optional[str] = None

    embedding_model_provider_api_key: Optional[str] = None
    """API key for the embedding model provider."""

    __config_gen_annotations__: ClassVar[List[str]] = [
        "embedding_model_provider_api_key",
    ]


@configspec
class LanceClientOptions(BaseConfiguration):
    max_retries: Optional[int] = 3
    """`EmbeddingFunction` class wraps the calls for source and query embedding
    generation inside a rate limit handler that retries the requests with exponential
    backoff after successive failures.

    You can tune it by setting it to a different number, or disable it by setting it to 0."""

    __config_gen_annotations__: ClassVar[List[str]] = [
        "max_retries",
    ]


TEmbeddingProvider = Literal[
    "gemini-text",
    "bedrock-text",
    "cohere",
    "gte-text",
    "imagebind",
    "instructor",
    "open-clip",
    "openai",
    "sentence-transformers",
    "huggingface",
    "colbert",
    "ollama",
]


@configspec
class LanceClientConfiguration(WithLocalFiles, DestinationClientDwhConfiguration):
    destination_type: Final[str] = dataclasses.field(  # type: ignore
        default="lance", init=False, repr=False, compare=False
    )
    lance_uri: Optional[str] = None
    """Directory containing .lance datasets. Defaults to local `.lancedb` directory."""
    credentials: LanceCredentials = None

    options: Optional[LanceClientOptions] = None
    """Lance client options."""

    embedding_model_provider: TEmbeddingProvider = "cohere"
    """Embedding provider used for generating embeddings. Default is "cohere". You can find the full list of
    providers at https://github.com/lancedb/lancedb/tree/main/python/python/lancedb/embeddings as well as
    https://lancedb.github.io/lancedb/embeddings/default_embedding_functions/."""
    embedding_model_provider_host: Optional[str] = None
    """Full host URL with protocol and port (e.g. 'http://localhost:11434'). Uses LanceDB's default if not specified, assuming the provider accepts this parameter."""
    embedding_model: str = "embed-english-v3.0"
    """The model used by the embedding provider for generating embeddings.
    Check with the embedding provider which options are available.
    Reference https://lancedb.github.io/lancedb/embeddings/default_embedding_functions/."""
    embedding_model_dimensions: Optional[int] = None
    """The dimensions of the embeddings generated. In most cases it will be automatically inferred, by LanceDB,
    but it is configurable in rare cases.

    Make sure it corresponds with the associated embedding model's dimensionality."""
    vector_field_name: str = "vector"
    """Name of the special field to store the vector embeddings."""
    __config_gen_annotations__: ClassVar[List[str]] = [
        "lance_uri",
        "embedding_model",
        "embedding_model_provider",
    ]

    def on_resolved(self) -> None:
        if self.credentials.uri and not self.lance_uri:
            uri_on_credentials_deprecated()
            self.lance_uri = self.credentials.uri

        # use local path if uri not provided or relative path
        if (
            not self.lance_uri
            or FilesystemConfiguration.is_local_path(self.lance_uri)
            and not os.path.isabs(self.lance_uri)
        ):
            self.lance_uri = self.make_location(self.lance_uri, "%s.lancedb")
        # TODO: move uri back to credentials to make it more like other connections
        self.credentials.uri = self.lance_uri

    def fingerprint(self) -> str:
        """Returns a fingerprint of a connection string."""

        if self.lance_uri:
            return digest128(self.lance_uri)
        return ""
