import os
import dataclasses
from typing import Optional, Final, Literal, ClassVar, List

from dlt.common.configuration import configspec
from dlt.common.configuration.specs.base_configuration import (
    BaseConfiguration,
    CredentialsConfiguration,
)
from dlt.common.destination.client import DestinationClientDwhConfiguration
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.typing import TSecretStrValue
from dlt.common.utils import digest128
from dlt.destinations.configuration import WithLocalFiles
from dlt.destinations.impl.lancedb.warnings import uri_on_credentials_deprecated


@configspec
class LanceDBCredentials(CredentialsConfiguration):
    uri: Optional[str] = None  # deprecated!
    api_key: Optional[TSecretStrValue] = None
    """API key for the remote connections (LanceDB cloud)."""
    embedding_model_provider_api_key: Optional[str] = None
    """API key for the embedding model provider."""

    __config_gen_annotations__: ClassVar[List[str]] = [
        "api_key",
        "embedding_model_provider_api_key",
    ]


@configspec
class LanceDBClientOptions(BaseConfiguration):
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
class LanceDBClientConfiguration(WithLocalFiles, DestinationClientDwhConfiguration):
    destination_type: Final[str] = dataclasses.field(  # type: ignore
        default="LanceDB", init=False, repr=False, compare=False
    )
    lance_uri: Optional[str] = None
    """LanceDB database URI. Defaults to local, on-disk instance.

    The available schemas are:

    - `/path/to/database` - local database.
    - `db://host:port` - remote database (LanceDB cloud).
    """
    credentials: LanceDBCredentials = None
    dataset_separator: str = "___"
    """Character for the dataset separator."""
    dataset_name: Final[Optional[str]] = dataclasses.field(  # type: ignore
        default=None, init=False, repr=False, compare=False
    )

    options: Optional[LanceDBClientOptions] = None
    """LanceDB client options."""

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
    sentinel_table_name: str = "dltSentinelTable"
    """Name of the sentinel table that encapsulates datasets. Since LanceDB has no
    concept of schemas, this table serves as a proxy to group related dlt tables together."""

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

    def fingerprint(self) -> str:
        """Returns a fingerprint of a connection string."""

        if self.lance_uri:
            return digest128(self.lance_uri)
        return ""
