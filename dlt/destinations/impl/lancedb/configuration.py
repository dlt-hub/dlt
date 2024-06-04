import dataclasses
from typing import Optional, Final, Literal, ClassVar, List

from dlt.common.configuration import configspec
from dlt.common.configuration.specs.base_configuration import (
    BaseConfiguration,
    CredentialsConfiguration,
)
from dlt.common.destination.reference import DestinationClientDwhConfiguration
from dlt.common.typing import TSecretStrValue
from dlt.common.utils import digest128


@configspec
class LanceDBCredentials(CredentialsConfiguration):
    uri: str = "./.lancedb"
    """LanceDB database URI. Defaults to local, on-disk instance.

    The available schemas are:

    - `/path/to/database` - local database.
    - `db://host:port` - remote database (LanceDB cloud).
    """
    api_key: TSecretStrValue = None
    """API key for the remote connections (LanceDB cloud)."""
    embedding_model_provider_api_key: str = None
    """API key for the embedding model provider."""

    __config_gen_annotations__: ClassVar[List[str]] = [
        "uri",
        "api_key",
        "embedding_model_provider_api_key"
    ]


@configspec
class LanceDBClientOptions(BaseConfiguration):
    max_retries: Optional[int] = 1
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
]


@configspec
class LanceDBClientConfiguration(DestinationClientDwhConfiguration):
    destination_type: Final[str] = dataclasses.field(  # type: ignore
        default="LanceDB", init=False, repr=False, compare=False
    )
    credentials: LanceDBCredentials = None
    dataset_separator: str = "_"
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
    embedding_model: str = "embed-english-v3.0"
    """The model used by the embedding provider for generating embeddings.
    Check with the embedding provider which options are available.
    Reference https://lancedb.github.io/lancedb/embeddings/default_embedding_functions/."""
    embedding_model_dimensions: int = 1024
    """The dimensions of the embeddings generated. Make sure it corresponds with the embedding model's."""

    __config_gen_annotations__: ClassVar[List[str]] = [
        "embedding_model_dimensions",
        "embedding_model",
        "embedding_model_provider",
    ]

    def fingerprint(self) -> str:
        """Returns a fingerprint of a connection string."""

        if self.credentials and self.credentials.uri:
            return digest128(self.credentials.uri)
        return ""
