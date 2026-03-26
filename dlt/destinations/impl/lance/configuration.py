import dataclasses
from typing import Dict, Optional, Final, Literal, ClassVar, List, Type

from lance.namespace import DirectoryNamespace

from dlt.common.configuration import configspec
from dlt.common.configuration.specs.base_configuration import (
    BaseConfiguration,
    CredentialsConfiguration,
    resolve_type,
)
from dlt.common.configuration.specs.mixins import WithObjectStoreRsCredentials
from dlt.common.destination.client import DestinationClientDwhConfiguration
from dlt.common.storages.configuration import (
    FileSystemCredentials,
    FilesystemConfigurationWithLocalFiles,
)

DEFAULT_LANCE_BUCKET_URL = "."  # active run dir
DEFAULT_LANCE_NAMESPACE_NAME = "dlt_lance_namespace"


@configspec
class LanceCredentials(CredentialsConfiguration):
    embedding_model_provider_api_key: Optional[str] = None
    """API key for the embedding model provider."""

    __config_gen_annotations__: ClassVar[List[str]] = [
        "embedding_model_provider_api_key",
    ]


@configspec(init=False)
class LanceStorageConfiguration(FilesystemConfigurationWithLocalFiles):
    namespace_name: Optional[str] = DEFAULT_LANCE_NAMESPACE_NAME
    """Name of subdirectory in `bucket_url` to use as namespace root. Leave empty to use `bucket_url` as namespace root."""
    options: Optional[Dict[str, str]] = None
    """Options to pass to storage client. Used as `storage.*` properties on `DirectoryNamespace` client.

    Will be merged with `credentials`-derived options (if present), with `options` taking precedence in case of conflicts.
    """

    __config_gen_annotations__: ClassVar[List[str]] = ["bucket_url", "namespace_name"]

    def __init__(
        self,
        bucket_url: str = DEFAULT_LANCE_BUCKET_URL,
        namespace_name: Optional[str] = DEFAULT_LANCE_NAMESPACE_NAME,
        credentials: Optional[FileSystemCredentials] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__(bucket_url=bucket_url, credentials=credentials)
        self.namespace_name = namespace_name
        self.options = options

    @property
    def namespace_url(self) -> str:
        namespace_url = self.bucket_url.rstrip("/")
        if self.namespace_name:
            namespace_url += "/" + self.namespace_name
        return namespace_url

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        return super().resolve_credentials_type()

    def on_resolved(self) -> None:
        super().on_resolved()
        credentials = (
            self.credentials.to_object_store_rs_credentials()
            if isinstance(self.credentials, WithObjectStoreRsCredentials)
            else {}
        )
        if credentials or self.options:
            self.options = credentials | (self.options or {})

    def make_directory_namespace(self) -> DirectoryNamespace:
        storage_props = {f"storage.{k}": v for k, v in (self.options or {}).items()}
        return DirectoryNamespace(root=self.namespace_url, **storage_props)


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
class LanceClientConfiguration(DestinationClientDwhConfiguration):
    destination_type: Final[str] = dataclasses.field(  # type: ignore
        default="lance", init=False, repr=False, compare=False
    )
    storage: LanceStorageConfiguration = None
    """Storage configuration including URI and cloud credentials."""
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
        "embedding_model",
        "embedding_model_provider",
    ]

    def fingerprint(self) -> str:
        return self.storage.fingerprint()
