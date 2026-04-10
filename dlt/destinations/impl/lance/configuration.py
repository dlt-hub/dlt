from __future__ import annotations

import dataclasses
import os
from typing import TYPE_CHECKING, Any, Dict, Literal, Optional, Final, ClassVar, List, Type

from dlt.common import logger
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
    WithLocalFiles,
)

if TYPE_CHECKING:
    from lance.namespace import DirectoryNamespace
    from lancedb.embeddings import EmbeddingFunction, EmbeddingFunctionRegistry

DEFAULT_LANCE_BUCKET_URL = "."  # active run dir
DEFAULT_LANCE_NAMESPACE_NAME = "dlt_lance_root"
DEFAULT_LANCE_VECTOR_COLUMN_NAME = "vector"

# NOTE: list providers with `EmbeddingFunctionRegistry.get_instance()._functions.keys()`
TEmbeddingProvider = Literal[
    "bedrock-text",
    "cohere",
    "colbert",
    "colpali",
    "gemini-text",
    "gte-text",
    "huggingface",
    "imagebind",
    "instructor",
    "jina",
    "ollama",
    "open-clip",
    "openai",
    "sentence-transformers",
    "siglip",
    "voyageai",
    "watsonx",
]


@configspec(init=False)
class LanceStorageConfiguration(FilesystemConfigurationWithLocalFiles):
    namespace_name: Optional[str] = DEFAULT_LANCE_NAMESPACE_NAME
    """Name of subdirectory in `bucket_url` to use as namespace root. Leave empty to use `bucket_url` as namespace root."""
    branch_name: Optional[str] = None
    """Name of branch to use for read/write table operations. Uses `main` branch if not set."""
    options: Optional[Dict[str, str]] = None
    """Options to pass to storage client. See available options at https://lance.org/guide/object_store/#general-configuration.

    Will be merged with `credentials`-derived options (if present), with `options` taking precedence in case of conflicts.
    """

    __config_gen_annotations__: ClassVar[List[str]] = ["bucket_url"]

    def __init__(
        self,
        bucket_url: str = DEFAULT_LANCE_BUCKET_URL,
        namespace_name: Optional[str] = DEFAULT_LANCE_NAMESPACE_NAME,
        branch_name: Optional[str] = None,
        credentials: Optional[FileSystemCredentials] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__(bucket_url=bucket_url, credentials=credentials)
        self.namespace_name = namespace_name
        self.branch_name = branch_name
        self.options = options

    @property
    def namespace_uri(self) -> str:
        namespace_uri = self.bucket_url.rstrip("/")
        if self.namespace_name:
            namespace_uri += "/" + self.namespace_name
        return namespace_uri

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
        from lance.namespace import DirectoryNamespace

        storage_props = {f"storage.{k}": v for k, v in (self.options or {}).items()}
        return DirectoryNamespace(root=self.namespace_uri, **storage_props)


@configspec
class LanceEmbeddingsCredentials(CredentialsConfiguration):
    api_key: str = None
    """API key for embedding model provider."""

    __config_gen_annotations__: ClassVar[List[str]] = ["api_key"]


@configspec
class LanceEmbeddingsConfiguration(BaseConfiguration):
    credentials: Optional[LanceEmbeddingsCredentials] = None
    """Credentials for embedding model provider. Leave empty if authentication is not required (e.g. local providers)."""

    vector_column: str = DEFAULT_LANCE_VECTOR_COLUMN_NAME
    """Name of column to store vector embeddings in."""
    provider: TEmbeddingProvider = None
    """Provider of model used to generate embeddings, e.g. `cohere` or `openai`.

    Find all providers at https://github.com/lancedb/lancedb/tree/main/python/python/lancedb/embeddings.
    """
    name: str = None
    """Name of model used by provider to generate embeddings, e.g. `embed-english-v3.0`."""
    max_retries: Optional[int] = 3
    """Number of retries for embedding requests. Set to 0 to disable retries."""
    kwargs: Optional[Dict[str, Any]] = None
    """Additional provider-specific keyword arguments passed to `EmbeddingFunction.create()`."""

    __config_gen_annotations__: ClassVar[List[str]] = [
        "provider",
        "name",
    ]

    # NOTE: env var names derived from `lancedb==0.26.1` source code.
    # Providers with `api_key` field (openai, jina, watsonx) also accept
    # the key via kwargs, but we set the env var regardless for consistency.
    _PROVIDER_ENV_VAR_NAMES: ClassVar[Dict[TEmbeddingProvider, str]] = {
        "cohere": "COHERE_API_KEY",
        "gemini-text": "GOOGLE_API_KEY",
        "jina": "JINA_API_KEY",
        "openai": "OPENAI_API_KEY",
        "voyageai": "VOYAGE_API_KEY",
        "watsonx": "WATSONX_API_KEY",
    }

    def create_embedding_function(self) -> EmbeddingFunction:
        from lancedb.embeddings import EmbeddingFunctionRegistry

        if self.credentials:
            self._set_provider_api_key_env_var(self.credentials.api_key)
        kwargs = {"name": self.name, "max_retries": self.max_retries} | (self.kwargs or {})
        return EmbeddingFunctionRegistry.get_instance().get(self.provider).create(**kwargs)

    def _set_provider_api_key_env_var(self, api_key: str) -> None:
        if env_var := self._PROVIDER_ENV_VAR_NAMES.get(self.provider):
            os.environ[env_var] = api_key
        else:
            logger.warning(
                "Provider %r is not in _PROVIDER_ENV_VAR_NAMES. The provided API key"
                " will be ignored.",
                self.provider,
            )


@configspec
class LanceClientConfiguration(WithLocalFiles, DestinationClientDwhConfiguration):
    destination_type: Final[str] = dataclasses.field(  # type: ignore
        default="lance", init=False, repr=False, compare=False
    )
    storage: LanceStorageConfiguration = None
    """Storage configuration including URI and cloud credentials."""
    embeddings: Optional[LanceEmbeddingsConfiguration] = None
    """Optional embeddings configuration to add a vector embedding column. Leave empty to skip embedding generation."""

    def on_resolved(self) -> None:
        # propagate pipeline context (local_dir, pipeline_name, etc.) to nested storage
        if isinstance(self.storage, WithLocalFiles):
            self.storage.attach_from(self)
            if not self.storage.is_resolved():
                self.storage.resolve()
            else:
                self.storage.normalize_bucket_url()

    def fingerprint(self) -> str:
        return self.storage.fingerprint()
