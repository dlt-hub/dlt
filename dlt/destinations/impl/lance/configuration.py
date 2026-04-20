from __future__ import annotations

import dataclasses
import os
from typing import TYPE_CHECKING, Any, Dict, Literal, Optional, Final, ClassVar, List, Type, Union

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
    FilesystemConfiguration,
    FilesystemConfigurationWithLocalFiles,
    WithLocalFiles,
)

if TYPE_CHECKING:
    from lance_namespace import LanceNamespace
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


LanceCatalogType = Literal["dir", "rest"]


def _merge_cloud_options(cfg: FilesystemConfigurationWithLocalFiles) -> None:
    """Merges credentials-derived object-store options into `cfg.options` with cloud timeouts."""
    credentials = (
        cfg.credentials.to_object_store_rs_credentials()
        if isinstance(cfg.credentials, WithObjectStoreRsCredentials)
        else {}
    )
    if not cfg.is_local_filesystem:
        defaults = {"connect_timeout": "30s", "timeout": "120s"}
        cfg.options = defaults | (cfg.options or {})  # type: ignore[attr-defined]
    if credentials or cfg.options:  # type: ignore[attr-defined]
        cfg.options = credentials | (cfg.options or {})  # type: ignore[attr-defined]


@configspec(init=False)
class LanceStorageConfiguration(FilesystemConfigurationWithLocalFiles):
    namespace_name: Optional[str] = DEFAULT_LANCE_NAMESPACE_NAME
    """Name of subdirectory in `bucket_url` to use as namespace root. Leave empty to use `bucket_url` as namespace root."""
    options: Optional[Dict[str, str]] = None
    """Options to pass to storage client. See available options at https://lance.org/guide/object_store/#general-configuration.

    Will be merged with `credentials`-derived options (if present), with `options` taking precedence in case of conflicts.
    """

    __config_gen_annotations__: ClassVar[List[str]] = ["bucket_url"]

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
        _merge_cloud_options(self)


@configspec(init=False)
class DirectoryCatalogCredentials(FilesystemConfigurationWithLocalFiles):
    """Optional filesystem location for Lance directory catalog (`__manifest` root).

    When `bucket_url` is empty, the catalog colocates with data (`storage`)
    Setting `bucket_url` explicitly enables
    Lance multi-base layout: `__manifest` in one bucket, data in another.
    """

    options: Optional[Dict[str, str]] = None
    """Object-store options for the catalog filesystem. Merged with credentials-derived options."""

    __config_gen_annotations__: ClassVar[List[str]] = []

    def __init__(
        self,
        bucket_url: Optional[str] = None,
        credentials: Optional[FileSystemCredentials] = None,
        options: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__(bucket_url=bucket_url, credentials=credentials)
        self.options = options

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        return super().resolve_credentials_type()

    def verify_bucket_url(self) -> None:
        # accept empty bucket_url — inheritance from storage happens at parent level
        if self.bucket_url:
            super().verify_bucket_url()

    def on_resolved(self) -> None:
        if self.bucket_url:
            _merge_cloud_options(self)

    def on_partial(self) -> None:
        # allow unresolved state when bucket_url is not set; parent fills from storage
        if not self.bucket_url:
            self.resolve()
        else:
            super().on_partial()


@configspec
class RestCatalogCredentials(CredentialsConfiguration):
    """Credentials for connecting to a Lance REST Namespace server."""

    uri: str = None
    """Base URI of the Lance REST Namespace server, e.g. `http://127.0.0.1:2333`."""


LanceCredentials = Union[DirectoryCatalogCredentials, RestCatalogCredentials]
"""Polymorphic credentials resolved per `catalog_type`."""


@configspec
class LanceCatalogCapabilities(BaseConfiguration):
    max_identifier_length: int = 200
    max_column_identifier_length: int = 1024
    supports_nested_namespaces: bool = True


@configspec
class DirectoryCatalogCapabilities(LanceCatalogCapabilities):
    manifest_enabled: bool = True
    """V2 catalog tracking via a shared `__manifest` Lance table.

    Enables fast listing and nested namespaces. Disable to fall back to directory scanning
    if concurrent writers on the same root cause manifest contention.
    """
    dir_listing_enabled: bool = True
    """V1 fallback: discover tables by scanning directories for `.lance` suffixes."""


@configspec
class RestCatalogCapabilities(LanceCatalogCapabilities):
    pass


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
    catalog_type: LanceCatalogType = "dir"

    CATALOG_CREDENTIALS: ClassVar[Dict[LanceCatalogType, Any]] = {
        "dir": DirectoryCatalogCredentials,
        "rest": RestCatalogCredentials,
    }
    CATALOG_CAPABILITIES: ClassVar[Dict[LanceCatalogType, Any]] = {
        "dir": DirectoryCatalogCapabilities,
        "rest": RestCatalogCapabilities,
    }
    CATALOG_STORAGE: ClassVar[Dict[LanceCatalogType, Any]] = {
        "dir": LanceStorageConfiguration,
    }

    credentials: LanceCredentials = None  # type: ignore[assignment]
    capabilities: LanceCatalogCapabilities = None
    storage: Optional[LanceStorageConfiguration] = None
    """Storage configuration for table data. Required for `"dir"` catalog, optional for `"rest"`."""
    branch_name: Optional[str] = None
    """Name of branch to use for read/write table operations. Uses `main` branch if not set."""
    embeddings: Optional[LanceEmbeddingsConfiguration] = None
    """Optional embeddings configuration to add a vector embedding column."""

    @property
    def storage_options(self) -> Optional[Dict[str, str]]:
        return self.storage.options if self.storage else None

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        return self.CATALOG_CREDENTIALS.get(self.catalog_type) or CredentialsConfiguration

    @resolve_type("capabilities")
    def resolve_capabilities_type(self) -> Type[LanceCatalogCapabilities]:
        return self.CATALOG_CAPABILITIES.get(self.catalog_type) or LanceCatalogCapabilities

    @resolve_type("storage")
    def resolve_storage_type(self) -> Type[FilesystemConfiguration]:
        # mypy can't narrow the ClassVar lookup; fall through is acceptable for now
        return self.CATALOG_STORAGE.get(self.catalog_type) or Optional[FilesystemConfiguration]  # type: ignore[return-value]

    def on_resolved(self) -> None:
        # propagate pipeline context (local_dir, pipeline_name, etc.) to nested storage
        if isinstance(self.storage, WithLocalFiles):
            self.storage.attach_from(self)
            if not self.storage.is_resolved():
                self.storage.resolve()
            else:
                self.storage.normalize_bucket_url()

        # propagate pipeline context to catalog credentials too
        if isinstance(self.credentials, WithLocalFiles):
            self.credentials.attach_from(self)

        # DirectoryCatalogCredentials falls back to storage's location/auth when unset
        if isinstance(self.credentials, DirectoryCatalogCredentials) and self.storage is not None:
            if not self.credentials.bucket_url:
                self.credentials.bucket_url = self.storage.namespace_uri
                if self.credentials.credentials is None:
                    self.credentials.credentials = self.storage.credentials
                if self.credentials.options is None:
                    # storage.options is already merged (cloud defaults + creds)
                    self.credentials.options = self.storage.options
            elif isinstance(self.credentials, FilesystemConfigurationWithLocalFiles):
                # explicit catalog location may be relative — re-run normalization under local_dir
                self.credentials.normalize_bucket_url()

    def make_namespace(self) -> "LanceNamespace":
        from lance_namespace import connect

        props: Dict[str, str] = {}
        if isinstance(self.credentials, DirectoryCatalogCredentials):
            props["root"] = self.credentials.bucket_url.rstrip("/")
            assert isinstance(self.capabilities, DirectoryCatalogCapabilities)
            props["manifest_enabled"] = str(self.capabilities.manifest_enabled).lower()
            props["dir_listing_enabled"] = str(self.capabilities.dir_listing_enabled).lower()
            for k, v in (self.credentials.options or {}).items():
                if v is not None:
                    props[f"storage.{k}"] = str(v)
        elif isinstance(self.credentials, RestCatalogCredentials):
            props["uri"] = self.credentials.uri
        return connect(self.catalog_type, props)

    def fingerprint(self) -> str:
        return self.storage.fingerprint() if self.storage else ""
