import os
import dataclasses
from typing import Optional, Final, Any
from typing_extensions import Annotated, TYPE_CHECKING

from dlt.common.configuration import configspec, NotResolved
from dlt.common.configuration.specs.base_configuration import (
    BaseConfiguration,
    CredentialsConfiguration,
)
from dlt.common.destination.client import DestinationClientDwhConfiguration
from dlt.common.utils import digest128
from dlt.common.storages.configuration import WithLocalFiles

from dlt.destinations.impl.qdrant.exceptions import InvalidInMemoryQdrantCredentials
from dlt.destinations.impl.qdrant.warnings import location_on_credentials_deprecated

if TYPE_CHECKING:
    from qdrant_client import QdrantClient


@configspec
class QdrantCredentials(CredentialsConfiguration):
    if TYPE_CHECKING:
        _external_client: "QdrantClient"
    location: Optional[str] = None
    api_key: Optional[str] = None
    """# API key for authentication in Qdrant Cloud. Default: `None`"""
    # Persistence path for QdrantLocal. Default: `None`
    path: Optional[str] = None

    def on_resolved(self) -> None:
        if self.location == ":memory:":
            raise InvalidInMemoryQdrantCredentials()

    def parse_native_representation(self, native_value: Any) -> None:
        try:
            from qdrant_client import QdrantClient

            if isinstance(native_value, QdrantClient):
                self._external_client = native_value
                self.resolve()
        except ModuleNotFoundError:
            pass

        super().parse_native_representation(native_value)


@configspec
class QdrantClientOptions(BaseConfiguration):
    # Port of the REST API interface. Default: 6333
    port: int = 6333
    # Port of the gRPC interface. Default: 6334
    grpc_port: int = 6334
    # If `true` - use gPRC interface whenever possible in custom methods
    prefer_grpc: bool = False
    # If `true` - use HTTPS(SSL) protocol. Default: `None`
    https: bool = False
    # If not `None` - add `prefix` to the REST URL path.
    # Example: `service/v1` will result in `http://localhost:6333/service/v1/{qdrant-endpoint}` for REST API.
    # Default: `None`
    prefix: Optional[str] = None
    # Timeout for REST and gRPC API requests.
    # Default: 5.0 seconds for REST and unlimited for gRPC
    timeout: Optional[int] = None
    # Host name of Qdrant service. If url and host are None, set to 'localhost'.
    # Default: `None`
    host: Optional[str] = None


@configspec
class QdrantClientConfiguration(WithLocalFiles, DestinationClientDwhConfiguration):
    destination_type: Final[str] = dataclasses.field(default="qdrant", init=False, repr=False, compare=False)  # type: ignore
    credentials: QdrantCredentials = None
    "Qdrant connection credentials"
    qd_location: Optional[str] = None
    """
    If `str` - use it as a `url` parameter.
    If `None` - use default values for `host` and `port`
    """
    qd_path: Optional[str] = None
    """Persistence path for QdrantLocal. Default: `None`"""
    # character for the dataset separator
    dataset_separator: str = "_"

    # make it optional so empty dataset is allowed
    dataset_name: Annotated[Optional[str], NotResolved()] = dataclasses.field(
        default=None, init=False, repr=False, compare=False
    )

    # Batch size for generating embeddings
    embedding_batch_size: int = 32
    # Number of parallel processes for generating embeddings
    embedding_parallelism: int = 0

    # Batch size for uploading embeddings
    upload_batch_size: int = 64
    # Number of parallel processes for uploading embeddings
    upload_parallelism: int = 1
    # Number of retries for uploading embeddings
    upload_max_retries: int = 3

    # Qdrant client options
    options: QdrantClientOptions = None

    # FlagEmbedding model to use
    # Find the list here. https://qdrant.github.io/fastembed/examples/Supported_Models/.
    model: str = "BAAI/bge-small-en"

    def is_local(self) -> bool:
        return self.qd_path is not None

    def _create_client(self, model: str, **options: Any) -> "QdrantClient":
        from qdrant_client import QdrantClient

        client = QdrantClient(
            location=self.qd_location,
            path=self.qd_path,
            api_key=self.credentials.api_key,
            **options,
        )
        client.set_model(model)
        return client

    def get_client(self) -> "QdrantClient":
        client = getattr(self.credentials, "_external_client", None)
        return client or self._create_client(self.model, **dict(self.options))

    def close_client(self, client: "QdrantClient") -> None:
        """Close client if not external"""
        if getattr(self.credentials, "_external_client", None) is client:
            # Do not close client created externally
            return
        client.close()

    def on_resolved(self) -> None:
        if self.credentials.location and not self.qd_location:
            location_on_credentials_deprecated("location")
            self.qd_location = self.credentials.location

        if self.credentials.path and not self.qd_path:
            location_on_credentials_deprecated("path")
            self.qd_path = self.credentials.path

        # using path is a fallback for qdrant, activate only if path specified
        if self.qd_path and not os.path.isabs(self.qd_path):
            self.qd_path = self.make_location(self.qd_path, "%s.qdrant")

    def fingerprint(self) -> str:
        """Returns a fingerprint of a connection string"""
        if self.qd_location:
            return digest128(self.qd_location)
        return ""

    def __str__(self) -> str:
        """Return displayable destination location"""
        if self.qd_path:
            return self.qd_path
        return self.qd_location or "localhost"
