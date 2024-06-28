import dataclasses
from typing import Optional, Final, Any
from typing_extensions import Annotated, TYPE_CHECKING, Iterator
import threading
from contextlib import contextmanager

from dlt.common.configuration import configspec, NotResolved
from dlt.common.configuration.specs.base_configuration import (
    BaseConfiguration,
    CredentialsConfiguration,
)
from dlt.common.destination.reference import DestinationClientDwhConfiguration

if TYPE_CHECKING:
    from qdrant_client import QdrantClient


@configspec
class QdrantCredentials(CredentialsConfiguration):
    # If `:memory:` - use in-memory Qdrant instance.
    # If `str` - use it as a `url` parameter.
    # If `None` - use default values for `host` and `port`
    location: Optional[str] = None
    # API key for authentication in Qdrant Cloud. Default: `None`
    api_key: Optional[str] = None
    # Persistence path for QdrantLocal. Default: `None`
    path: Optional[str] = None

    # __client: Optional[Any] = None
    # if TYPE_CHECKING:
    #     __client: Optional["QdrantClient"] = None
    # __external_client: bool = False

    def parse_native_representation(self, native_value: Any) -> None:
        try:
            from qdrant_client import QdrantClient

            if isinstance(native_value, QdrantClient):
                self._client = native_value
                self._external_client = True
                self.resolve()
        except ModuleNotFoundError:
            pass

        super().parse_native_representation(native_value)

    def get_client(self, model: str, **options: Any) -> "QdrantClient":
        if not hasattr(self, "_client_lock"):
            self._client_lock = threading.Lock()

        with self._client_lock:
            client = getattr(self, "_client", None)
            borrow_count = getattr(self, "_client_borrow_count", 0)
            self._client_borrow_count = borrow_count + 1
            if client is not None:
                return client  # type: ignore[no-any-return]

            from qdrant_client import QdrantClient

            client = self._client = QdrantClient(**dict(self), **options)
            client.set_model(model)
            return client  # type: ignore[no-any-return]

    def close_client(self) -> None:
        """Close client if not external"""
        if getattr(self, "_external_client", False):
            return
        with self._client_lock:
            borrow_count = getattr(self, "_client_borrow_count", 1)
            client = getattr(self, "_client", None)
            borrow_count = self._client_borrow_count = borrow_count - 1
            if borrow_count == 0 and client is not None:
                client.close()
                self._client = None

    def __str__(self) -> str:
        return self.location or "localhost"


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
    # Persistence path for QdrantLocal. Default: `None`
    # path: Optional[str] = None


@configspec
class QdrantClientConfiguration(DestinationClientDwhConfiguration):
    destination_type: Final[str] = dataclasses.field(default="qdrant", init=False, repr=False, compare=False)  # type: ignore
    # Qdrant connection credentials
    credentials: QdrantCredentials = None
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

    def get_client(self) -> "QdrantClient":
        return self.credentials.get_client(self.model, **dict(self.options))

    def close_client(self) -> None:
        self.credentials.close_client()

    def fingerprint(self) -> str:
        """Returns a fingerprint of a connection string"""

        return self.credentials.location
