from typing import Optional, Final

from dlt.common.configuration import configspec
from dlt.common.configuration.specs.base_configuration import (
    BaseConfiguration,
    CredentialsConfiguration,
)
from dlt.common.destination.reference import DestinationClientDwhConfiguration


@configspec
class QdrantCredentials(CredentialsConfiguration):
    # If `:memory:` - use in-memory Qdrant instance.
    # If `str` - use it as a `url` parameter.
    # If `None` - use default values for `host` and `port`
    location: Optional[str] = None
    # API key for authentication in Qdrant Cloud. Default: `None`
    api_key: Optional[str]

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
    path: Optional[str] = None


@configspec
class QdrantClientConfiguration(DestinationClientDwhConfiguration):
    destination_type: Final[str] = "qdrant"  # type: ignore
    # character for the dataset separator
    dataset_separator: str = "_"

    # make it optional so empty dataset is allowed
    dataset_name: Final[Optional[str]] = None  # type: ignore[misc]

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
    options: QdrantClientOptions

    # Qdrant connection credentials
    credentials: QdrantCredentials

    # FlagEmbedding model to use
    # Find the list here. https://qdrant.github.io/fastembed/examples/Supported_Models/.
    model: str = "BAAI/bge-small-en"

    def fingerprint(self) -> str:
        """Returns a fingerprint of a connection string"""

        return self.credentials.location
