import dataclasses
from typing import Optional, Final, Literal, ClassVar, List

from dlt.common.configuration import configspec
from dlt.common.configuration.specs.base_configuration import (BaseConfiguration, CredentialsConfiguration, )
from dlt.common.destination.reference import DestinationClientDwhConfiguration
from dlt.common.typing import DictStrStr, TSecretStrValue
from dlt.common.utils import digest128


@configspec
class LanceDBCredentials(CredentialsConfiguration):
    uri: str = "./.lancedb"
    """LanceDB database URI. Defaults to local, on-disk instance.

    The available schemas are:

    - `/path/to/database` - local database.
    - `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud storage.
    - `db://host:port` - remote database (LanceDB cloud).
    """
    api_key: TSecretStrValue = None
    """API key for the remote connections (LanceDB cloud)."""

    __config_gen_annotations__: ClassVar[List[str]] = ["uri", "api_key", ]


def _default_storage_options() -> DictStrStr:
    return {"timeout": "60s", "aws_access_key_id": "my-access-key", "aws_secret_access_key": "my-secret-key",
            "aws_session_token": "my-session-token", }


@configspec
class LanceDBClientOptions(BaseConfiguration):
    storage_options: Optional[DictStrStr] = dataclasses.field(default_factory=_default_storage_options)
    """Dictionary containing configuration options for connecting to different storage backends.

    The `storage_options` dictionary allows you to specify various settings required for connecting to storage
    services such as AWS S3,
    Google Cloud Storage, and Azure Blob Storage. These settings can include parameters like `region`, `endpoint`,
    and other service-specific configurations.

    Here is an example of how to include storage options the configuration or secrets file:

    ```toml
    [destination.lancedb]
    storage_options = { region = "us-east-1", endpoint = "http://minio:9000" }
    ```

    You can find more information about the available options and their usage in the LanceDB storage configuration
    guide:
    https://lancedb.github.io/lancedb/guides/storage/
    """
    max_retries: Optional[int] = 1
    """`EmbeddingFunction` class wraps the calls for source and query embedding
    generation inside a rate limit handler that retries the requests with exponential
    backoff after successive failures.

    You can tune it by setting it to a different number, or disable it by setting it to 0."""

    __config_gen_annotations__: ClassVar[List[str]] = ["storage_options", "max_retries", ]


TEmbeddingProvider = Literal[
    "gemini-text", "bedrock-text", "cohere", "gte-text", "imagebind", "instructor", "open-clip", "openai",
    "sentence-transformers", "huggingface", "colbert",]


@configspec
class LanceDBClientConfiguration(DestinationClientDwhConfiguration):
    destination_type: Final[str] = dataclasses.field(  # type: ignore
        default="LanceDB", init=False, repr=False, compare=False)
    credentials: LanceDBCredentials = None
    dataset_separator: str = "_"
    """Character for the dataset separator."""
    dataset_name: Final[Optional[str]] = dataclasses.field(  # type: ignore
        default=None, init=False, repr=False, compare=False)

    options: Optional[LanceDBClientOptions] = None
    """LanceDB client options."""

    provider: TEmbeddingProvider = "cohere"
    """Embedding provider used for generating embeddings. Default is "cohere". You can find the full list of
    providers at https://github.com/lancedb/lancedb/tree/main/python/python/lancedb/embeddings as well as
    https://lancedb.github.io/lancedb/embeddings/default_embedding_functions/."""
    embedding_model: str = "embed-english-v3.0"
    """The model used by the embedding provider for generating embeddings.
    Check with the embedding provider which options are available.
    Reference https://lancedb.github.io/lancedb/embeddings/default_embedding_functions/."""
    embedding_model_dimensions: int = 1024
    """The dimensions of the embeddings generated. Make sure it corresponds with the embedding model's."""

    __config_gen_annotations__: ClassVar[List[str]] = ["embedding_model_dimensions", "embedding_model", "provider"]


    def fingerprint(self) -> str:
        """Returns a fingerprint of a connection string."""

        if self.credentials and self.credentials.uri:
            return digest128(self.credentials.uri)
        return ""
