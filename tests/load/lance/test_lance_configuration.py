import pytest

from dlt.common.configuration import configspec
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration
from dlt.common.configuration.specs.mixins import WithObjectStoreRsCredentials

from dlt.destinations.impl.lance.configuration import (
    DEFAULT_LANCE_BUCKET_URL,
    DEFAULT_LANCE_NAMESPACE_NAME,
    LanceEmbeddingsConfiguration,
    LanceEmbeddingsCredentials,
    LanceStorageConfiguration,
)


pytestmark = pytest.mark.essential


def test_lance_storage_configuration_namespace_url() -> None:
    from dlt.common.runtime.run_context import active  # from auto_test_run_context fixture

    # falls back to defaults when no values are provided
    config = LanceStorageConfiguration()
    assert config.namespace_url == f"{DEFAULT_LANCE_BUCKET_URL}/{DEFAULT_LANCE_NAMESPACE_NAME}"
    config = LanceStorageConfiguration(bucket_url="s3://my-bucket")
    assert config.namespace_url == f"s3://my-bucket/{DEFAULT_LANCE_NAMESPACE_NAME}"
    config = LanceStorageConfiguration(namespace_name="my-namespace")
    assert config.namespace_url == f"{DEFAULT_LANCE_BUCKET_URL}/my-namespace"

    # concatenates bucket_url and namespace_name to form namespace_url
    config = LanceStorageConfiguration(bucket_url="s3://my-bucket", namespace_name="my-namespace")
    assert config.namespace_url == "s3://my-bucket/my-namespace"

    # handles trailing slash
    config = LanceStorageConfiguration(bucket_url="s3://my-bucket/", namespace_name="my-namespace")
    assert config.namespace_url == "s3://my-bucket/my-namespace"

    # allows empty namespace_name (i.e. namespace_url is just bucket_url)
    config = LanceStorageConfiguration(bucket_url="s3://my-bucket/", namespace_name=None)
    assert config.namespace_url == "s3://my-bucket"

    # resolution should turn relative local path into absolute path
    local_dir_uri = f"file://{active().local_dir}"

    config = LanceStorageConfiguration(bucket_url=".", namespace_name="bar")
    config.call_method_in_mro("on_partial")  # to resolve config.local_dir
    assert config.namespace_url == f"{local_dir_uri}/bar"

    config = LanceStorageConfiguration(bucket_url="foo", namespace_name="bar")
    config.call_method_in_mro("on_partial")  # to resolve config.local_dir
    assert config.namespace_url == f"{local_dir_uri}/foo/bar"


def test_lance_storage_configuration_options() -> None:
    CREDS_PROVIDED_OPTS = {"creds_opt": "foo", "another_creds_opt": "bar"}
    USER_PROVIDED_OPTS = {"user_opt": "foo", "another_user_opt": "bar"}

    @configspec
    class DummyObjectStoreRsCredentials(CredentialsConfiguration, WithObjectStoreRsCredentials):
        def to_object_store_rs_credentials(self):
            return CREDS_PROVIDED_OPTS

    @configspec
    class DummyNonObjectStoreRsCredentials(CredentialsConfiguration):
        token: str = "some-token"

    # user-provided options are set
    config = LanceStorageConfiguration(
        bucket_url="s3://my-bucket",
        options=USER_PROVIDED_OPTS,
    )
    config.resolve()
    assert config.options == USER_PROVIDED_OPTS

    # credential-derived options are set when credentials implement WithObjectStoreRsCredentials
    config = LanceStorageConfiguration(
        bucket_url="s3://my-bucket",
        credentials=DummyObjectStoreRsCredentials(),  # type: ignore[arg-type]
    )
    config.resolve()
    assert config.options == CREDS_PROVIDED_OPTS

    # no credential-derived options are set when credentials do not implement WithObjectStoreRsCredentials
    config = LanceStorageConfiguration(
        bucket_url="s3://my-bucket",
        credentials=DummyNonObjectStoreRsCredentials(),  # type: ignore[arg-type]
    )
    config.resolve()
    assert config.options is None

    # user-provided options override credential-derived options when keys overlap
    config = LanceStorageConfiguration(
        bucket_url="s3://my-bucket",
        credentials=DummyObjectStoreRsCredentials(),  # type: ignore[arg-type]
        options={"creds_opt": "user_foo"} | USER_PROVIDED_OPTS,
    )
    config.resolve()
    assert config.options == CREDS_PROVIDED_OPTS | {"creds_opt": "user_foo"} | USER_PROVIDED_OPTS


def test_lance_embeddings_configuration_create_embedding_function() -> None:
    from lancedb.embeddings import OpenAIEmbeddings, CohereEmbeddingFunction, OllamaEmbeddings

    creds = LanceEmbeddingsCredentials(api_key="test-key")

    # selects correct class based on provider, sets name and max_retries
    config = LanceEmbeddingsConfiguration(
        credentials=creds, provider="openai", name="text-embedding-3-small", max_retries=5
    )
    func = config.create_embedding_function()
    assert isinstance(func, OpenAIEmbeddings)
    assert func.name == "text-embedding-3-small"
    assert func.max_retries == 5

    config = LanceEmbeddingsConfiguration(
        credentials=creds, provider="cohere", name="embed-english-v3.0", max_retries=2
    )
    func = config.create_embedding_function()
    assert isinstance(func, CohereEmbeddingFunction)
    assert func.name == "embed-english-v3.0"
    assert func.max_retries == 2

    # works without credentials (local providers don't need API key)
    config = LanceEmbeddingsConfiguration(provider="ollama", name="nomic-embed-text")
    func = config.create_embedding_function()
    assert isinstance(func, OllamaEmbeddings)
    assert func.name == "nomic-embed-text"

    # kwargs are forwarded and set provider-specific attributes
    config = LanceEmbeddingsConfiguration(
        provider="ollama",
        name="nomic-embed-text",
        kwargs={"host": "http://localhost:11434"},
    )
    func = config.create_embedding_function()
    assert isinstance(func, OllamaEmbeddings)
    assert func.name == "nomic-embed-text"
    assert func.host == "http://localhost:11434"

    # kwargs override name and max_retries when conflicting
    config = LanceEmbeddingsConfiguration(
        credentials=creds,
        provider="openai",
        name="text-embedding-3-small",
        max_retries=3,
        kwargs={"name": "text-embedding-3-large", "max_retries": 1},
    )
    func = config.create_embedding_function()
    assert func.name == "text-embedding-3-large"
    assert func.max_retries == 1
