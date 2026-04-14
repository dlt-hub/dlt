import os
from typing import Optional

import pytest
from lancedb.embeddings import CohereEmbeddingFunction, OllamaEmbeddings, OpenAIEmbeddings

import dlt
from dlt.common.configuration import configspec, resolve_configuration
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration
from dlt.common.configuration.specs.mixins import WithObjectStoreRsCredentials
from dlt.common.known_env import DLT_LOCAL_DIR
from dlt.common.runtime.run_context import active
from dlt.common.utils import uniq_id

from dlt.destinations.impl.lance.configuration import (
    DEFAULT_LANCE_BUCKET_URL,
    DEFAULT_LANCE_NAMESPACE_NAME,
    DirectoryCatalogCapabilities,
    DirectoryCatalogCredentials,
    LanceClientConfiguration,
    LanceEmbeddingsConfiguration,
    LanceEmbeddingsCredentials,
    LanceStorageConfiguration,
    TEmbeddingProvider,
)
from tests.utils import capture_dlt_logger, get_test_storage_root


pytestmark = pytest.mark.essential


def test_lance_storage_configuration_namespace_uri() -> None:
    # falls back to defaults when no values are provided
    config = LanceStorageConfiguration()
    assert config.namespace_uri == f"{DEFAULT_LANCE_BUCKET_URL}/{DEFAULT_LANCE_NAMESPACE_NAME}"
    config = LanceStorageConfiguration(bucket_url="s3://my-bucket")
    assert config.namespace_uri == f"s3://my-bucket/{DEFAULT_LANCE_NAMESPACE_NAME}"
    config = LanceStorageConfiguration(namespace_name="my-namespace")
    assert config.namespace_uri == f"{DEFAULT_LANCE_BUCKET_URL}/my-namespace"

    # concatenates bucket_url and namespace_name to form namespace_uri
    config = LanceStorageConfiguration(bucket_url="s3://my-bucket", namespace_name="my-namespace")
    assert config.namespace_uri == "s3://my-bucket/my-namespace"

    # handles trailing slash
    config = LanceStorageConfiguration(bucket_url="s3://my-bucket/", namespace_name="my-namespace")
    assert config.namespace_uri == "s3://my-bucket/my-namespace"

    # allows empty namespace_name (i.e. namespace_uri is just bucket_url)
    config = LanceStorageConfiguration(bucket_url="s3://my-bucket/", namespace_name=None)
    assert config.namespace_uri == "s3://my-bucket"

    # resolution should turn relative local path into absolute path
    local_dir_uri = f"file://{active().local_dir}"

    config = LanceStorageConfiguration(bucket_url=".", namespace_name="bar")
    config.call_method_in_mro("on_partial")  # to resolve config.local_dir
    assert config.namespace_uri == f"{local_dir_uri}/bar"

    config = LanceStorageConfiguration(bucket_url="foo", namespace_name="bar")
    config.call_method_in_mro("on_partial")  # to resolve config.local_dir
    assert config.namespace_uri == f"{local_dir_uri}/foo/bar"


def test_lance_storage_configuration_options() -> None:
    CREDS_PROVIDED_OPTS = {"creds_opt": "foo", "another_creds_opt": "bar"}
    USER_PROVIDED_OPTS = {"user_opt": "foo", "another_user_opt": "bar"}
    # cloud storage gets default timeouts to prevent infinite hangs in the Rust object_store
    CLOUD_DEFAULTS = {"connect_timeout": "30s", "timeout": "120s"}

    @configspec
    class DummyObjectStoreRsCredentials(CredentialsConfiguration, WithObjectStoreRsCredentials):
        def to_object_store_rs_credentials(self):
            return CREDS_PROVIDED_OPTS

    @configspec
    class DummyNonObjectStoreRsCredentials(CredentialsConfiguration):
        token: str = "some-token"

    # user-provided options are set (merged on top of cloud defaults)
    config = LanceStorageConfiguration(
        bucket_url="s3://my-bucket",
        options=USER_PROVIDED_OPTS,
    )
    config.resolve()
    assert config.options == CLOUD_DEFAULTS | USER_PROVIDED_OPTS

    # user can override default timeouts
    config = LanceStorageConfiguration(
        bucket_url="s3://my-bucket",
        options={"timeout": "300s"},
    )
    config.resolve()
    assert config.options["timeout"] == "300s"
    assert config.options["connect_timeout"] == "30s"

    # credential-derived options are set when credentials implement WithObjectStoreRsCredentials
    config = LanceStorageConfiguration(
        bucket_url="s3://my-bucket",
        credentials=DummyObjectStoreRsCredentials(),  # type: ignore[arg-type]
    )
    config.resolve()
    assert config.options == CLOUD_DEFAULTS | CREDS_PROVIDED_OPTS

    # no credential-derived options when credentials do not implement WithObjectStoreRsCredentials
    # but cloud defaults are still set
    config = LanceStorageConfiguration(
        bucket_url="s3://my-bucket",
        credentials=DummyNonObjectStoreRsCredentials(),  # type: ignore[arg-type]
    )
    config.resolve()
    assert config.options == CLOUD_DEFAULTS

    # local filesystem gets no default timeouts
    config = LanceStorageConfiguration(
        bucket_url="/tmp/local",
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
    # merge order: credentials | defaults | user_options (user wins over creds on overlap)
    assert (
        config.options
        == CREDS_PROVIDED_OPTS | CLOUD_DEFAULTS | {"creds_opt": "user_foo"} | USER_PROVIDED_OPTS
    )


def test_lance_embeddings_configuration_create_embedding_function() -> None:
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


def test_lance_embeddings_configuration_warns_on_unknown_provider_api_key(
    caplog: pytest.LogCaptureFixture,
) -> None:
    provider: TEmbeddingProvider = "ollama"
    assert provider not in LanceEmbeddingsConfiguration._PROVIDER_ENV_VAR_NAMES

    creds = LanceEmbeddingsCredentials(api_key="test-key")
    config = LanceEmbeddingsConfiguration(
        credentials=creds, provider=provider, name="nomic-embed-text"
    )

    with capture_dlt_logger(caplog) as caplog:
        func = config.create_embedding_function()
    assert isinstance(func, OllamaEmbeddings)
    assert "ollama" in caplog.text
    assert "_PROVIDER_ENV_VAR_NAMES" in caplog.text
    assert "ignored" in caplog.text


def test_lance_client_configuration_catalog_dispatch() -> None:
    # default catalog_type "dir" routes to DirectoryCatalog* resolved types
    c = resolve_configuration(
        LanceClientConfiguration()._bind_dataset_name(dataset_name="test_dataset"),
        sections=("destination", "lance"),
    )
    assert c.catalog_type == "dir"
    assert isinstance(c.credentials, DirectoryCatalogCredentials)
    assert isinstance(c.capabilities, DirectoryCatalogCapabilities)
    assert isinstance(c.storage, LanceStorageConfiguration)
    assert c.capabilities.manifest_enabled is True
    assert c.capabilities.dir_listing_enabled is True


def test_directory_catalog_credentials_inherit_from_storage() -> None:
    # empty catalog credentials fall back to storage location, credentials, and merged options
    c = resolve_configuration(
        LanceClientConfiguration(
            storage=LanceStorageConfiguration(bucket_url="s3://my-bucket", namespace_name="my-ns")
        )._bind_dataset_name(dataset_name="test_dataset"),
        sections=("destination", "lance"),
    )
    assert c.storage.bucket_url == "s3://my-bucket"
    assert c.storage.namespace_uri == "s3://my-bucket/my-ns"
    assert c.credentials.bucket_url == c.storage.namespace_uri
    assert c.credentials.options == c.storage.options


def test_directory_catalog_credentials_override_storage() -> None:
    # explicit catalog bucket_url points __manifest at a different location than data
    os.environ["DESTINATION__STORAGE__BUCKET_URL"] = "s3://data-bucket"
    os.environ["DESTINATION__CREDENTIALS__BUCKET_URL"] = "s3://catalog-bucket/catalog"
    os.environ["AWS_ACCESS_KEY_ID"] = "AKIAFAKE"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "fake"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    c = resolve_configuration(
        LanceClientConfiguration()._bind_dataset_name(dataset_name="test_dataset"),
        sections=("destination", "lance"),
    )
    assert c.storage.bucket_url == "s3://data-bucket"
    assert c.credentials.bucket_url == "s3://catalog-bucket/catalog"
    # catalog credentials ran their own merge — options carry AWS creds + cloud timeouts
    assert c.credentials.options is not None
    for key in ("aws_access_key_id", "aws_secret_access_key", "region", "connect_timeout"):
        assert key in c.credentials.options


@pytest.mark.parametrize(
    "catalog_bucket_url,inherits_from_storage",
    [
        (None, True),
        ("catalog_data", False),
    ],
    ids=["catalog_inherits_storage", "catalog_own_relative_path"],
)
def test_lance_follows_local_dir(
    catalog_bucket_url: Optional[str], inherits_from_storage: bool
) -> None:
    # WithLocalFiles context (local_dir, pipeline_name) must propagate to BOTH storage and
    # catalog credentials so relative paths resolve under the isolated test storage
    local_dir = os.path.join(get_test_storage_root(), uniq_id())
    os.makedirs(local_dir)
    os.environ[DLT_LOCAL_DIR] = local_dir
    abs_local_dir = os.path.abspath(local_dir)

    # default bucket_url="." resolves to local_dir on both storage and credentials
    c = resolve_configuration(
        LanceClientConfiguration(
            credentials=DirectoryCatalogCredentials(bucket_url=catalog_bucket_url)
        )._bind_dataset_name(dataset_name="test_dataset"),
        sections=("destination", "lance"),
    )
    assert c.storage.bucket_url == f"file://{abs_local_dir}"
    assert c.storage.namespace_uri == f"file://{abs_local_dir}/{DEFAULT_LANCE_NAMESPACE_NAME}"
    if inherits_from_storage:
        assert c.credentials.bucket_url == c.storage.namespace_uri
    else:
        expected_catalog = os.path.join(abs_local_dir, catalog_bucket_url)
        assert c.credentials.bucket_url == f"file://{expected_catalog}"

    # explicit relative storage.bucket_url also resolves under local_dir
    c = resolve_configuration(
        LanceClientConfiguration(
            storage=LanceStorageConfiguration(bucket_url="my_lance_data"),
            credentials=DirectoryCatalogCredentials(bucket_url=catalog_bucket_url),
        )._bind_dataset_name(dataset_name="test_dataset"),
        sections=("destination", "lance"),
    )
    expected_storage = os.path.join(abs_local_dir, "my_lance_data")
    assert c.storage.bucket_url == f"file://{expected_storage}"
    assert c.storage.namespace_uri == f"file://{expected_storage}/{DEFAULT_LANCE_NAMESPACE_NAME}"
    if inherits_from_storage:
        assert c.credentials.bucket_url == c.storage.namespace_uri
    else:
        expected_catalog = os.path.join(abs_local_dir, catalog_bucket_url)
        assert c.credentials.bucket_url == f"file://{expected_catalog}"

    # pipeline context propagates through _bind_local_files to BOTH nested configs
    pipeline = dlt.pipeline("test_lance_follows_local_dir")
    c = resolve_configuration(
        pipeline._bind_local_files(
            LanceClientConfiguration(
                credentials=DirectoryCatalogCredentials(bucket_url=catalog_bucket_url)
            )._bind_dataset_name(dataset_name="test_dataset")
        ),
        sections=("destination", "lance"),
    )
    assert c.storage.pipeline_name == "test_lance_follows_local_dir"
    assert c.storage.bucket_url == f"file://{abs_local_dir}"
    assert c.storage.namespace_uri == f"file://{abs_local_dir}/{DEFAULT_LANCE_NAMESPACE_NAME}"
    assert c.credentials.pipeline_name == "test_lance_follows_local_dir"
    if inherits_from_storage:
        assert c.credentials.bucket_url == c.storage.namespace_uri
    else:
        expected_catalog = os.path.join(abs_local_dir, catalog_bucket_url)
        assert c.credentials.bucket_url == f"file://{expected_catalog}"
