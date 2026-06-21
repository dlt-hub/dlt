import os
from typing import Iterator, Optional

import pytest

import dlt
from dlt.common.configuration import resolve_configuration
from dlt.common.known_env import DLT_LOCAL_DIR
from dlt.common.utils import digest128, uniq_id

from dlt.destinations.impl.lancedb.configuration import (
    LanceDBClientConfiguration,
)


from tests.utils import get_test_storage_root


# Mark all tests as essential, do not remove.
pytestmark = pytest.mark.essential


def test_lancedb_configuration() -> None:
    os.environ["DESTINATION__LANCEDB__EMBEDDING_MODEL_PROVIDER"] = "colbert"
    os.environ["DESTINATION__LANCEDB__EMBEDDING_MODEL"] = "text-embedding-3-small"

    config = resolve_configuration(
        LanceDBClientConfiguration()._bind_dataset_name(dataset_name="dataset"),
        sections=("destination", "lancedb"),
    )
    assert config.embedding_model_provider == "colbert"
    assert config.embedding_model == "text-embedding-3-small"


@pytest.mark.parametrize(
    "lance_uri,expected_fingerprint",
    [
        pytest.param(None, "", id="empty"),
        pytest.param("local.db", digest128("local.db"), id="raw_local_uri"),
        pytest.param("db://host/unknown", digest128("db://host/unknown"), id="raw_cloud_uri"),
        pytest.param(":external:", digest128(":external:"), id="external_native_client"),
    ],
)
def test_lancedb_fingerprint(lance_uri: Optional[str], expected_fingerprint: str) -> None:
    config = LanceDBClientConfiguration(lance_uri=lance_uri)

    assert config.fingerprint() == expected_fingerprint


def test_lancedb_follows_local_dir() -> None:
    local_dir = os.path.join(get_test_storage_root(), uniq_id())
    os.makedirs(local_dir)
    # mock tmp dir
    os.environ[DLT_LOCAL_DIR] = local_dir
    # we expect duckdb db to appear there
    c = resolve_configuration(
        LanceDBClientConfiguration()._bind_dataset_name(dataset_name="test_dataset")
    )
    db_path = os.path.join(local_dir, "lancedb.lancedb")
    # db path is relative
    assert c.lance_uri.endswith(db_path)
    assert c.lance_uri == os.path.abspath(db_path)
    assert c.credentials.uri == c.lance_uri

    # check named destination
    c = resolve_configuration(
        LanceDBClientConfiguration(destination_name="named")._bind_dataset_name(
            dataset_name="test_dataset"
        )
    )
    db_path = os.path.join(local_dir, "named.lancedb")
    assert c.lance_uri.endswith(db_path)

    # check explicit location
    c = resolve_configuration(
        LanceDBClientConfiguration(lance_uri="local.db")._bind_dataset_name(
            dataset_name="test_dataset"
        )
    )
    db_path = os.path.join(local_dir, "local.db")
    assert c.lance_uri.endswith(db_path)

    # check pipeline name
    pipeline = dlt.pipeline("test_lancedb_follows_local_dir")
    c = resolve_configuration(
        pipeline._bind_local_files(
            LanceDBClientConfiguration()._bind_dataset_name(dataset_name="test_dataset")
        )
    )
    db_path = os.path.join(local_dir, "test_lancedb_follows_local_dir.lancedb")
    assert c.lance_uri.endswith(db_path)


def test_credentials_uri_deprecation() -> None:
    os.environ["DESTINATION__LANCEDB__CREDENTIALS__URI"] = "db://host/unknown"
    c = resolve_configuration(
        LanceDBClientConfiguration()._bind_dataset_name(dataset_name="test_dataset"),
        sections=("destination", "lancedb"),
    )
    assert c.lance_uri == "db://host/unknown"
