import os
from typing import Iterator

import pytest

import dlt
from dlt.common.configuration import resolve_configuration
from dlt.common.utils import uniq_id

from dlt.destinations.impl.lancedb.configuration import (
    LanceDBClientConfiguration,
)

from tests.load.utils import (
    drop_active_pipeline_data,
)
from tests.utils import TEST_STORAGE_ROOT


# Mark all tests as essential, do not remove.
pytestmark = pytest.mark.essential


@pytest.fixture(autouse=True)
def drop_lancedb_data() -> Iterator[None]:
    yield
    drop_active_pipeline_data()


def test_lancedb_configuration() -> None:
    os.environ["DESTINATION__LANCEDB__EMBEDDING_MODEL_PROVIDER"] = "colbert"
    os.environ["DESTINATION__LANCEDB__EMBEDDING_MODEL"] = "text-embedding-3-small"

    config = resolve_configuration(
        LanceDBClientConfiguration()._bind_dataset_name(dataset_name="dataset"),
        sections=("destination", "lancedb"),
    )
    assert config.embedding_model_provider == "colbert"
    assert config.embedding_model == "text-embedding-3-small"


def test_lancedb_follows_tmp_dir() -> None:
    tmp_dir = os.path.join(TEST_STORAGE_ROOT, uniq_id())
    os.makedirs(tmp_dir)
    # mock tmp dir
    os.environ["DLT_TMP_DIR"] = tmp_dir
    # we expect duckdb db to appear there
    c = resolve_configuration(
        LanceDBClientConfiguration()._bind_dataset_name(dataset_name="test_dataset")
    )
    db_path = os.path.join(tmp_dir, ".lancedb")
    # db path is relative
    assert c.lance_uri.endswith(db_path)
    assert c.lance_uri == os.path.abspath(db_path)
    assert c.credentials.uri is None

    # check named destination
    c = resolve_configuration(
        LanceDBClientConfiguration(destination_name="named")._bind_dataset_name(
            dataset_name="test_dataset"
        )
    )
    db_path = os.path.join(tmp_dir, "named.lancedb")
    assert c.lance_uri.endswith(db_path)

    # check explicit location
    c = resolve_configuration(
        LanceDBClientConfiguration(lance_uri="local.db")._bind_dataset_name(
            dataset_name="test_dataset"
        )
    )
    db_path = os.path.join(tmp_dir, "local.db")
    assert c.lance_uri.endswith(db_path)

    # check pipeline name
    pipeline = dlt.pipeline("test_lancedb_follows_tmp_dir")
    c = resolve_configuration(
        LanceDBClientConfiguration()
        ._bind_dataset_name(dataset_name="test_dataset")
        ._bind_local_files(pipeline)
    )
    db_path = os.path.join(tmp_dir, "test_lancedb_follows_tmp_dir.lancedb")
    assert c.lance_uri.endswith(db_path)


def test_credentials_uri_deprecation() -> None:
    os.environ["DESTINATION__LANCEDB__CREDENTIALS__URI"] = "db://host/unknown"
    c = resolve_configuration(
        LanceDBClientConfiguration()._bind_dataset_name(dataset_name="test_dataset"),
        sections=("destination", "lancedb"),
    )
    assert c.lance_uri == "db://host/unknown"
