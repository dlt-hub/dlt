from typing import Iterator, cast

import lancedb
import numpy as np
import pyarrow as pa
import pytest

from dlt.common.configuration import resolve_configuration
from dlt.common.utils import uniq_id
from dlt.destinations.impl.lance.configuration import LanceEmbeddingsConfiguration
from dlt.destinations.impl.lance.lance_client import LanceClient
from dlt.destinations.impl.lancedb.lancedb_adapter import VECTORIZE_HINT

from tests.utils import (
    auto_module_test_storage,
    auto_module_test_run_context,
)
from tests.load.utils import yield_client, yield_client_with_storage


pytestmark = pytest.mark.essential


@pytest.fixture(scope="module")
def client() -> Iterator[LanceClient]:
    dataset_name = "test_" + uniq_id()
    yield from cast(Iterator[LanceClient], yield_client("lance", dataset_name=dataset_name))


@pytest.fixture()
def client_with_storage() -> Iterator[LanceClient]:
    yield from cast(Iterator[LanceClient], yield_client_with_storage("lance"))


def test_lance_client_namespace_methods(client: LanceClient) -> None:
    namespace_name = "foo"
    assert not client.child_namespace_exists(namespace_name)
    client.create_child_namespace(namespace_name)
    assert client.child_namespace_exists(namespace_name)
    client.drop_child_namespace(namespace_name)
    assert not client.child_namespace_exists(namespace_name)


def test_lance_client_storage(client: LanceClient) -> None:
    assert not client.is_storage_initialized()
    assert not client.child_namespace_exists(client.dataset_name)

    # initializing storage should create dataset namespace
    client.initialize_storage()
    assert client.is_storage_initialized()
    assert client.child_namespace_exists(client.dataset_name)

    # dropping storage should drop dataset namespace
    client.drop_storage()
    assert not client.is_storage_initialized()
    assert not client.child_namespace_exists(client.dataset_name)


def test_lance_client_create_branch_if_not_exists(client_with_storage: LanceClient) -> None:
    client = client_with_storage
    table_name = "foo"
    alt_branch_name = "dev"

    # create table
    arrow_schema = pa.schema([("id", pa.int64())])
    client.create_table(table_name, arrow_schema)

    # branch doesn't exist initially
    ds = client.open_lance_dataset(table_name)
    assert alt_branch_name not in ds.branches.list()

    # create branch
    client.create_branch_if_not_exists(table_name, alt_branch_name)

    # branch now exists
    ds = client.open_lance_dataset(table_name)
    assert alt_branch_name in ds.branches.list()

    # calling again doesn't error
    client.create_branch_if_not_exists(table_name, alt_branch_name)


def test_lance_client_write_records_into_branch(client_with_storage: LanceClient) -> None:
    client = client_with_storage
    table_name = "foo"
    alt_branch_name = "dev"

    # create table
    arrow_schema = pa.schema([("id", pa.int64())])
    client.create_table(table_name, arrow_schema)

    # write to main
    client.write_records([{"id": 1}], table_name)
    assert client.open_lance_dataset(table_name).count_rows() == 1

    # create branch and write to it
    client.create_branch_if_not_exists(table_name, alt_branch_name)
    client.write_records([{"id": 2}, {"id": 3}], table_name, branch_name=alt_branch_name)

    # main still has 1 row
    assert client.open_lance_dataset(table_name).count_rows() == 1

    # branch has 3 rows (1 from main + 2 new)
    assert client.open_lance_dataset(table_name, branch_name=alt_branch_name).count_rows() == 3


def test_lance_client_write_records_matches_lancedb_table_add(
    client_with_storage: LanceClient, tmp_path: str
) -> None:
    """Asserts `write_records` produces same table as `lancedb.Table.add()`.

    We assert this, primarily, to ensure `write_records` produces correct vector embeddings.
    """
    client = client_with_storage

    # configure embeddings on client
    embeddings_config = resolve_configuration(
        LanceEmbeddingsConfiguration(),
        sections=("destination", "lance", "embeddings"),
    )
    client.config.embeddings = embeddings_config
    client.embedding_function = embeddings_config.create_embedding_function()

    table_name = "docs"
    client.schema.update_table(
        {
            "name": table_name,
            "columns": {
                "doc_id": {"name": "doc_id", "data_type": "bigint"},
                "text": {"name": "text", "data_type": "text", VECTORIZE_HINT: True},  # type: ignore[misc]
            },
        }
    )
    arrow_schema = client.make_arrow_table_schema(table_name)

    records = pa.table(
        {
            "doc_id": [1, 2],
            "text": ["Frodo was a happy puppy", "LanceDB is a vector database"],
        }
    )

    # create table + vectors via native LanceDB API
    lance_uri = str(tmp_path)
    db = lancedb.connect(lance_uri)
    db.create_table("lancedb", schema=arrow_schema)
    db.open_table("lancedb").add(records)

    # create table + vectors via LanceClient.write_records
    client.create_table(table_name, arrow_schema)
    client.write_records(records.to_reader(), table_name)

    # assert equality between the two tables
    lancedb_tbl = db.open_table("lancedb").to_arrow()
    write_records_tbl = client.open_lance_dataset(table_name).to_table()
    assert write_records_tbl.select(["doc_id", "text"]).equals(
        lancedb_tbl.select(["doc_id", "text"])
    )
    # approximate equality for vector field — model provider (openai) produces
    # non-deterministic embeddings
    assert np.allclose(
        write_records_tbl.column("vector").to_pylist(),
        lancedb_tbl.column("vector").to_pylist(),
        atol=1e-3,
    )
