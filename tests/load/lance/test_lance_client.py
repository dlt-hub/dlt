from typing import Iterator, cast

import lancedb
import numpy as np
import pyarrow as pa
import pytest

import dlt
from dlt.common.configuration import resolve_configuration
from dlt.common.schema import Schema
from dlt.common.utils import uniq_id
from dlt.destinations.impl.lance.configuration import LanceEmbeddingsConfiguration
from dlt.destinations.impl.lance.lance_client import LanceClient
from dlt.destinations.impl.lance.schema import make_arrow_table_schema

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
    assert not client.namespace_exists(namespace_name)
    client.create_namespace(namespace_name)
    assert client.namespace_exists(namespace_name)
    client.drop_namespace(namespace_name)
    assert not client.namespace_exists(namespace_name)


def test_lance_client_storage(client: LanceClient) -> None:
    assert not client.is_storage_initialized()
    assert not client.namespace_exists(client.dataset_name)

    # initializing storage should create dataset namespace
    client.initialize_storage()
    assert client.is_storage_initialized()
    assert client.namespace_exists(client.dataset_name)

    # dropping storage should drop dataset namespace
    client.drop_storage()
    assert not client.is_storage_initialized()
    assert not client.namespace_exists(client.dataset_name)


def test_write_records_matches_lancedb_table_add(
    client_with_storage: LanceClient, tmp_path: str
) -> None:
    """Asserts `write_records` produces same table as `lancedb.Table.add()`.

    We assert this, primarily, to ensure `write_records` produces correct vector embeddings.
    """
    embeddings_config = resolve_configuration(
        LanceEmbeddingsConfiguration(),
        sections=("destination", "lance", "embeddings"),
    )
    model_func = embeddings_config.create_embedding_function()

    table_name = "docs"
    schema = Schema("test")
    schema.update_table(
        {
            "name": table_name,
            "columns": {
                "doc_id": {"name": "doc_id", "data_type": "bigint"},
                "text": {"name": "text", "data_type": "text"},
            },
        }
    )
    arrow_schema = make_arrow_table_schema(
        table_name,
        schema=schema,
        type_mapper=dlt.destinations.lance().capabilities().get_type_mapper(),
        vector_column="vector",
        embedding_fields=["text"],
        embedding_model_func=model_func,
    )

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
    client = client_with_storage
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
