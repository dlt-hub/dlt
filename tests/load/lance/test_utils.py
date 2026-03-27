from pathlib import Path

import lance
import lancedb
import numpy as np
import pyarrow as pa
import pytest
from lance.namespace import CreateNamespaceRequest, DirectoryNamespace

import dlt
from dlt.common.configuration import resolve_configuration
from dlt.common.schema import Schema
from dlt.destinations.impl.lance.configuration import LanceEmbeddingsConfiguration
from dlt.destinations.impl.lance.schema import make_arrow_table_schema
from dlt.destinations.impl.lance.utils import write_records


pytestmark = pytest.mark.essential


def test_write_records_matches_lancedb_table_add(tmp_path: Path) -> None:
    """Asserts `write_records` util produces same table as `lancedb.Table.add()`.

    We assert this, primarily, to ensure `write_records` produces correct vector embeddings.
    """
    embeddings_config = resolve_configuration(
        LanceEmbeddingsConfiguration(),
        sections=("destination", "lance", "embeddings"),
    )
    model_func = embeddings_config.create_embedding_function()

    # create arrow schema with `embedding_functions` metadata
    schema = Schema("test")
    schema.update_table(
        {
            "name": "docs",
            "columns": {
                "doc_id": {"name": "doc_id", "data_type": "bigint"},
                "text": {"name": "text", "data_type": "text"},
            },
        }
    )
    arrow_schema = make_arrow_table_schema(
        "docs",
        schema=schema,
        type_mapper=dlt.destinations.lance().capabilities().get_type_mapper(),
        vector_column="vector",
        embedding_fields=["text"],
        embedding_model_func=model_func,
    )

    lance_uri = str(tmp_path)
    db = lancedb.connect(lance_uri)
    records = pa.table(
        {
            "doc_id": [1, 2],
            "text": ["Frodo was a happy puppy", "LanceDB is a vector database"],
        }
    )

    # create table + vectors via native LanceDB API
    db.create_table("lancedb", schema=arrow_schema)
    db.open_table("lancedb").add(records)

    # create table + vectors with our `write_records` util
    namespace = DirectoryNamespace(root=lance_uri)
    dataset_name = "foo"
    table_name = "write_records"
    table_id = [dataset_name, table_name]
    namespace.create_namespace(CreateNamespaceRequest(id=[dataset_name]))
    lance.write_dataset(arrow_schema.empty_table(), namespace=namespace, table_id=table_id)
    write_records(records.to_reader(), namespace=namespace, table_id=table_id)

    # assert equality between the two tables
    lancedb_tbl = db.open_table("lancedb").to_arrow()
    write_records_tbl = lance.dataset(namespace=namespace, table_id=table_id).to_table()
    # exact equality for non-vector fields
    assert write_records_tbl.select(["doc_id", "text"]).equals(
        lancedb_tbl.select(["doc_id", "text"])
    )
    # approximate equality for vector field, because model provider (openai) produces non-deterministic embeddings
    assert np.allclose(
        write_records_tbl.column("vector").to_pylist(),
        lancedb_tbl.column("vector").to_pylist(),
        atol=1e-3,
    )
