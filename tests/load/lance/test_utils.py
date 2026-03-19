from typing import Any, Dict

import lancedb
import numpy as np
import pyarrow as pa
import pytest
from lancedb.embeddings import EmbeddingFunctionRegistry

import dlt
from dlt.common.schema import Schema
from dlt.destinations.impl.lance.schema import make_arrow_table_schema
from dlt.destinations.impl.lance.utils import (
    _make_lance_table_uri,
    create_empty_lance_dataset,
    set_non_standard_providers_environment_variables,
    write_records,
)


pytestmark = pytest.mark.essential


def test_write_records_matches_lancedb_table_add(tmp_path: str) -> None:
    """Asserts `write_records` util produces same table as `lancedb.Table.add()`.

    We assert this, primarily, to ensure `write_records` produces correct vector embeddings.
    """

    # get lancedb config
    config: Dict[str, Any] = dlt.secrets.get("destination.lancedb")
    provider = config.get("embedding_model_provider")
    model = config.get("embedding_model")
    api_key = config.get("credentials", {}).get("embedding_model_provider_api_key")

    # get embedding function
    set_non_standard_providers_environment_variables(provider, api_key)
    registry = EmbeddingFunctionRegistry.get_instance()
    model_func = registry.get(provider).create(name=model)

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
        vector_field_name="vector",
        embedding_fields=["text"],
        embedding_model_func=model_func,
    )

    lance_uri = tmp_path
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
    create_empty_lance_dataset(arrow_schema, uri=_make_lance_table_uri(lance_uri, "write_records"))
    write_records(records.to_reader(), lance_uri=lance_uri, table_name="write_records")

    # assert equality between between the two tables
    lancedb_tbl = db.open_table("lancedb").to_arrow()
    write_records_tbl = db.open_table("write_records").to_arrow()
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
