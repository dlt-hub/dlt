from typing import Any, Dict

import numpy as np
import lancedb
import pyarrow as pa
import pytest
from lancedb.embeddings import EmbeddingFunctionRegistry

import dlt
from dlt.common.schema import Schema
from dlt.destinations.impl.lancedb.exceptions import is_lancedb_not_found_error
from dlt.destinations.impl.lancedb.schema import make_arrow_table_schema
from dlt.destinations.impl.lancedb.utils import (
    _make_lance_table_uri,
    create_empty_lance_dataset,
    create_in_filter,
    set_non_standard_providers_environment_variables,
    write_records,
)


# Mark all tests as essential, don't remove.
pytestmark = pytest.mark.essential


def test_create_filter_condition() -> None:
    assert (
        create_in_filter("_dlt_load_id", pa.array(["A", "B", "C'c\n"]))
        == "_dlt_load_id IN ('A', 'B', 'C''c\\n')"
    )
    assert (
        create_in_filter("_dlt_load_id", pa.array([1.2, 3, 5 / 2]))
        == "_dlt_load_id IN (1.2, 3.0, 2.5)"
    )


def test_lancedb_exception_parsing() -> None:
    assert is_lancedb_not_found_error("Unknown table 'test_table'")
    assert is_lancedb_not_found_error("unknown table 'test_table'")
    assert is_lancedb_not_found_error("Field 'test_field' not found")
    assert is_lancedb_not_found_error("Column 'test_column' not found")
    assert is_lancedb_not_found_error("Missing value for column 'test_column'")
    assert is_lancedb_not_found_error("Missing column 'test_column'")


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
        type_mapper=dlt.destinations.lancedb().capabilities().get_type_mapper(),
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
