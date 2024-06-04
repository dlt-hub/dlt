from typing import Union, List, Any

import numpy as np
from lancedb.embeddings import TextEmbeddingFunction
from lancedb.pydantic import LanceModel, Vector

import dlt
from dlt.common.configuration.container import Container
from dlt.common.pipeline import PipelineContext
from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient
from dlt.destinations.impl.lancedb.utils import infer_lancedb_model_from_data


def assert_unordered_list_equal(list1: List[Any], list2: List[Any]) -> None:
    assert len(list1) == len(list2), "Lists have different length"
    for item in list1:
        assert item in list2, f"Item {item} not found in list2"


def assert_table(
    pipeline: dlt.Pipeline,
    collection_name: str,
    expected_items_count: int = None,
    items: List[Any] = None,
) -> None:
    client: LanceDBClient = pipeline.destination_client()  # type: ignore[assignment]

    # Check whether table exists.
    exists = client._table_exists(collection_name)
    assert exists

    qualified_collection_name = client._make_qualified_table_name(collection_name)
    records = (
        client.db_client.open_table(qualified_collection_name)
        .search()
        .limit(50)
        .to_list()
    )

    if expected_items_count is not None:
        assert expected_items_count == len(records)

    if items is None:
        return

    drop_keys = ["_dlt_id", "_dlt_load_id"]
    objects_without_dlt_keys = [
        {k: v for k, v in record.items() if k not in drop_keys} for record in records
    ]

    assert_unordered_list_equal(objects_without_dlt_keys, items)


def drop_active_pipeline_data() -> None:
    print("Dropping active pipeline data for test.")

    def has_tables(client: LanceDBClient) -> bool:
        schema = list(client.db_client.table_names())
        return len(schema) > 0

    if Container()[PipelineContext].is_active():
        # take existing pipeline
        p = dlt.pipeline()
        client: LanceDBClient = p.destination_client()  # type: ignore[assignment]

        if has_tables(client):
            client.drop_storage()

        p._wipe_working_folder()
        # deactivate context
        Container()[PipelineContext].deactivate()


class MockEmbeddingFunc(TextEmbeddingFunction):
    def generate_embeddings(  # type: ignore
        self, texts: Union[List[str], np.ndarray]
    ) -> List[np.array]:
        return [np.array(None)]

    def ndims(self) -> int:
        return 2


def test_infer_lancedb_model_from_data() -> None:
    data = [
        {"id__": "1", "item": "tyre", "price": 12.0, "customer": "jack"},
        {"id__": "2", "item": "wheel", "price": 100, "customer": "jill"},
    ]
    id_field_name = "id__"
    vector_field_name = "vector__"
    embedding_fields = ["item", "price"]
    embedding_model_func = MockEmbeddingFunc()

    inferred_model = infer_lancedb_model_from_data(
        data, id_field_name, vector_field_name, embedding_fields, embedding_model_func
    )

    expected_fields = {
        "id__": (str, ...),
        "vector__": (Vector(2), ...),
        "item": (str, embedding_model_func.SourceField()),
        "price": (float, embedding_model_func.SourceField()),
        "customer": (str, ...),
    }

    assert issubclass(inferred_model, LanceModel)
    for field_name, (field_type, field_default) in expected_fields.items():
        assert field_name in inferred_model.model_fields
