from typing import Union, List, Any

import numpy as np
from lancedb.embeddings import TextEmbeddingFunction  # type: ignore

import dlt
from dlt.common.configuration.container import Container
from dlt.common.pipeline import PipelineContext
from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient


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

    exists = client._table_exists(collection_name)
    assert exists

    qualified_collection_name = client._make_qualified_table_name(collection_name)
    records = client.db_client.open_table(qualified_collection_name).search().limit(50).to_list()

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
        pipe = dlt.pipeline()
        client: LanceDBClient = pipe.destination_client()  # type: ignore[assignment]

        if has_tables(client):
            client.drop_storage()

        pipe._wipe_working_folder()
        Container()[PipelineContext].deactivate()


class MockEmbeddingFunc(TextEmbeddingFunction):
    def generate_embeddings(
        self,
        texts: Union[List[str], np.ndarray],  # type: ignore[type-arg]
    ) -> List[np.array]:  # type: ignore[valid-type]
        return [np.array(None)]

    def ndims(self) -> int:
        return 2
