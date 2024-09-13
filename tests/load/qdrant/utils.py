import dlt
from typing import Any, List

import dlt
from dlt.common.pipeline import PipelineContext
from dlt.common.configuration.container import Container

from dlt.destinations.impl.qdrant.qdrant_job_client import QdrantClient


def assert_unordered_list_equal(list1: List[Any], list2: List[Any]) -> None:
    assert len(list1) == len(list2), "Lists have different length"
    for item in list1:
        assert item in list2, f"Item {item} not found in list2"


def assert_collection(
    pipeline: dlt.Pipeline,
    collection_name: str,
    expected_items_count: int = None,
    items: List[Any] = None,
) -> None:
    client: QdrantClient
    with pipeline.destination_client() as client:  # type: ignore[assignment]
        # Check if collection exists
        exists = client._collection_exists(collection_name)
        assert exists

        qualified_collection_name = client._make_qualified_collection_name(collection_name)
        point_records, offset = client.db_client.scroll(
            qualified_collection_name, with_payload=True, limit=50
        )

    if expected_items_count is not None:
        assert expected_items_count == len(point_records)

    if items is None:
        return

    drop_keys = ["_dlt_id", "_dlt_load_id"]
    objects_without_dlt_keys = [
        {k: v for k, v in point.payload.items() if k not in drop_keys} for point in point_records
    ]

    assert_unordered_list_equal(objects_without_dlt_keys, items)


def drop_active_pipeline_data() -> None:
    print("Dropping active pipeline data for test")

    def has_collections(client):
        schema = client.db_client.get_collections().collections
        return len(schema) > 0

    if Container()[PipelineContext].is_active():
        # take existing pipeline
        p = dlt.pipeline()
        client: QdrantClient

        with p.destination_client() as client:  # type: ignore[assignment]
            if has_collections(client):
                client.drop_storage()

        p._wipe_working_folder()
        # deactivate context
        Container()[PipelineContext].deactivate()
