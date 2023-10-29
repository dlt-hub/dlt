import dlt
from typing import Any, List

import dlt
from dlt.common.pipeline import PipelineContext
from dlt.common.configuration.container import Container

from dlt.destinations.qdrant.qdrant import QdrantClient


def assert_unordered_list_equal(list1: List[Any], list2: List[Any]) -> None:
    assert len(list1) == len(list2), "Lists have different length"
    for item in list1:
        assert item in list2, f"Item {item} not found in list2"


def assert_class(
    pipeline: dlt.Pipeline,
    class_name: str,
    expected_items_count: int = None,
    items: List[Any] = None,
) -> None:
    client: QdrantClient = pipeline.destination_client()

    # Check if class exists
    exists = client.table_exists(class_name)
    assert exists

    columns = pipeline.default_schema.get_table_columns(class_name)
    qualified_class_name = client.make_qualified_class_name(class_name)
    point_records, offset = client.db_client.scroll(qualified_class_name, with_payload=True, limit=50)

    if expected_items_count is not None:
        assert expected_items_count == len(point_records)

    if items is None:
        return

    drop_keys = ["_dlt_id", "_dlt_load_id"]
    objects_without_dlt_keys = [
        {k: v for k, v in point.payload.items() if k not in drop_keys} for point in point_records
    ]

    assert_unordered_list_equal(objects_without_dlt_keys, items)


def delete_classes(p, class_list):
    db_client = p.destination_client().db_client
    for class_name in class_list:
        db_client.delete_collection(class_name)


def drop_active_pipeline_data() -> None:
    print("Dropping active pipeline data for test")
    def schema_has_classes(client):
        schema = client.db_client.get_collections().collections
        print(schema)
        return len(schema) > 0

    if Container()[PipelineContext].is_active():
        # take existing pipeline
        p = dlt.pipeline()
        client: QdrantClient = p.destination_client()

        if schema_has_classes(client):
            client.drop_storage()

        p._wipe_working_folder()
        # deactivate context
        Container()[PipelineContext].deactivate()
