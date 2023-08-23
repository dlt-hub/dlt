import dlt
from typing import Any, List

import dlt
from dlt.common.pipeline import PipelineContext
from dlt.common.configuration.container import Container

from dlt.destinations.weaviate.weaviate_client import WeaviateClient
from dlt.destinations.weaviate.weaviate_adapter import VECTORIZE_HINT, TOKENIZATION_HINT


def assert_unordered_list_equal(list1: List[Any], list2: List[Any]) -> None:
    assert len(list1) == len(list2)
    for item in list1:
        assert item in list2


def assert_class(
    pipeline: dlt.Pipeline,
    class_name: str,
    expected_items_count: int = None,
    items: List[Any] = None,
) -> None:
    client: WeaviateClient = pipeline._destination_client()

    # Check if class exists
    schema = client.get_class_schema(class_name)
    assert schema is not None

    columns = pipeline.default_schema.get_table_columns(class_name)

    properties = {prop["name"]: prop for prop in schema["properties"]}
    assert set(properties.keys()) == set(columns.keys())

    # make sure expected columns are vectorized
    for column_name, column in columns.items():
        prop = properties[column_name]
        # text2vec-openai is the default
        assert prop["moduleConfig"]["text2vec-openai"]["skip"] == (
            not column.get(VECTORIZE_HINT, False)
        )
        # tokenization
        if TOKENIZATION_HINT in column:
            assert prop["tokenization"] == column[TOKENIZATION_HINT]

    # response = db_client.query.get(class_name, list(properties.keys())).do()
    response = client.query_class(class_name, list(properties.keys())).do()
    objects = response["data"]["Get"][client.make_full_name(class_name)]

    if expected_items_count is not None:
        assert expected_items_count == len(objects)

    if items is None:
        return

    # TODO: Remove this once we have a better way comparing the data
    drop_keys = ["_dlt_id", "_dlt_load_id"]
    objects_without_dlt_keys = [
        {k: v for k, v in obj.items() if k not in drop_keys} for obj in objects
    ]

    # pytest compares content wise but ignores order of elements of dict
    # assert sorted(objects_without_dlt_keys, key=lambda d: d['doc_id']) == sorted(data, key=lambda d: d['doc_id'])
    assert_unordered_list_equal(objects_without_dlt_keys, items)


def delete_classes(p, class_list):
    db_client = p._destination_client().db_client
    for class_name in class_list:
        db_client.schema.delete_class(class_name)

def drop_active_pipeline_data() -> None:
    def schema_has_classes(client):
        schema = client.db_client.schema.get()
        return schema["classes"]

    if Container()[PipelineContext].is_active():
        # take existing pipeline
        p = dlt.pipeline()
        client = p._destination_client()

        if schema_has_classes(client):
            client.drop_dataset()

        p._wipe_working_folder()
        # deactivate context
        Container()[PipelineContext].deactivate()
