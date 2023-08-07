from typing import Any, List
import dlt
from dlt.helpers.weaviate_helper import weaviate_adapter
from dlt.destinations.weaviate.weaviate import WeaviateClient
from dlt.common.pipeline import LoadInfo

from tests.pipeline.utils import assert_load_info


def assert_unordered_list_equal(list1: List[Any], list2: List[Any]) -> None:
    assert len(list1) == len(list2)
    for item in list1:
        assert item in list2


def assert_class(pipeline: dlt.Pipeline, class_name: str, data: List[Any]) -> None:
    db_client = pipeline._destination_client().db_client

    # Check if class exists
    schema = db_client.schema.get(class_name)
    assert schema is not None

    property_names = [prop["name"] for prop in schema["properties"]]

    response = db_client.query.get(class_name, property_names).do()

    objects = response["data"]["Get"][class_name]

    # TODO: Remove this once we have a better way comparing the data
    drop_keys = ["_dlt_id", "_dlt_load_id"]
    objects_without_dlt_keys = [
        {k: v for k, v in obj.items() if k not in drop_keys} for obj in objects
    ]

    # pytest compares content wise but ignores order of elements of dict
    # assert sorted(objects_without_dlt_keys, key=lambda d: d['doc_id']) == sorted(data, key=lambda d: d['doc_id'])
    assert_unordered_list_equal(objects_without_dlt_keys, data)


def test_pipeline_append() -> None:
    def sequence_generator():
        count = 1
        while True:
            yield [{"content": str(count + i)} for i in range(3)]
            count += 3

    generator_instance1 = sequence_generator()
    generator_instance2 = sequence_generator()

    @dlt.resource
    def some_data():
        yield from next(generator_instance1)

    weaviate_adapter(
        some_data,
        vectorize=["content"],
    )

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_append",
        destination="weaviate",
        dataset_name="test_pipeline_append_dataset",
    )
    info = pipeline.run(
        some_data(),
    )

    data = next(generator_instance2)
    assert_class(info.pipeline, "SomeData", data)

    info = pipeline.run(
        some_data(),
    )
    assert_load_info(info)

    data.extend(next(generator_instance2))
    assert_class(info.pipeline, "SomeData", data)


def test_pipeline_merge() -> None:
    data = [
        {
            "doc_id": 1,
            "title": "The Shawshank Redemption",
            "description": (
                "Two imprisoned men find redemption through acts "
                "of decency over the years."
            ),
        },
        {
            "doc_id": 2,
            "title": "The Godfather",
            "description": (
                "A crime dynasty's aging patriarch transfers "
                "control to his reluctant son."
            ),
        },
        {
            "doc_id": 3,
            "title": "The Dark Knight",
            "description": (
                "The Joker wreaks havoc on Gotham, challenging "
                "The Dark Knight's ability to fight injustice."
            ),
        },
    ]

    # @weaviate_adapter(vectorize=["description"])  # TODO: make it work
    @dlt.resource(primary_key="doc_id")
    def movies_data():
        yield data

    weaviate_adapter(
        movies_data,
        vectorize=["description"]
    )

    pipeline = dlt.pipeline(
        pipeline_name="movies",
        destination="weaviate",
        dataset_name="movies_dataset",
    )
    info = pipeline.run(
        movies_data(),
        write_disposition="merge",
    )
    assert_load_info(info)
    assert_class(info.pipeline, "MoviesData", data)

    # Change some data
    data[0]["title"] = "The Shawshank Redemption 2"

    info = pipeline.run(
        movies_data(),
        write_disposition="merge",
    )
    assert_load_info(info)
    assert_class(info.pipeline, "MoviesData", data)
