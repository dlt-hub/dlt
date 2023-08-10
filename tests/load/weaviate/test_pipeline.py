from typing import Any, List

import dlt
from dlt.common import json
from dlt.common.schema.typing import TTableSchema

from dlt.destinations.weaviate.weaviate_adapter import weaviate_adapter, VECTORIZE_HINT, TOKENIZATION_HINT
from dlt.destinations.weaviate.weaviate_client import WeaviateClient

from tests.pipeline.utils import assert_load_info


def assert_unordered_list_equal(list1: List[Any], list2: List[Any]) -> None:
    assert len(list1) == len(list2)
    for item in list1:
        assert item in list2


def assert_class(pipeline: dlt.Pipeline, class_name: str, expected_items_count: int = None, items: List[Any] = None) -> None:
    db_client = pipeline._destination_client().db_client

    # Check if class exists
    schema = db_client.schema.get(class_name)
    assert schema is not None

    columns = pipeline.default_schema.get_table_columns(class_name)

    properties = {prop["name"]:prop for prop in schema["properties"]}
    assert set(properties.keys()) == set(columns.keys())

    # make sure expected columns are vectorized
    for column_name, column in columns.items():
        prop = properties[column_name]
        # text2vec-openai is the default
        assert prop["moduleConfig"]["text2vec-openai"]["skip"] == (not column.get(VECTORIZE_HINT, False))
        # tokenization
        if TOKENIZATION_HINT in column:
            assert prop["tokenization"] == column[TOKENIZATION_HINT]

    response = db_client.query.get(class_name, list(properties.keys())).do()
    objects = response["data"]["Get"][class_name]

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


def sequence_generator():
    count = 1
    while True:
        yield [{"content": str(count + i)} for i in range(3)]
        count += 3


def test_pipeline_append() -> None:
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
        dataset_name="TestPipelineAppendDataset",
    )
    info = pipeline.run(
        some_data(),
    )
    assert_load_info(info)

    data = next(generator_instance2)
    assert_class(info.pipeline, "SomeData", items=data)

    info = pipeline.run(
        some_data(),
    )
    assert_load_info(info)

    data.extend(next(generator_instance2))
    assert_class(info.pipeline, "SomeData", items=data)


def test_explicit_append() -> None:
    """Append should work even when primary key is specified."""
    data = [
        {"doc_id": 1, "content": "1"},
        {"doc_id": 2, "content": "2"},
        {"doc_id": 3, "content": "3"},
    ]

    @dlt.resource(primary_key="doc_id")
    def some_data():
        yield data

    weaviate_adapter(
        some_data,
        vectorize=["content"],
    )

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_append",
        destination="weaviate",
        dataset_name="TestPipelineAppendDataset",
    )
    info = pipeline.run(
        some_data(),
    )

    assert_class(info.pipeline, "SomeData", items=data)

    info = pipeline.run(
        some_data(),
        write_disposition="append",
    )
    assert_load_info(info)

    data.extend(data)
    assert_class(info.pipeline, "SomeData", items=data)


def test_pipeline_replace() -> None:
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
        pipeline_name="test_pipeline_replace",
        destination="weaviate",
        dataset_name="test_pipeline_replace_dataset",  # normalized internally
    )

    info = pipeline.run(
        some_data(),
        write_disposition="replace",
    )
    assert_load_info(info)
    assert info.dataset_name == "TestPipelineReplaceDataset"  # normalized internally

    data = next(generator_instance2)
    assert_class(info.pipeline, "SomeData", items=data)

    info = pipeline.run(
        some_data(),
        write_disposition="replace",
    )
    assert_load_info(info)

    data = next(generator_instance2)
    assert_class(info.pipeline, "SomeData", items=data)


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

    weaviate_adapter(movies_data, vectorize=["description"])

    pipeline = dlt.pipeline(
        pipeline_name="movies",
        destination="weaviate",
        dataset_name="MoviesDataset",
    )
    info = pipeline.run(
        movies_data(),
        write_disposition="merge",
    )
    assert_load_info(info)
    assert_class(info.pipeline, "MoviesData", items=data)

    # Change some data
    data[0]["title"] = "The Shawshank Redemption 2"

    info = pipeline.run(
        movies_data(),
        write_disposition="merge",
    )
    assert_load_info(info)
    assert_class(info.pipeline, "MoviesData", items=data)


def test_pipeline_with_schema_evolution():
    data = [
        {
            "doc_id": 1,
            "content": "1",
        },
        {
            "doc_id": 2,
            "content": "2",
        },
    ]

    @dlt.resource()
    def some_data():
        yield data

    weaviate_adapter(some_data, vectorize=["content"])

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_append",
        destination="weaviate",
        dataset_name="TestSchemaEvolutionDataset",
    )
    info = pipeline.run(
        some_data(),
    )

    assert_class(info.pipeline, "SomeData", items=data)

    aggregated_data = data.copy()

    data = [
        {
            "doc_id": 3,
            "content": "3",
            "new_column": "new",
        },
        {
            "doc_id": 4,
            "content": "4",
            "new_column": "new",
        },
    ]

    info = pipeline.run(
        some_data(),
    )

    table_schema = pipeline.default_schema.tables["SomeData"]
    assert "new_column" in table_schema["columns"]

    aggregated_data.extend(data)
    aggregated_data[0]["new_column"] = None
    aggregated_data[1]["new_column"] = None

    assert_class(info.pipeline, "SomeData", items=aggregated_data)


def test_merge_github_nested() -> None:
    p = dlt.pipeline(destination="weaviate", dataset_name="github1", full_refresh=True)
    assert p.dataset_name.startswith("github1_202")

    with open("tests/normalize/cases/github.issues.load_page_5_duck.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    info = p.run(
        weaviate_adapter(data[:17], vectorize=["title", "body"], tokenization={"user__login": "lowercase"}),
        table_name="issues",
        write_disposition="merge",
        primary_key="id"
    )
    assert_load_info(info)
    # assert if schema contains tables with right names
    assert set(p.default_schema.tables.keys()) == {'DltVersion', 'DltLoads', 'Issues', 'DltPipelineState', 'Issues__Labels', 'Issues__Assignees'}
    assert set([t["name"] for t in p.default_schema.data_tables()]) == {'Issues', 'Issues__Labels', 'Issues__Assignees'}
    assert set([t["name"] for t in p.default_schema.dlt_tables()]) == {'DltVersion', 'DltLoads', 'DltPipelineState'}
    issues = p.default_schema.tables["Issues"]
    # make sure that both "id" column and "primary_key" were changed to __id
    assert issues["columns"]["__id"]["primary_key"] is True
    # make sure that vectorization is enabled for
    assert issues["columns"]["title"][VECTORIZE_HINT]
    assert issues["columns"]["body"][VECTORIZE_HINT]
    assert VECTORIZE_HINT not in issues["columns"]["url"]
    assert issues["columns"]["user__login"][TOKENIZATION_HINT] == "lowercase"

    assert_class(p, "Issues", expected_items_count=17)


def test_empty_dataset_allowed() -> None:
    # weaviate dataset_name is optional so dataset name won't be autogenerated when not explicitly passed
    p = dlt.pipeline(destination="weaviate", full_refresh=True)
    assert p.dataset_name is None
    info = p.run(weaviate_adapter(["a", "b", "c"], vectorize=["value"]))
    # dataset in load info is empty
    assert info.dataset_name is None
    # also check trace
    print(p.last_trace.steps[-1].step_info)
    assert_class(p, "Content", expected_items_count=3)


def test_vectorize_property_without_data() -> None:
    # we request to vectorize "content" but property with this name does not appear in the data
    # an incomplete column was created and it can't be created at destination
    p = dlt.pipeline(destination="weaviate", full_refresh=True)
    assert p.dataset_name is None
    info = p.run(weaviate_adapter(["a", "b", "c"], vectorize=["content"]))
    # dataset in load info is empty
    assert_load_info(info)
    assert_class(p, "Content", expected_items_count=3)