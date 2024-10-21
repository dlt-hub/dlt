import os
import pytest
from typing import Iterator

import dlt
from dlt.common import json
from dlt.common.schema.exceptions import (
    SchemaCorruptedException,
    SchemaIdentifierNormalizationCollision,
)
from dlt.common.utils import uniq_id

from dlt.destinations.adapters import weaviate_adapter
from dlt.destinations.impl.weaviate.exceptions import PropertyNameConflict
from dlt.destinations.impl.weaviate.weaviate_adapter import VECTORIZE_HINT, TOKENIZATION_HINT
from dlt.destinations.impl.weaviate.weaviate_client import WeaviateClient

from dlt.pipeline.exceptions import PipelineStepFailed
from tests.pipeline.utils import assert_load_info
from .utils import assert_class, drop_active_pipeline_data
from tests.load.utils import sequence_generator

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture(autouse=True)
def drop_weaviate_schema() -> Iterator[None]:
    yield
    drop_active_pipeline_data()


def test_adapter_and_hints() -> None:
    generator_instance1 = sequence_generator()

    @dlt.resource(columns=[{"name": "content", "data_type": "text"}])
    def some_data():
        yield from next(generator_instance1)

    assert some_data.columns["content"] == {"name": "content", "data_type": "text"}  # type: ignore[index]

    # adapter merges with existing columns
    weaviate_adapter(
        some_data,
        vectorize=["content"],
    )
    assert some_data.columns["content"] == {"name": "content", "data_type": "text", "x-weaviate-vectorize": True}  # type: ignore[index]


def test_basic_state_and_schema() -> None:
    generator_instance1 = sequence_generator()

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
        dataset_name="TestPipelineAppendDataset" + uniq_id(),
    )
    info = pipeline.run(
        some_data(),
    )
    assert_load_info(info)

    client: WeaviateClient
    with pipeline.destination_client() as client:  # type: ignore[assignment]
        # check if we can get a stored schema and state
        schema = client.get_stored_schema(client.schema.name)
        assert schema
        state = client.get_stored_state("test_pipeline_append")
        assert state


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
        dataset_name="TestPipelineAppendDataset" + uniq_id(),
    )
    info = pipeline.run(
        some_data(),
    )
    assert_load_info(info)

    data = next(generator_instance2)
    assert_class(pipeline, "SomeData", items=data)

    info = pipeline.run(
        some_data(),
    )
    assert_load_info(info)

    data.extend(next(generator_instance2))
    assert_class(pipeline, "SomeData", items=data)


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
        dataset_name="TestPipelineAppendDataset" + uniq_id(),
    )
    info = pipeline.run(
        some_data(),
    )

    assert_class(pipeline, "SomeData", items=data)

    info = pipeline.run(
        some_data(),
        write_disposition="append",
    )
    assert_load_info(info)

    data.extend(data)
    assert_class(pipeline, "SomeData", items=data)


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

    uid = uniq_id()

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_replace",
        destination="weaviate",
        dataset_name="test_pipeline_replace_dataset" + uid,  # normalized internally
    )

    info = pipeline.run(
        some_data(),
        write_disposition="replace",
    )
    assert_load_info(info)
    assert info.dataset_name == "TestPipelineReplaceDataset" + uid  # normalized internally

    data = next(generator_instance2)
    assert_class(pipeline, "SomeData", items=data)

    info = pipeline.run(
        some_data(),
        write_disposition="replace",
    )
    assert_load_info(info)

    data = next(generator_instance2)
    assert_class(pipeline, "SomeData", items=data)


def test_pipeline_merge() -> None:
    data = [
        {
            "doc_id": 1,
            "title": "The Shawshank Redemption",
            "description": (
                "Two imprisoned men find redemption through acts of decency over the years."
            ),
        },
        {
            "doc_id": 2,
            "title": "The Godfather",
            "description": (
                "A crime dynasty's aging patriarch transfers control to his reluctant son."
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
        dataset_name="MoviesDataset" + uniq_id(),
    )
    info = pipeline.run(
        movies_data(),
        write_disposition="merge",
    )
    assert_load_info(info)
    assert_class(pipeline, "MoviesData", items=data)

    # Change some data
    data[0]["title"] = "The Shawshank Redemption 2"

    info = pipeline.run(
        movies_data(),
        write_disposition="merge",
    )
    assert_load_info(info)
    assert_class(pipeline, "MoviesData", items=data)


@pytest.mark.parametrize("vectorized", (True, False), ids=("vectorized", "not-vectorized"))
def test_pipeline_with_schema_evolution(vectorized: bool):
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

    if vectorized:
        weaviate_adapter(some_data, vectorize=["content"])

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_append",
        destination="weaviate",
        dataset_name="TestSchemaEvolutionDataset" + uniq_id(),
    )
    pipeline.run(
        some_data(),
    )

    assert_class(pipeline, "SomeData", items=data)

    aggregated_data = data.copy()

    data = [
        {
            "doc_id": 3,
            "content": "3",
            "new_column": "new",
            "new_vec_column": "lorem lorem",
        },
        {
            "doc_id": 4,
            "content": "4",
            "new_column": "new",
            "new_vec_column": "lorem lorem",
        },
    ]

    some_data_2 = some_data()

    if vectorized:
        weaviate_adapter(some_data_2, vectorize=["new_vec_column"])

    pipeline.run(some_data_2)

    table_schema = pipeline.default_schema.tables["SomeData"]
    assert "new_column" in table_schema["columns"]

    aggregated_data.extend(data)
    aggregated_data[0]["new_column"] = None
    aggregated_data[1]["new_column"] = None
    aggregated_data[0]["new_vec_column"] = None
    aggregated_data[1]["new_vec_column"] = None

    assert_class(pipeline, "SomeData", items=aggregated_data)


def test_merge_github_nested() -> None:
    p = dlt.pipeline(destination="weaviate", dataset_name="github1", dev_mode=True)
    assert p.dataset_name.startswith("github1_202")

    with open(
        "tests/normalize/cases/github.issues.load_page_5_duck.json", "r", encoding="utf-8"
    ) as f:
        data = json.load(f)

    info = p.run(
        weaviate_adapter(
            data[:17], vectorize=["title", "body"], tokenization={"user__login": "lowercase"}
        ),
        table_name="issues",
        write_disposition="merge",
        primary_key="id",
    )
    assert_load_info(info)
    # assert if schema contains tables with right names
    assert set(p.default_schema.tables.keys()) == {
        "DltVersion",
        "DltLoads",
        "Issues",
        "DltPipelineState",
        "Issues__Labels",
        "Issues__Assignees",
    }
    assert set([t["name"] for t in p.default_schema.data_tables()]) == {
        "Issues",
        "Issues__Labels",
        "Issues__Assignees",
    }
    assert set([t["name"] for t in p.default_schema.dlt_tables()]) == {
        "DltVersion",
        "DltLoads",
        "DltPipelineState",
    }
    issues = p.default_schema.tables["Issues"]
    # make sure that both "id" column and "primary_key" were changed to __id
    assert issues["columns"]["__id"]["primary_key"] is True
    # make sure that vectorization is enabled for
    assert issues["columns"]["title"][VECTORIZE_HINT]  # type: ignore[literal-required]
    assert issues["columns"]["body"][VECTORIZE_HINT]  # type: ignore[literal-required]
    assert VECTORIZE_HINT not in issues["columns"]["url"]
    assert issues["columns"]["user__login"][TOKENIZATION_HINT] == "lowercase"  # type: ignore[literal-required]
    assert_class(p, "Issues", expected_items_count=17)


def test_empty_dataset_allowed() -> None:
    # weaviate dataset_name is optional so dataset name won't be autogenerated when not explicitly passed
    p = dlt.pipeline(destination="weaviate", dev_mode=True)
    # check if we use localhost
    client: WeaviateClient = p.destination_client()  # type: ignore[assignment]
    if "localhost" not in client.config.credentials.url:
        pytest.skip("skip to avoid race condition with other tests")

    assert p.dataset_name is None
    info = p.run(weaviate_adapter(["context", "created", "not a stop word"], vectorize=["value"]))
    # dataset in load info is empty
    assert info.dataset_name is None
    client = p.destination_client()  # type: ignore[assignment]
    assert client.dataset_name is None
    assert client.sentinel_class == "DltSentinelClass"
    # also check trace
    print(p.last_trace.steps[-1].step_info)
    assert_class(p, "Content", expected_items_count=3)


def test_vectorize_property_without_data() -> None:
    # disable state sync to not restore schemas when we switch
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    # we request to vectorize "content" but property with this name does not appear in the data
    # an incomplete column was created and it can't be created at destination
    dataset_name = "without_data_" + uniq_id()
    p = dlt.pipeline(destination="weaviate", dataset_name=dataset_name)

    info = p.run(weaviate_adapter(["a", "b", "c"], vectorize=["content"]))
    # dataset in load info is empty
    assert_load_info(info)
    assert_class(p, "Content", expected_items_count=3)

    # here we increase the abuse and try to vectorize a `Value` field, where in the data there's `value`
    # in standard naming convention this results in property conflict
    with pytest.raises(PipelineStepFailed) as pipe_ex:
        p.run(
            weaviate_adapter(["a", "b", "c"], vectorize="vAlue"),
            primary_key="vAlue",
            columns={"vAlue": {"data_type": "text"}},
        )
    assert isinstance(pipe_ex.value.__context__, SchemaIdentifierNormalizationCollision)

    # set the naming convention to case insensitive
    os.environ["SCHEMA__NAMING"] = "dlt.destinations.impl.weaviate.ci_naming"
    # dlt.config["schema.naming"] = "dlt.destinations.impl.weaviate.ci_naming"
    # create new schema with changed naming convention
    p = p.drop()
    info = p.run(
        weaviate_adapter(["there are", "no stop", "words in here"], vectorize="vAlue"),
        primary_key="vALue",
        columns={"vAlue": {"data_type": "text"}},
    )
    # dataset in load info is empty
    assert_load_info(info)
    # print(p.default_schema.to_pretty_yaml())
    table_schema = p.default_schema.get_table("Content")
    value_column = table_schema["columns"]["value"]
    assert value_column["primary_key"] is True
    assert value_column["x-weaviate-vectorize"] is True  # type: ignore[typeddict-item]

    # we forced schema change in the pipeline but weaviate does not support enabling vectorization on existing properties and classes
    # so mock the class otherwise the test will not pass
    value_column["x-weaviate-vectorize"] = False  # type: ignore[typeddict-unknown-key]
    assert_class(p, "Content", expected_items_count=6)
