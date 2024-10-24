import pytest
from typing import Iterator
from tempfile import TemporaryDirectory
import os

import dlt
from dlt.common import json
from dlt.common.utils import uniq_id
from dlt.common.typing import DictStrStr

from dlt.destinations.adapters import qdrant_adapter
from dlt.destinations.impl.qdrant.qdrant_adapter import qdrant_adapter, VECTORIZE_HINT
from dlt.destinations.impl.qdrant.qdrant_job_client import QdrantClient
from tests.pipeline.utils import assert_load_info
from tests.load.qdrant.utils import drop_active_pipeline_data, assert_collection
from tests.load.utils import sequence_generator
from tests.utils import preserve_environ

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.fixture(autouse=True)
def drop_qdrant_data() -> Iterator[None]:
    yield
    drop_active_pipeline_data()


def test_adapter_and_hints() -> None:
    generator_instance1 = sequence_generator()

    @dlt.resource(columns=[{"name": "content", "data_type": "text"}])
    def some_data():
        yield from next(generator_instance1)

    assert some_data.columns["content"] == {"name": "content", "data_type": "text"}  # type: ignore[index]

    qdrant_adapter(
        some_data,
        embed=["content"],
    )

    assert some_data.columns["content"] == {"name": "content", "data_type": "text", "x-qdrant-embed": True}  # type: ignore[index]


def test_basic_state_and_schema() -> None:
    generator_instance1 = sequence_generator()

    @dlt.resource
    def some_data():
        yield from next(generator_instance1)

    qdrant_adapter(
        some_data,
        embed=["content"],
    )

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_append",
        destination="qdrant",
        dataset_name="test_pipeline_append_dataset" + uniq_id(),
    )
    info = pipeline.run(
        some_data(),
    )
    assert_load_info(info)

    client: QdrantClient
    with pipeline.destination_client() as client:  # type: ignore[assignment]
        # check if we can get a stored schema and state
        schema = client.get_stored_schema(client.schema.name)
        print("Print dataset name", client.dataset_name)
        assert schema
        state = client.get_stored_state("test_pipeline_append")
        assert state
        state = client.get_stored_state("unknown_pipeline")
        assert state is None


def test_pipeline_append() -> None:
    generator_instance1 = sequence_generator()
    generator_instance2 = sequence_generator()

    @dlt.resource
    def some_data():
        yield from next(generator_instance1)

    qdrant_adapter(
        some_data,
        embed=["content"],
    )

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_append",
        destination="qdrant",
        dataset_name="TestPipelineAppendDataset" + uniq_id(),
    )
    info = pipeline.run(
        some_data(),
    )
    assert_load_info(info)

    data = next(generator_instance2)
    assert_collection(pipeline, "some_data", items=data)

    info = pipeline.run(
        some_data(),
    )
    assert_load_info(info)

    data.extend(next(generator_instance2))
    assert_collection(pipeline, "some_data", items=data)


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

    qdrant_adapter(
        some_data,
        embed=["content"],
    )

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_append",
        destination="qdrant",
        dataset_name="TestPipelineAppendDataset" + uniq_id(),
    )
    info = pipeline.run(
        some_data(),
    )

    assert_collection(pipeline, "some_data", items=data)

    info = pipeline.run(
        some_data(),
        write_disposition="append",
    )
    assert_load_info(info)

    data.extend(data)
    assert_collection(pipeline, "some_data", items=data)


def test_pipeline_replace() -> None:
    generator_instance1 = sequence_generator()
    generator_instance2 = sequence_generator()

    @dlt.resource
    def some_data():
        yield from next(generator_instance1)

    qdrant_adapter(
        some_data,
        embed=["content"],
    )

    uid = uniq_id()

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_replace",
        destination="qdrant",
        dataset_name="test_pipeline_replace_dataset"
        + uid,  # Qdrant doesn't mandate any name normalization
    )

    info = pipeline.run(
        some_data(),
        write_disposition="replace",
    )
    assert_load_info(info)
    assert (
        info.dataset_name == "test_pipeline_replace_dataset" + uid
    )  # Qdrant doesn't mandate any name normalization

    data = next(generator_instance2)
    assert_collection(pipeline, "some_data", items=data)

    info = pipeline.run(
        some_data(),
        write_disposition="replace",
    )
    assert_load_info(info)

    data = next(generator_instance2)
    assert_collection(pipeline, "some_data", items=data)


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

    @dlt.resource(primary_key="doc_id")
    def movies_data():
        yield data

    qdrant_adapter(
        movies_data,
        embed=["description"],
    )

    pipeline = dlt.pipeline(
        pipeline_name="movies",
        destination="qdrant",
        dataset_name="TestPipelineAppendDataset" + uniq_id(),
    )
    info = pipeline.run(
        movies_data(), write_disposition="merge", dataset_name="MoviesDataset" + uniq_id()
    )
    assert_load_info(info)
    assert_collection(pipeline, "movies_data", items=data)

    # Change some data
    data[0]["title"] = "The Shawshank Redemption 2"

    info = pipeline.run(
        movies_data(),
        write_disposition="merge",
    )
    assert_load_info(info)
    assert_collection(pipeline, "movies_data", items=data)


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

    qdrant_adapter(some_data, embed=["content"])

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_append",
        destination="qdrant",
        dataset_name="TestSchemaEvolutionDataset" + uniq_id(),
    )
    pipeline.run(
        some_data(),
    )

    assert_collection(pipeline, "some_data", items=data)

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

    pipeline.run(
        some_data(),
    )

    table_schema = pipeline.default_schema.tables["some_data"]
    assert "new_column" in table_schema["columns"]

    aggregated_data.extend(data)

    assert_collection(pipeline, "some_data", items=aggregated_data)


def test_merge_github_nested() -> None:
    p = dlt.pipeline(destination="qdrant", dataset_name="github1", dev_mode=True)
    assert p.dataset_name.startswith("github1_202")

    with open(
        "tests/normalize/cases/github.issues.load_page_5_duck.json", "r", encoding="utf-8"
    ) as f:
        data = json.load(f)

    info = p.run(
        qdrant_adapter(data[:17], embed=["title", "body"]),
        table_name="issues",
        write_disposition="merge",
        primary_key="id",
    )
    assert_load_info(info)

    # assert if schema contains tables with right names
    assert set(p.default_schema.tables.keys()) == {
        "_dlt_version",
        "_dlt_loads",
        "issues",
        "_dlt_pipeline_state",
        "issues__labels",
        "issues__assignees",
    }
    assert set([t["name"] for t in p.default_schema.data_tables()]) == {
        "issues",
        "issues__labels",
        "issues__assignees",
    }
    assert set([t["name"] for t in p.default_schema.dlt_tables()]) == {
        "_dlt_version",
        "_dlt_loads",
        "_dlt_pipeline_state",
    }
    issues = p.default_schema.tables["issues"]
    assert issues["columns"]["id"]["primary_key"] is True
    # make sure that vectorization is enabled for
    assert issues["columns"]["title"][VECTORIZE_HINT]  # type: ignore[literal-required]
    assert issues["columns"]["body"][VECTORIZE_HINT]  # type: ignore[literal-required]
    assert VECTORIZE_HINT not in issues["columns"]["url"]
    assert_collection(p, "issues", expected_items_count=17)


def test_empty_dataset_allowed() -> None:
    # dataset_name is optional so dataset name won't be autogenerated when not explicitly passed
    p = dlt.pipeline(destination="qdrant", dev_mode=True)
    client: QdrantClient = p.destination_client()  # type: ignore[assignment]

    assert p.dataset_name is None
    info = p.run(qdrant_adapter(["context", "created", "not a stop word"], embed=["value"]))
    # dataset in load info is empty
    assert info.dataset_name is None
    client = p.destination_client()  # type: ignore[assignment]
    assert client.dataset_name is None
    assert client.sentinel_collection == "DltSentinelCollection"
    assert_collection(p, "content", expected_items_count=3)


def test_qdrant_local_parallelism_disabled(preserve_environ) -> None:
    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "20"

    with TemporaryDirectory() as tmpdir:
        p = dlt.pipeline(destination=dlt.destinations.qdrant(path=tmpdir))

        # Data writer limit ensures that we create multiple load files to the same table
        @dlt.resource
        def q_data():
            for i in range(222):
                yield {"doc_id": i, "content": f"content {i}"}

        info = p.run(q_data)

        assert_load_info(info)
