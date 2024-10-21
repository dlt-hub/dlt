import multiprocessing
from typing import Iterator, Generator, Any, List, Mapping

import pytest
import lancedb  # type: ignore
from lancedb import DBConnection
from lancedb.embeddings import EmbeddingFunctionRegistry  # type: ignore

import dlt
from dlt.common import json
from dlt.common.typing import DictStrStr, DictStrAny
from dlt.common.utils import uniq_id
from dlt.destinations.impl.lancedb.lancedb_adapter import (
    lancedb_adapter,
    VECTORIZE_HINT,
)
from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient
from tests.load.lancedb.utils import assert_table
from tests.load.utils import sequence_generator, drop_active_pipeline_data
from tests.pipeline.utils import assert_load_info

# Mark all tests as essential, do not remove.
pytestmark = pytest.mark.essential


@pytest.fixture(autouse=True)
def drop_lancedb_data() -> Iterator[Any]:
    yield
    drop_active_pipeline_data()


def test_adapter_and_hints() -> None:
    generator_instance1 = sequence_generator()

    @dlt.resource(columns=[{"name": "content", "data_type": "text"}])
    def some_data() -> Generator[DictStrStr, Any, None]:
        yield from next(generator_instance1)

    assert some_data.columns["content"] == {"name": "content", "data_type": "text"}  # type: ignore[index]

    lancedb_adapter(
        some_data,
        embed=["content"],
    )

    assert some_data.columns["content"] == {  # type: ignore
        "name": "content",
        "data_type": "text",
        "x-lancedb-embed": True,
    }


def test_basic_state_and_schema() -> None:
    generator_instance1 = sequence_generator()

    @dlt.resource
    def some_data() -> Generator[DictStrStr, Any, None]:
        yield from next(generator_instance1)

    lancedb_adapter(
        some_data,
        embed=["content"],
    )

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_append",
        destination="lancedb",
        dataset_name=f"test_pipeline_append_dataset{uniq_id()}",
    )
    info = pipeline.run(
        some_data(),
    )
    assert_load_info(info)

    client: LanceDBClient
    with pipeline.destination_client() as client:  # type: ignore
        # Check if we can get a stored schema and state.
        schema = client.get_stored_schema(client.schema.name)
        print("Print dataset name", client.dataset_name)
        assert schema
        state = client.get_stored_state("test_pipeline_append")
        assert state


def test_pipeline_append() -> None:
    generator_instance1 = sequence_generator()
    generator_instance2 = sequence_generator()

    @dlt.resource
    def some_data() -> Generator[DictStrStr, Any, None]:
        yield from next(generator_instance1)

    lancedb_adapter(
        some_data,
        embed=["content"],
    )

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_append",
        destination="lancedb",
        dataset_name=f"TestPipelineAppendDataset{uniq_id()}",
    )
    info = pipeline.run(
        some_data(),
    )
    assert_load_info(info)

    data = next(generator_instance2)
    assert_table(pipeline, "some_data", items=data)

    info = pipeline.run(
        some_data(),
    )
    assert_load_info(info)

    data.extend(next(generator_instance2))
    assert_table(pipeline, "some_data", items=data)


def test_explicit_append() -> None:
    """Append should work even when the primary key is specified."""
    data = [
        {"doc_id": 1, "content": "1"},
        {"doc_id": 2, "content": "2"},
        {"doc_id": 3, "content": "3"},
    ]

    @dlt.resource(primary_key="doc_id")
    def some_data() -> Generator[List[DictStrAny], Any, None]:
        yield data

    lancedb_adapter(
        some_data,
        embed=["content"],
    )

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_append",
        destination="lancedb",
        dataset_name=f"TestPipelineAppendDataset{uniq_id()}",
    )
    info = pipeline.run(
        some_data(),
    )

    assert_table(pipeline, "some_data", items=data)

    info = pipeline.run(
        some_data(),
        write_disposition="append",
    )
    assert_load_info(info)

    data.extend(data)
    assert_table(pipeline, "some_data", items=data)


def test_pipeline_replace() -> None:
    generator_instance1 = sequence_generator()
    generator_instance2 = sequence_generator()

    @dlt.resource
    def some_data() -> Generator[DictStrStr, Any, None]:
        yield from next(generator_instance1)

    lancedb_adapter(
        some_data,
        embed=["content"],
    )

    uid = uniq_id()

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_replace",
        destination="lancedb",
        dataset_name="test_pipeline_replace_dataset"
        + uid,  # lancedb doesn't mandate any name normalization
    )

    info = pipeline.run(
        some_data(),
        write_disposition="replace",
    )
    assert_load_info(info)
    assert info.dataset_name == f"test_pipeline_replace_dataset{uid}"

    data = next(generator_instance2)
    assert_table(pipeline, "some_data", items=data)

    info = pipeline.run(
        some_data(),
        write_disposition="replace",
    )
    assert_load_info(info)

    data = next(generator_instance2)
    assert_table(pipeline, "some_data", items=data)


def test_pipeline_merge() -> None:
    data = [
        {
            "doc_id": 1,
            "merge_id": "shawshank-redemption-1994",
            "title": "The Shawshank Redemption",
            "description": (
                "Two imprisoned men find redemption through acts of decency over the years."
            ),
        },
        {
            "doc_id": 2,
            "merge_id": "the-godfather-1972",
            "title": "The Godfather",
            "description": (
                "A crime dynasty's aging patriarch transfers control to his reluctant son."
            ),
        },
        {
            "doc_id": 3,
            "merge_id": "the-dark-knight-2008",
            "title": "The Dark Knight",
            "description": (
                "The Joker wreaks havoc on Gotham, challenging The Dark Knight's ability to fight"
                " injustice."
            ),
        },
        {
            "doc_id": 4,
            "merge_id": "pulp-fiction-1994",
            "title": "Pulp Fiction",
            "description": (
                "The lives of two mob hitmen, a boxer, a gangster and his wife, and a pair of diner"
                " bandits intertwine in four tales of violence and redemption."
            ),
        },
        {
            "doc_id": 5,
            "merge_id": "schindlers-list-1993",
            "title": "Schindler's List",
            "description": (
                "In German-occupied Poland during World War II, industrialist Oskar Schindler"
                " gradually becomes concerned for his Jewish workforce after witnessing their"
                " persecution by the Nazis."
            ),
        },
        {
            "doc_id": 6,
            "merge_id": "the-lord-of-the-rings-the-return-of-the-king-2003",
            "title": "The Lord of the Rings: The Return of the King",
            "description": (
                "Gandalf and Aragorn lead the World of Men against Sauron's army to draw his gaze"
                " from Frodo and Sam as they approach Mount Doom with the One Ring."
            ),
        },
        {
            "doc_id": 7,
            "merge_id": "the-matrix-1999",
            "title": "The Matrix",
            "description": (
                "A computer hacker learns from mysterious rebels about the true nature of his"
                " reality and his role in the war against its controllers."
            ),
        },
    ]

    @dlt.resource(primary_key="doc_id")
    def movies_data() -> Any:
        yield data

    @dlt.resource(primary_key="doc_id", merge_key=["merge_id", "title"])
    def movies_data_explicit_merge_keys() -> Any:
        yield data

    lancedb_adapter(
        movies_data,
        embed=["description"],
    )

    lancedb_adapter(
        movies_data_explicit_merge_keys,
        embed=["description"],
    )

    pipeline = dlt.pipeline(
        pipeline_name="movies",
        destination="lancedb",
        dataset_name=f"TestPipelineAppendDataset{uniq_id()}",
    )
    info = pipeline.run(
        movies_data(),
        write_disposition="merge",
        dataset_name=f"MoviesDataset{uniq_id()}",
    )
    assert_load_info(info)
    assert_table(pipeline, "movies_data", items=data)

    # Change some data.
    data[0]["title"] = "The Shawshank Redemption 2"

    info = pipeline.run(
        movies_data(),
        write_disposition="merge",
    )
    assert_load_info(info)
    assert_table(pipeline, "movies_data", items=data)

    info = pipeline.run(
        movies_data(),
        write_disposition="merge",
    )
    assert_load_info(info)
    assert_table(pipeline, "movies_data", items=data)

    # Test with explicit merge keys.
    info = pipeline.run(
        movies_data_explicit_merge_keys(),
        write_disposition="merge",
    )
    assert_load_info(info)
    assert_table(pipeline, "movies_data_explicit_merge_keys", items=data)


def test_pipeline_with_schema_evolution() -> None:
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
    def some_data() -> Generator[List[DictStrAny], Any, None]:
        yield data

    lancedb_adapter(some_data, embed=["content"])

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_append",
        destination="lancedb",
        dataset_name=f"TestSchemaEvolutionDataset{uniq_id()}",
    )
    pipeline.run(
        some_data(),
    )

    assert_table(pipeline, "some_data", items=data)

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

    assert_table(pipeline, "some_data", items=aggregated_data)


def test_merge_github_nested() -> None:
    pipe = dlt.pipeline(destination="lancedb", dataset_name="github1", dev_mode=True)
    assert pipe.dataset_name.startswith("github1_202")

    with open(
        "tests/normalize/cases/github.issues.load_page_5_duck.json",
        "r",
        encoding="utf-8",
    ) as f:
        data = json.load(f)

    info = pipe.run(
        lancedb_adapter(data[:17], embed=["title", "body"]),
        table_name="issues",
        write_disposition="merge",
        primary_key="id",
    )
    assert_load_info(info)
    # assert if schema contains tables with right names
    print(pipe.default_schema.tables.keys())
    assert set(pipe.default_schema.tables.keys()) == {
        "_dlt_version",
        "_dlt_loads",
        "issues",
        "_dlt_pipeline_state",
        "issues__labels",
        "issues__assignees",
    }
    assert {t["name"] for t in pipe.default_schema.data_tables()} == {
        "issues",
        "issues__labels",
        "issues__assignees",
    }
    assert {t["name"] for t in pipe.default_schema.dlt_tables()} == {
        "_dlt_version",
        "_dlt_loads",
        "_dlt_pipeline_state",
    }
    issues = pipe.default_schema.tables["issues"]
    assert issues["columns"]["id"]["primary_key"] is True
    # Make sure vectorization is enabled for.
    assert issues["columns"]["title"][VECTORIZE_HINT]  # type: ignore[literal-required]
    assert issues["columns"]["body"][VECTORIZE_HINT]  # type: ignore[literal-required]
    assert VECTORIZE_HINT not in issues["columns"]["url"]
    assert_table(pipe, "issues", expected_items_count=17)


def test_empty_dataset_allowed() -> None:
    # dataset_name is optional so dataset name won't be autogenerated when not explicitly passed.
    pipe = dlt.pipeline(destination="lancedb", dev_mode=True)
    client: LanceDBClient = pipe.destination_client()  # type: ignore[assignment]

    assert pipe.dataset_name is None
    info = pipe.run(lancedb_adapter(["context", "created", "not a stop word"], embed=["value"]))
    # Dataset in load info is empty.
    assert info.dataset_name is None
    client = pipe.destination_client()  # type: ignore[assignment]
    assert client.dataset_name is None
    assert client.sentinel_table == "dltSentinelTable"
    assert_table(pipe, "content", expected_items_count=3)


search_data = [
    {"text": "Frodo was a happy puppy"},
    {"text": "There are several kittens playing"},
]


def test_fts_query() -> None:
    @dlt.resource
    def search_data_resource() -> Generator[Mapping[str, object], Any, None]:
        yield from search_data

    pipeline = dlt.pipeline(
        pipeline_name="test_fts_query",
        destination="lancedb",
        dataset_name=f"test_pipeline_append{uniq_id()}",
    )
    info = pipeline.run(
        search_data_resource(),
    )
    assert_load_info(info)

    client: LanceDBClient
    with pipeline.destination_client() as client:  # type: ignore[assignment]
        db_client: DBConnection = client.db_client

        table_name = client.make_qualified_table_name("search_data_resource")
        tbl = db_client[table_name]
        tbl.checkout_latest()

        tbl.create_fts_index("text")
        results = tbl.search("kittens", query_type="fts").select(["text"]).to_list()
        assert results[0]["text"] == "There are several kittens playing"


def test_semantic_query() -> None:
    @dlt.resource
    def search_data_resource() -> Generator[Mapping[str, object], Any, None]:
        yield from search_data

    lancedb_adapter(
        search_data_resource,
        embed=["text"],
    )

    pipeline = dlt.pipeline(
        pipeline_name="test_fts_query",
        destination="lancedb",
        dataset_name=f"test_pipeline_append{uniq_id()}",
    )
    info = pipeline.run(
        search_data_resource(),
    )
    assert_load_info(info)

    client: LanceDBClient
    with pipeline.destination_client() as client:  # type: ignore[assignment]
        db_client: DBConnection = client.db_client

        table_name = client.make_qualified_table_name("search_data_resource")
        tbl = db_client[table_name]
        tbl.checkout_latest()

        results = (
            tbl.search("puppy", query_type="vector", ordering_field_name="_distance")
            .select(["text"])
            .to_list()
        )
        assert results[0]["text"] == "Frodo was a happy puppy"


def test_semantic_query_custom_embedding_functions_registered() -> None:
    """Test the LanceDB registry registered custom embedding functions defined in models, if any.
    See: https://github.com/dlt-hub/dlt/issues/1765"""

    @dlt.resource
    def search_data_resource() -> Generator[Mapping[str, object], Any, None]:
        yield from search_data

    lancedb_adapter(
        search_data_resource,
        embed=["text"],
    )

    pipeline = dlt.pipeline(
        pipeline_name="test_fts_query",
        destination="lancedb",
        dataset_name=f"test_pipeline_append{uniq_id()}",
    )
    info = pipeline.run(
        search_data_resource(),
    )
    assert_load_info(info)

    client: LanceDBClient
    with pipeline.destination_client() as client:  # type: ignore[assignment]
        db_client_uri = client.db_client.uri
        table_name = client.make_qualified_table_name("search_data_resource")

    # A new python process doesn't seem to correctly deserialize the custom embedding
    # functions into global __REGISTRY__.
    # We make sure to reset it as well to make sure no globals are propagated to the spawned process.
    EmbeddingFunctionRegistry().reset()
    with multiprocessing.get_context("spawn").Pool(1) as pool:
        results = pool.apply(run_lance_search_in_separate_process, (db_client_uri, table_name))

    assert results[0]["text"] == "Frodo was a happy puppy"


def run_lance_search_in_separate_process(db_client_uri: str, table_name: str) -> Any:
    import lancedb

    # Must read into __REGISTRY__ here.
    db = lancedb.connect(db_client_uri)
    tbl = db[table_name]
    tbl.checkout_latest()

    return (
        tbl.search("puppy", query_type="vector", ordering_field_name="_distance")
        .select(["text"])
        .to_list()
    )
