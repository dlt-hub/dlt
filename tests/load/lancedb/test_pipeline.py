import multiprocessing
import os
from typing import TYPE_CHECKING, Generator, Any, List, cast
from typing import Mapping
from typing import Union, Dict

import pytest
from lancedb.embeddings import EmbeddingFunctionRegistry
import pyarrow as pa
import numpy as np

import dlt
from dlt.common import json
from dlt.common.typing import DictStrAny
from dlt.common.typing import DictStrStr
from dlt.common.utils import uniq_id, digest128
from dlt.destinations.impl.lancedb.lancedb_adapter import lancedb_adapter, VECTORIZE_HINT
from dlt.extract import DltResource
from tests.load.lancedb.utils import (
    assert_table,
    chunk_document,
    mock_embed,
    destination_config,
    get_table_location,
    open_lance_table,
)
from tests.load.utils import DestinationTestConfiguration
from tests.load.utils import sequence_generator
from tests.pipeline.utils import assert_load_info
from tests.utils import get_test_storage_root

if TYPE_CHECKING:
    from tests.load.lancedb.utils import TLanceDestinationClient
else:
    TLanceDestinationClient = Any

# Mark all tests as essential, don't remove.
pytestmark = pytest.mark.essential


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
        "nullable": True,  # lancedb will override nullability
    }

    lancedb_adapter(
        some_data,
        merge_key="content",
    )

    # via merge_key
    assert some_data._hints["merge_key"] == "content"

    assert some_data.columns["content"] == {  # type: ignore
        "name": "content",
        "data_type": "text",
        "x-lancedb-embed": True,
        "nullable": True,  # lancedb will override nullability
    }

    assert some_data.compute_table_schema()["columns"]["content"]["merge_key"] is True


def test_changing_merge_key(destination_config: DestinationTestConfiguration) -> None:
    @dlt.resource
    def some_data():
        yield {"id": 1, "other_id": 2, "content": "random"}

    # Initially "id" is set as key
    lancedb_adapter(
        some_data,
        embed=["random"],
        merge_key="id",
    )

    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_changing_merge_key",
        dev_mode=True,
    )
    info = pipeline.run(
        some_data(),
    )
    assert_load_info(info)

    assert pipeline.default_schema.tables["some_data"]["columns"]["id"]["merge_key"] is True
    assert pipeline.default_schema.tables["some_data"]["columns"]["id"]["nullable"] is False

    # We change key to "other_id"
    lancedb_adapter(
        some_data,
        embed=["random"],
        merge_key="other_id",
    )
    info = pipeline.run(
        some_data(),
    )
    assert_load_info(info)

    # "id" should no longer be key
    assert not pipeline.default_schema.tables["some_data"]["columns"]["id"].get("merge_key")
    assert pipeline.default_schema.tables["some_data"]["columns"]["id"]["nullable"] is False
    assert (
        pipeline.default_schema.tables["some_data"]["columns"]["other_id"].get("merge_key") is True
    )
    assert pipeline.default_schema.tables["some_data"]["columns"]["other_id"]["nullable"] is False


def test_basic_state_and_schema(destination_config: DestinationTestConfiguration) -> None:
    generator_instance1 = sequence_generator()

    @dlt.resource
    def some_data() -> Generator[DictStrStr, Any, None]:
        yield from next(generator_instance1)

    lancedb_adapter(
        some_data,
        embed=["content"],
    )

    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_basic_state_and_schema",
        dev_mode=True,
    )
    info = pipeline.run(
        some_data(),
    )
    assert_load_info(info)

    with pipeline.destination_client() as client:
        client = cast(TLanceDestinationClient, client)
        # Check if we can get a stored schema and state.
        schema = client.get_stored_schema(client.schema.name)
        print("Print dataset name", client.dataset_name)
        assert schema
        state = client.get_stored_state("test_basic_state_and_schema")
        assert state


def test_pipeline_append(destination_config: DestinationTestConfiguration) -> None:
    generator_instance1 = sequence_generator()
    generator_instance2 = sequence_generator()

    @dlt.resource
    def some_data() -> Generator[DictStrStr, Any, None]:
        yield from next(generator_instance1)

    lancedb_adapter(
        some_data,
        embed=["content"],
    )

    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_pipeline_append",
        dev_mode=True,
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


def test_explicit_append(destination_config: DestinationTestConfiguration) -> None:
    data = [
        {"doc_id": 1, "content": "1"},
        {"doc_id": 2, "content": "2"},
        {"doc_id": 3, "content": "3"},
    ]

    @dlt.resource()
    def some_data() -> Generator[List[DictStrAny], Any, None]:
        yield data

    lancedb_adapter(
        some_data,
        embed=["content"],
    )

    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_explicit_append",
        dev_mode=True,
    )
    info = pipeline.run(
        some_data(),
    )
    assert_load_info(info)

    assert_table(pipeline, "some_data", items=data)

    info = pipeline.run(
        some_data(),
        write_disposition="append",
    )
    assert_load_info(info)

    data.extend(data)
    assert_table(pipeline, "some_data", items=data)


def test_pipeline_replace(destination_config: DestinationTestConfiguration) -> None:
    os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "2"
    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "2"

    generator_instance1, generator_instance2 = (sequence_generator(), sequence_generator())

    @dlt.resource
    def some_data() -> Generator[DictStrStr, Any, None]:
        yield from next(generator_instance1)

    uid = uniq_id()

    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_pipeline_replace",
        dataset_name="test_pipeline_replace_dataset"
        + uid,  # Lancedb doesn't mandate any name normalization.
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


def test_pipeline_merge(destination_config: DestinationTestConfiguration) -> None:
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

    @dlt.resource(primary_key=["doc_id"])
    def movies_data() -> Any:
        yield data

    lancedb_adapter(movies_data, embed=["description"], no_remove_orphans=True)

    pipeline = destination_config.setup_pipeline(
        pipeline_name="movies",
        dev_mode=True,
    )
    info = pipeline.run(
        movies_data(),
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        dataset_name=f"MoviesDataset{uniq_id()}",
    )
    assert_load_info(info)
    assert_table(pipeline, "movies_data", items=data)

    # Change some data.
    data[0]["title"] = "The Shawshank Redemption 2"

    info = pipeline.run(
        movies_data(),
        write_disposition={"disposition": "merge", "strategy": "upsert"},
    )
    assert_load_info(info)
    assert_table(pipeline, "movies_data", items=data)


def test_pipeline_with_schema_evolution(destination_config: DestinationTestConfiguration) -> None:
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

    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_pipeline_with_schema_evolution",
        dev_mode=True,
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


@pytest.mark.parametrize("lance_location", (":external:", ":pipeline:", "default"))
def test_merge_github_nested(
    lance_location: str, destination_config: DestinationTestConfiguration
) -> None:
    if lance_location == ":pipeline:":
        destination_ = getattr(dlt.destinations, destination_config.destination_type)(
            lance_uri=lance_location
        )
    elif lance_location == ":external:":
        import lancedb

        path = os.path.join(get_test_storage_root(), "test.lancedb")
        destination_ = getattr(dlt.destinations, destination_config.destination_type)(
            credentials=lancedb.connect(path)
        )
    else:
        destination_ = destination_config.destination_type

    pipe = dlt.pipeline(destination=destination_, dataset_name="github1", dev_mode=True)
    assert pipe.dataset_name.startswith("github1_202")

    with open(
        "tests/normalize/cases/github.issues.load_page_5_duck.json",
        "r",
        encoding="utf-8",
    ) as f:
        data = json.load(f)

    info = pipe.run(
        lancedb_adapter(data[:17], embed=["title", "body"], no_remove_orphans=True),
        table_name="issues",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
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


def test_bring_your_own_vector(destination_config: DestinationTestConfiguration) -> None:
    """Test pipeline with explicitly provided vector data in an arrow table."""

    # TODO: support Python objects - requires serializing arrow types (get_nested_column_type_from_py_arrow)

    os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID"] = "TRUE"

    num_rows = 5
    vector_dim = 10

    # vector column must be of fixed size! other nested types are not implemented
    vectors = [np.random.rand(vector_dim).tolist() for _ in range(num_rows)]
    table = pa.table(
        {
            "id": pa.array(list(range(1, num_rows + 1)), pa.int32()),
            "text": pa.array([f"Sample text {i}" for i in range(1, num_rows + 1)]),
            "vector": pa.array(
                vectors, pa.list_(pa.float32(), vector_dim)
            ),  # This is the format LanceDB expects
        }
    )

    @dlt.resource(
        table_name="vector_data",
        primary_key="id",
        write_disposition="merge",
    )
    def identity_resource(data: pa.Table) -> Generator[pa.Table, None, None]:
        yield data

    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_bring_your_own_vector",
        dev_mode=True,
    )

    info = pipeline.run(identity_resource(table))
    assert_load_info(info)

    # run it twice to see how merge behaves
    vectors = [np.random.rand(vector_dim).tolist() for _ in range(num_rows)]
    table = pa.table(
        {
            "id": pa.array(list(range(1, num_rows + 1)), pa.int32()),
            "text": pa.array([f"Sample text {i}" for i in range(1, num_rows + 1)]),
            "vector": pa.array(
                vectors, pa.list_(pa.float32(), vector_dim)
            ),  # This is the format LanceDB expects
        }
    )
    info = pipeline.run(identity_resource(table))
    assert_load_info(info)

    # Verify the data was loaded correctly
    with pipeline.destination_client() as client:
        client = cast(TLanceDestinationClient, client)
        tbl = open_lance_table(client, "vector_data")
        tbl.create_scalar_index("id")

        # Check that the vector column exists and has the right dimensions
        assert "vector" in tbl.schema.names

        # Check we can do vector search with the provided vectors
        results = tbl.search(
            query=vectors[0],  # Use the first vector as query
            vector_column_name="vector",
            query_type="vector",
        ).to_pandas()

        # The first result should be the first item (exact match)
        assert results.iloc[0]["id"] == 1

        # Check that all rows were loaded
        assert len(tbl.to_pandas()) == num_rows


def test_empty_dataset_allowed(destination_config: DestinationTestConfiguration) -> None:
    if destination_config.destination_type == "lance":
        pytest.skip("lance destination does not allow empty datasets")

    # dataset_name is optional so dataset name won't be autogenerated when not explicitly passed.
    pipe = dlt.pipeline(destination=destination_config.destination_type, dev_mode=True)

    assert pipe.dataset_name is None
    info = pipe.run(lancedb_adapter(["context", "created", "not a stop word"], embed=["value"]))
    # Dataset in load info is empty.
    assert info.dataset_name is None
    client = pipe.destination_client()
    assert client.dataset_name is None  # type: ignore
    if destination_config.destination_type == "lancedb":
        assert client.sentinel_table == "dltSentinelTable"  # type: ignore
    assert_table(pipe, "content", expected_items_count=3)

    dataset = pipe.dataset()
    rows = dataset.content.select("value").fetchall()
    assert len(rows) == 3


def test_lancedb_remove_nested_orphaned_records_with_chunks(
    destination_config: DestinationTestConfiguration,
) -> None:
    @dlt.resource(
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        table_name="document",
        primary_key=["doc_id"],
        merge_key=["doc_id"],
    )
    def documents(docs: List[DictStrAny]) -> Generator[DictStrAny, None, None]:
        for doc in docs:
            doc_id = doc["doc_id"]
            chunks = chunk_document(doc["text"])
            embeddings = [
                {
                    "chunk_hash": digest128(chunk),
                    "chunk_text": chunk,
                    "embedding": mock_embed(),
                }
                for chunk in chunks
            ]
            yield {"doc_id": doc_id, "doc_text": doc["text"], "embeddings": embeddings}

    @dlt.source(max_table_nesting=1)
    def documents_source(
        docs: List[DictStrAny],
    ) -> Union[Generator[Dict[str, Any], None, None], DltResource]:
        return documents(docs)

    pipeline = destination_config.setup_pipeline(
        pipeline_name="chunked_docs",
        dev_mode=True,
    )

    initial_docs = [
        {
            "text": (
                "This is the first document. It contains some text that will be chunked and"
                " embedded. (I don't want to be seen in updated run's embedding chunk texts btw)"
            ),
            "doc_id": 1,
        },
        {
            "text": "Here's another document. It's a bit different from the first one.",
            "doc_id": 2,
        },
    ]

    info = pipeline.run(documents_source(initial_docs))
    assert_load_info(info)

    updated_docs = [
        {
            "text": "This is the first document, but it has been updated with new content.",
            "doc_id": 1,
        },
        {
            "text": "This is a completely new document that wasn't in the initial set.",
            "doc_id": 3,
        },
    ]

    info = pipeline.run(documents_source(updated_docs))
    assert_load_info(info)

    with pipeline.destination_client() as client:
        client = cast(TLanceDestinationClient, client)
        # Orphaned chunks/documents must have been discarded.
        # Shouldn't contain any text from `initial_docs' where doc_id=1.
        expected_text = {
            "Here's ano",
            "ther docum",
            "ent. It's ",
            "a bit diff",
            "erent from",
            " the first",
            " one.",
            "This is th",
            "e first do",
            "cument, bu",
            "t it has b",
            "een update",
            "d with new",
            " content.",
            "This is a ",
            "completely",
            " new docum",
            "ent that w",
            "asn't in t",
            "he initial",
            " set.",
        }

        tbl = open_lance_table(client, "document__embeddings")
        df = tbl.to_pandas()
        assert set(df["chunk_text"]) == expected_text


search_data = [
    {"text": "Frodo was a happy puppy"},
    {"text": "There are several kittens playing"},
]


def test_fts_query(destination_config: DestinationTestConfiguration) -> None:
    @dlt.resource
    def search_data_resource() -> Generator[Mapping[str, object], Any, None]:
        yield from search_data

    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_fts_query",
        dev_mode=True,
    )
    info = pipeline.run(
        search_data_resource(),
    )
    assert_load_info(info)

    with pipeline.destination_client() as client:
        client = cast(TLanceDestinationClient, client)
        tbl = open_lance_table(client, "search_data_resource")

        tbl.create_fts_index("text")
        results = tbl.search("kittens", query_type="fts").select(["text"]).to_list()
        assert results[0]["text"] == "There are several kittens playing"


def test_semantic_query(destination_config: DestinationTestConfiguration) -> None:
    @dlt.resource
    def search_data_resource() -> Generator[Mapping[str, object], Any, None]:
        yield from search_data

    lancedb_adapter(
        search_data_resource,
        embed=["text"],
    )

    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_semantic_query",
        dev_mode=True,
    )
    info = pipeline.run(
        search_data_resource(),
    )
    assert_load_info(info)

    with pipeline.destination_client() as client:
        client = cast(TLanceDestinationClient, client)
        tbl = open_lance_table(client, "search_data_resource")

        results = (
            tbl.search("puppy", query_type="vector", ordering_field_name="_distance")
            .select(["text"])
            .to_list()
        )
        assert results[0]["text"] == "Frodo was a happy puppy"


def test_semantic_query_custom_embedding_functions_registered(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Test the LanceDB registry registered custom embedding functions defined in models, if any.
    See: https://github.com/dlt-hub/dlt/issues/1765"""

    @dlt.resource
    def search_data_resource() -> Generator[Mapping[str, object], Any, None]:
        yield from search_data

    lancedb_adapter(
        search_data_resource,
        embed=["text"],
    )

    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_semantic_query_custom_embedding_functions_registered",
        dev_mode=True,
    )
    info = pipeline.run(
        search_data_resource(),
    )
    assert_load_info(info)

    with pipeline.destination_client() as client:
        client = cast(TLanceDestinationClient, client)
        lance_uri = client.config.lance_uri
        table_location = get_table_location(client, "search_data_resource")

    # A new python process doesn't seem to correctly deserialize the custom embedding
    # functions into global __REGISTRY__.
    # We make sure to reset it as well to make sure no globals are propagated to the spawned process.
    EmbeddingFunctionRegistry().reset()
    with multiprocessing.get_context("spawn").Pool(1) as pool:
        results = pool.apply(
            run_lance_search_in_separate_process,
            (lance_uri, "search_data_resource", table_location),
        )

    assert results[0]["text"] == "Frodo was a happy puppy"


def run_lance_search_in_separate_process(
    lance_uri: str, table_name: str, table_location: str
) -> Any:
    import lancedb
    from lancedb.table import LanceTable

    # Must read into __REGISTRY__ here.
    db = lancedb.connect(lance_uri)
    tbl = LanceTable.open(db, table_name, location=table_location)

    return (
        tbl.search("puppy", query_type="vector", ordering_field_name="_distance")
        .select(["text"])
        .to_list()
    )
