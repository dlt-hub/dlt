import os
import pytest
from typing import Iterator

import dlt
from dlt.common import json
from dlt.common.schema.exceptions import (
    SchemaCorruptedException,
    SchemaIdentifierNormalizationCollision,
)
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.utils import uniq_id

from dlt.destinations import weaviate
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
    batch_1 = [{"content": "alpha"}, {"content": "beta"}, {"content": "gamma"}]
    batch_2 = [{"content": "delta"}, {"content": "epsilon"}]

    @dlt.resource
    def some_data():
        yield data

    weaviate_adapter(some_data, vectorize=["content"])

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_replace",
        destination="weaviate",
        dataset_name="test_pipeline_replace_dataset" + uniq_id(),
    )

    data = batch_1
    info = pipeline.run(some_data(), write_disposition="replace")
    assert_load_info(info)
    assert_class(pipeline, "SomeData", expected_items_count=3, items=batch_1)

    # replace with a different batch — old data must be gone
    data = batch_2
    info = pipeline.run(some_data(), write_disposition="replace")
    assert_load_info(info)
    assert_class(pipeline, "SomeData", expected_items_count=2, items=batch_2)


def test_pipeline_replace_nested() -> None:
    """Replace should truncate both root and child collections."""

    @dlt.resource
    def issues():
        yield data

    weaviate_adapter(issues, vectorize=["title"])

    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_replace_nested",
        destination="weaviate",
        dataset_name="test_replace_nested_" + uniq_id(),
    )

    data = [
        {"id": 1, "title": "first", "labels": [{"name": "bug"}, {"name": "fix"}]},
        {"id": 2, "title": "second", "labels": [{"name": "feature"}]},
    ]
    info = pipeline.run(issues(), write_disposition="replace")
    assert_load_info(info)
    assert_class(pipeline, "Issues", expected_items_count=2)
    assert_class(pipeline, "Issues__Labels", expected_items_count=3)

    # replace with fewer items — old root and child data must be gone
    data = [
        {"id": 3, "title": "third", "labels": [{"name": "docs"}]},
    ]
    info = pipeline.run(issues(), write_disposition="replace")
    assert_load_info(info)
    assert_class(pipeline, "Issues", expected_items_count=1)
    assert_class(pipeline, "Issues__Labels", expected_items_count=1)


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

    # Weaviate fixes a vector's source properties when the collection is created and offers no
    # way to extend them, so `new_vec_column` is not vectorized even though it carries the hint
    assert_class(
        pipeline,
        "SomeData",
        items=aggregated_data,
        vectorized_columns=["content"] if vectorized else [],
    )


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
    assert_class(p, "Issues__Labels", expected_items_count=7)
    assert_class(p, "Issues__Assignees", expected_items_count=10)

    # second load: update issue 388089021 (drop one label, keep one) and add a new issue
    updated_issue = data[0].copy()
    updated_issue["labels"] = [data[0]["labels"][0]]  # keep only first label (was 2)
    updated_issue["assignees"] = []
    new_issue = {
        "id": 999999999,
        "title": "New issue",
        "body": "New body",
        "url": "https://example.com",
        "labels": [{"id": 1, "node_id": "x", "url": "x", "name": "bug", "color": "f00"}],
        "assignees": [],
    }
    info = p.run(
        weaviate_adapter(
            [updated_issue, new_issue],
            vectorize=["title", "body"],
            tokenization={"user__login": "lowercase"},
        ),
        table_name="issues",
        write_disposition="merge",
        primary_key="id",
    )
    assert_load_info(info)
    # 17 original + 1 new = 18 (updated issue deduped)
    assert_class(p, "Issues", expected_items_count=18)
    # labels: 7 original + 1 new issue label = 8
    # kept label (index 0) dedupes via deterministic _dlt_id
    # removed label (index 1) stays — upsert does not delete orphaned children
    assert_class(p, "Issues__Labels", expected_items_count=8)


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


def test_pipeline_with_precomputed_vectors() -> None:
    """Vectors supplied by the user are stored as object vectors, not as properties."""

    @dlt.resource(name="embeddings", primary_key="doc_id", write_disposition="merge")
    def embeddings(vector: float):
        yield {"doc_id": 1, "title": "first", "emb": [vector, 0.2, 0.3]}
        yield {"doc_id": 2, "title": "second", "emb": [0.9, 0.8, 0.7]}

    p = dlt.pipeline(
        pipeline_name="weaviate_byo_vectors_" + uniq_id(),
        destination="weaviate",
        dataset_name="TestByoVectors" + uniq_id(),
        dev_mode=True,
    )
    info = p.run(weaviate_adapter(embeddings(0.1), vector="emb"))
    assert_load_info(info)

    # the vector column is not unnested into a child table
    assert "Embeddings__emb" not in p.default_schema.tables

    client: WeaviateClient
    with p.destination_client() as client:  # type: ignore[assignment]
        collection = client.db_client.collections.get(
            client.make_qualified_collection_name("Embeddings")
        )
        # the vector column is not stored as a property
        assert "emb" not in {prop.name for prop in collection.config.get().properties}

        objects = collection.query.fetch_objects(include_vector=True, limit=10).objects
        vectors = {int(o.properties["doc_id"]): o.vector["default"] for o in objects}  # type: ignore[arg-type]
        assert vectors[1] == pytest.approx([0.1, 0.2, 0.3])
        assert vectors[2] == pytest.approx([0.9, 0.8, 0.7])

    # merging the same primary key replaces the vector
    info = p.run(weaviate_adapter(embeddings(0.5), vector="emb"))
    assert_load_info(info)

    with p.destination_client() as client:  # type: ignore[assignment]
        collection = client.db_client.collections.get(
            client.make_qualified_collection_name("Embeddings")
        )
        objects = collection.query.fetch_objects(include_vector=True, limit=10).objects
        assert len(objects) == 2
        vectors = {int(o.properties["doc_id"]): o.vector["default"] for o in objects}  # type: ignore[arg-type]
        assert vectors[1] == pytest.approx([0.5, 0.2, 0.3])


def test_adapter_rejects_vector_column_that_is_also_vectorized() -> None:
    with pytest.raises(ValueError):
        weaviate_adapter([{"emb": [0.1]}], vectorize="emb", vector="emb")


def test_adapter_requires_at_least_one_hint() -> None:
    with pytest.raises(ValueError):
        weaviate_adapter([{"emb": [0.1]}])


def test_pipeline_with_named_vectors() -> None:
    """Each named vector becomes its own vector on the collection, built from its own columns."""

    @dlt.resource(name="articles", primary_key="id")
    def articles():
        yield {"id": 1, "title": "weaviate vectors", "body": "the body text goes here"}

    p = dlt.pipeline(
        pipeline_name="weaviate_named_vectors_" + uniq_id(),
        destination="weaviate",
        dataset_name="TestNamedVectors" + uniq_id(),
        dev_mode=True,
    )
    info = p.run(
        weaviate_adapter(
            articles(),
            named_vectors={
                "title_vec": {"vectorize": ["title"]},
                "body_vec": {"vectorize": ["body"]},
            },
        )
    )
    assert_load_info(info)

    client: WeaviateClient
    with p.destination_client() as client:  # type: ignore[assignment]
        collection = client.db_client.collections.get(
            client.make_qualified_collection_name("Articles")
        )
        vector_config = collection.config.get().vector_config
        assert set(vector_config) == {"title_vec", "body_vec"}

        if client.config.vectorizer == "none":
            # without a vectorizer module each named vector carries user supplied vectors
            assert all(v.vectorizer.vectorizer == "none" for v in vector_config.values())
        else:
            assert vector_config["title_vec"].vectorizer.source_properties == ["title"]
            assert vector_config["body_vec"].vectorizer.source_properties == ["body"]


def test_named_vectors_require_source_columns() -> None:
    with pytest.raises(ValueError):
        weaviate_adapter([{"title": "x"}], named_vectors={"title_vec": {"vectorize": []}})


def test_pipeline_multi_tenancy_isolates_tenants() -> None:
    """Rows loaded under one tenant are invisible to another."""
    dataset_name = "TestTenants" + uniq_id()

    def load(tenant: str, rows) -> dlt.Pipeline:
        pipeline = dlt.pipeline(
            pipeline_name=f"weaviate_tenant_{tenant}_" + uniq_id(),
            destination=weaviate(multi_tenancy=True, tenant=tenant),
            dataset_name=dataset_name,
        )
        assert_load_info(
            pipeline.run(rows, table_name="items", primary_key="id", write_disposition="merge")
        )
        return pipeline

    p_a = load("tenanta", [{"id": 1, "v": "a1"}, {"id": 2, "v": "a2"}])
    load("tenantb", [{"id": 1, "v": "b1"}])

    client: WeaviateClient
    with p_a.destination_client() as client:  # type: ignore[assignment]
        items = client.db_client.collections.get(client.make_qualified_collection_name("Items"))
        assert items.config.get().multi_tenancy_config.enabled is True

        # dlt bookkeeping stays shared, it is already keyed by pipeline name
        state = client.db_client.collections.get(
            client.make_qualified_collection_name("DltPipelineState")
        )
        assert state.config.get().multi_tenancy_config.enabled is False

        by_tenant = {
            tenant: sorted(
                str(o.properties["v"])
                for o in items.with_tenant(tenant).query.fetch_objects(limit=10).objects
            )
            for tenant in ("tenanta", "tenantb")
        }
        assert by_tenant == {"tenanta": ["a1", "a2"], "tenantb": ["b1"]}


def test_pipeline_with_separate_grpc_host() -> None:
    """A custom connection can reach REST and gRPC on different hosts."""
    p = dlt.pipeline(
        pipeline_name="weaviate_grpc_host_" + uniq_id(),
        destination=weaviate(
            credentials={
                "url": "http://127.0.0.1",
                "http_port": 8080,
                "grpc_port": 50051,
                "grpc_host": "localhost",
            },
            connection_type="custom",
            skip_init_checks=False,
        ),
        dataset_name="TestGrpcHost" + uniq_id(),
        dev_mode=True,
    )
    info = p.run([{"doc_id": 1, "value": "loaded over a separate grpc host"}], table_name="content")
    assert_load_info(info)

    client: WeaviateClient
    with p.destination_client() as client:  # type: ignore[assignment]
        params = client.config.to_connector_params()
        assert params["http_host"] == "127.0.0.1"
        assert params["grpc_host"] == "localhost"

        collection = client.db_client.collections.get(
            client.make_qualified_collection_name("Content")
        )
        objects = collection.query.fetch_objects(limit=5).objects
        assert [o.properties["value"] for o in objects] == ["loaded over a separate grpc host"]


def test_pipeline_with_extra_collection_config() -> None:
    """`collection_config` reaches `collections.create` for arguments dlt has no field for."""
    from weaviate.classes.config import Configure

    p = dlt.pipeline(
        pipeline_name="weaviate_collection_config_" + uniq_id(),
        destination=weaviate(
            collection_config={
                "description": "created by dlt",
                "replication_config": Configure.replication(factor=1),
                "inverted_index_config": Configure.inverted_index(index_null_state=True),
            }
        ),
        dataset_name="TestCollectionConfig" + uniq_id(),
        dev_mode=True,
    )
    info = p.run([{"doc_id": 1, "value": "hello"}], table_name="content")
    assert_load_info(info)

    client: WeaviateClient
    with p.destination_client() as client:  # type: ignore[assignment]
        config = client.db_client.collections.get(
            client.make_qualified_collection_name("Content")
        ).config.get()

        assert config.description == "created by dlt"
        assert config.replication_config.factor == 1
        assert config.inverted_index_config.index_null_state is True


def test_extra_collection_config_rejects_what_dlt_derives() -> None:
    p = dlt.pipeline(
        pipeline_name="weaviate_bad_collection_config_" + uniq_id(),
        destination=weaviate(collection_config={"vector_config": "nope"}),
        dataset_name="TestBadCollectionConfig" + uniq_id(),
        dev_mode=True,
    )

    client: WeaviateClient
    with p.destination_client() as client:  # type: ignore[assignment]
        with pytest.raises(ConfigurationValueError, match="vector_config"):
            client.extra_collection_config()


def test_multi_tenancy_activates_an_inactive_tenant() -> None:
    """Weaviate refuses writes to a deactivated tenant unless the collection auto-activates."""
    from weaviate.classes.tenants import Tenant, TenantActivityStatus

    dataset_name = "TestTenantActivation" + uniq_id()

    def load(rows) -> dlt.Pipeline:
        pipeline = dlt.pipeline(
            pipeline_name="weaviate_tenant_activation_" + uniq_id(),
            destination=weaviate(multi_tenancy=True, tenant="tenanta"),
            dataset_name=dataset_name,
        )
        assert_load_info(
            pipeline.run(rows, table_name="items", primary_key="id", write_disposition="merge")
        )
        return pipeline

    p = load([{"id": 1, "v": "before"}])

    client: WeaviateClient
    with p.destination_client() as client:  # type: ignore[assignment]
        items = client.db_client.collections.get(client.make_qualified_collection_name("Items"))
        tenancy = items.config.get().multi_tenancy_config
        assert tenancy.auto_tenant_creation is True
        assert tenancy.auto_tenant_activation is True

        items.tenants.update(Tenant(name="tenanta", activity_status=TenantActivityStatus.INACTIVE))
        assert items.tenants.get_by_name("tenanta").activity_status == TenantActivityStatus.INACTIVE

    # the tenant is inactive, and loading into it must still work
    p = load([{"id": 2, "v": "after"}])

    with p.destination_client() as client:  # type: ignore[assignment]
        items = client.db_client.collections.get(client.make_qualified_collection_name("Items"))
        loaded = sorted(
            str(o.properties["v"])
            for o in items.with_tenant("tenanta").query.fetch_objects(limit=10).objects
        )
        assert loaded == ["after", "before"]
