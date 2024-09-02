from typing import Iterator, List, Generator, Any

import numpy as np
import pandas as pd
import pytest
from lancedb.table import Table  # type: ignore
from pandas import DataFrame
from pandas.testing import assert_frame_equal

import dlt
from dlt.common.typing import DictStrAny, DictStrStr
from dlt.common.utils import uniq_id
from dlt.destinations.impl.lancedb.lancedb_adapter import (
    lancedb_adapter,
)
from tests.load.lancedb.utils import chunk_document
from tests.load.utils import (
    drop_active_pipeline_data,
    sequence_generator,
)
from tests.pipeline.utils import (
    assert_load_info,
)


# Mark all tests as essential, don't remove.
pytestmark = pytest.mark.essential


@pytest.fixture(autouse=True)
def drop_lancedb_data() -> Iterator[None]:
    yield
    drop_active_pipeline_data()


def test_lancedb_remove_nested_orphaned_records() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_lancedb_remove_orphaned_records",
        destination="lancedb",
        dataset_name=f"test_lancedb_remove_orphaned_records_{uniq_id()}",
        dev_mode=True,
    )

    @dlt.resource(
        table_name="parent",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        primary_key="id",
        merge_key="id",
    )
    def identity_resource(
        data: List[DictStrAny],
    ) -> Generator[List[DictStrAny], None, None]:
        yield data

    run_1 = [
        {
            "id": 1,
            "child": [
                {"bar": 1, "grandchild": [{"baz": 1}, {"baz": 2}]},
                {"bar": 2, "grandchild": [{"baz": 3}]},
            ],
        },
        {"id": 2, "child": [{"bar": 3, "grandchild": [{"baz": 4}]}]},
        {
            "id": 3,
            "child": [
                {"bar": 10, "grandchild": [{"baz": 5}]},
                {"bar": 11, "grandchild": [{"baz": 6}, {"baz": 7}]},
            ],
        },
    ]
    info = pipeline.run(identity_resource(run_1))
    assert_load_info(info)

    run_2 = [
        {
            "id": 1,
            "child": [{"bar": 1, "grandchild": [{"baz": 1}]}],
        },  # Removes bar_2, baz_2 and baz_3.
        {
            "id": 2,
            "child": [{"bar": 4, "grandchild": [{"baz": 8}]}],
        },  # Removes bar_3, baz_4.
    ]
    info = pipeline.run(identity_resource(run_2))
    assert_load_info(info)

    with pipeline.destination_client() as client:
        expected_parent_data = pd.DataFrame(
            data=[
                {"id": 1},
                {"id": 2},
                {"id": 3},
            ]
        )

        expected_child_data = pd.DataFrame(
            data=[
                {"bar": 1},
                {"bar": 4},
                {"bar": 10},
                {"bar": 11},
            ]
        )

        expected_grandchild_data = pd.DataFrame(
            data=[
                {"baz": 1},
                {"baz": 8},
                {"baz": 5},
                {"baz": 6},
                {"baz": 7},
            ]
        )

        parent_table_name = client.make_qualified_table_name("parent")  # type: ignore[attr-defined]
        child_table_name = client.make_qualified_table_name("parent__child")  # type: ignore[attr-defined]
        grandchild_table_name = client.make_qualified_table_name(  # type: ignore[attr-defined]
            "parent__child__grandchild"
        )

        parent_tbl = client.db_client.open_table(parent_table_name)  # type: ignore[attr-defined]
        child_tbl = client.db_client.open_table(child_table_name)  # type: ignore[attr-defined]
        grandchild_tbl = client.db_client.open_table(grandchild_table_name)  # type: ignore[attr-defined]

        actual_parent_df = parent_tbl.to_pandas().sort_values(by="id").reset_index(drop=True)
        actual_child_df = child_tbl.to_pandas().sort_values(by="bar").reset_index(drop=True)
        actual_grandchild_df = (
            grandchild_tbl.to_pandas().sort_values(by="baz").reset_index(drop=True)
        )

        expected_parent_data = expected_parent_data.sort_values(by="id").reset_index(drop=True)
        expected_child_data = expected_child_data.sort_values(by="bar").reset_index(drop=True)
        expected_grandchild_data = expected_grandchild_data.sort_values(by="baz").reset_index(
            drop=True
        )

        assert_frame_equal(actual_parent_df[["id"]], expected_parent_data)
        assert_frame_equal(actual_child_df[["bar"]], expected_child_data)
        assert_frame_equal(actual_grandchild_df[["baz"]], expected_grandchild_data)


def test_lancedb_remove_orphaned_records_root_table() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_lancedb_remove_orphaned_records_root_table",
        destination="lancedb",
        dataset_name=f"test_lancedb_remove_orphaned_records_root_table_{uniq_id()}",
        dev_mode=True,
    )

    @dlt.resource(
        table_name="root",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        primary_key=["doc_id", "chunk_hash"],
        merge_key=["doc_id"],
    )
    def identity_resource(
        data: List[DictStrAny],
    ) -> Generator[List[DictStrAny], None, None]:
        yield data

    lancedb_adapter(identity_resource)

    run_1 = [
        {"doc_id": 1, "chunk_hash": "1a"},
        {"doc_id": 2, "chunk_hash": "2a"},
        {"doc_id": 2, "chunk_hash": "2b"},
        {"doc_id": 2, "chunk_hash": "2c"},
        {"doc_id": 3, "chunk_hash": "3a"},
        {"doc_id": 3, "chunk_hash": "3b"},
    ]
    info = pipeline.run(identity_resource(run_1))
    assert_load_info(info)

    run_2 = [
        {"doc_id": 2, "chunk_hash": "2d"},
        {"doc_id": 2, "chunk_hash": "2e"},
        {"doc_id": 3, "chunk_hash": "3b"},
    ]
    info = pipeline.run(identity_resource(run_2))
    assert_load_info(info)

    with pipeline.destination_client() as client:
        expected_root_table_df = (
            pd.DataFrame(
                data=[
                    {"doc_id": 1, "chunk_hash": "1a"},
                    {"doc_id": 2, "chunk_hash": "2d"},
                    {"doc_id": 2, "chunk_hash": "2e"},
                    {"doc_id": 3, "chunk_hash": "3b"},
                ]
            )
            .sort_values(by=["doc_id", "chunk_hash"])
            .reset_index(drop=True)
        )

        root_table_name = client.make_qualified_table_name("root")  # type: ignore[attr-defined]
        tbl = client.db_client.open_table(root_table_name)  # type: ignore[attr-defined]

        actual_root_df: DataFrame = (
            tbl.to_pandas().sort_values(by=["doc_id", "chunk_hash"]).reset_index(drop=True)
        )[["doc_id", "chunk_hash"]]

        assert_frame_equal(actual_root_df, expected_root_table_df)


def test_lancedb_remove_orphaned_records_root_table_string_doc_id() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_lancedb_remove_orphaned_records_root_table",
        destination="lancedb",
        dataset_name=f"test_lancedb_remove_orphaned_records_root_table_{uniq_id()}",
        dev_mode=True,
    )

    @dlt.resource(
        table_name="root",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        primary_key=["doc_id", "chunk_hash"],
        merge_key=["doc_id"],
    )
    def identity_resource(
        data: List[DictStrAny],
    ) -> Generator[List[DictStrAny], None, None]:
        yield data

    lancedb_adapter(identity_resource)

    run_1 = [
        {"doc_id": "A", "chunk_hash": "1a"},
        {"doc_id": "B", "chunk_hash": "2a"},
        {"doc_id": "B", "chunk_hash": "2b"},
        {"doc_id": "B", "chunk_hash": "2c"},
        {"doc_id": "C", "chunk_hash": "3a"},
        {"doc_id": "C", "chunk_hash": "3b"},
    ]
    info = pipeline.run(identity_resource(run_1))
    assert_load_info(info)

    run_2 = [
        {"doc_id": "B", "chunk_hash": "2d"},
        {"doc_id": "B", "chunk_hash": "2e"},
        {"doc_id": "C", "chunk_hash": "3b"},
    ]
    info = pipeline.run(identity_resource(run_2))
    assert_load_info(info)

    with pipeline.destination_client() as client:
        expected_root_table_df = (
            pd.DataFrame(
                data=[
                    {"doc_id": "A", "chunk_hash": "1a"},
                    {"doc_id": "B", "chunk_hash": "2d"},
                    {"doc_id": "B", "chunk_hash": "2e"},
                    {"doc_id": "C", "chunk_hash": "3b"},
                ]
            )
            .sort_values(by=["doc_id", "chunk_hash"])
            .reset_index(drop=True)
        )

        root_table_name = client.make_qualified_table_name("root")  # type: ignore[attr-defined]
        tbl = client.db_client.open_table(root_table_name)  # type: ignore[attr-defined]

        actual_root_df: DataFrame = (
            tbl.to_pandas().sort_values(by=["doc_id", "chunk_hash"]).reset_index(drop=True)
        )[["doc_id", "chunk_hash"]]

        assert_frame_equal(actual_root_df, expected_root_table_df)


def test_lancedb_root_table_remove_orphaned_records_with_real_embeddings() -> None:
    @dlt.resource(
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        table_name="document",
        primary_key=["doc_id", "chunk"],
        merge_key="doc_id",
    )
    def documents(docs: List[DictStrAny]) -> Generator[DictStrAny, None, None]:
        for doc in docs:
            doc_id = doc["doc_id"]
            for chunk in chunk_document(doc["text"]):
                yield {"doc_id": doc_id, "doc_text": doc["text"], "chunk": chunk}

    @dlt.source()
    def documents_source(
        docs: List[DictStrAny],
    ) -> Any:
        return documents(docs)

    lancedb_adapter(
        documents,
        embed=["chunk"],
    )

    pipeline = dlt.pipeline(
        pipeline_name="test_lancedb_remove_orphaned_records_with_embeddings",
        destination="lancedb",
        dataset_name=f"test_lancedb_remove_orphaned_records_{uniq_id()}",
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
        embeddings_table_name = client.make_qualified_table_name("document")  # type: ignore[attr-defined]
        tbl: Table = client.db_client.open_table(embeddings_table_name)  # type: ignore[attr-defined]
        df = tbl.to_pandas()

        # Check (non-empty) embeddings as present, and that orphaned embeddings have been discarded.
        assert len(df) == 21
        assert "vector" in df.columns
        for _, vector in enumerate(df["vector"]):
            assert isinstance(vector, np.ndarray)
            assert vector.size > 0


def test_lancedb_compound_merge_key_root_table() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_lancedb_compound_merge_key",
        destination="lancedb",
        dataset_name=f"test_lancedb_remove_orphaned_records_root_table_{uniq_id()}",
        dev_mode=True,
    )

    @dlt.resource(
        table_name="root",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        primary_key=["doc_id", "chunk_hash"],
        merge_key=["doc_id", "chunk_hash"],
    )
    def identity_resource(
        data: List[DictStrAny],
    ) -> Generator[List[DictStrAny], None, None]:
        yield data

    lancedb_adapter(identity_resource, no_remove_orphans=True)

    run_1 = [
        {"doc_id": 1, "chunk_hash": "a", "foo": "bar"},
        {"doc_id": 1, "chunk_hash": "b", "foo": "coo"},
    ]
    info = pipeline.run(identity_resource(run_1))
    assert_load_info(info)

    run_2 = [
        {"doc_id": 1, "chunk_hash": "a", "foo": "aat"},
        {"doc_id": 1, "chunk_hash": "c", "foo": "loot"},
    ]
    info = pipeline.run(identity_resource(run_2))
    assert_load_info(info)

    with pipeline.destination_client() as client:
        expected_root_table_df = (
            pd.DataFrame(
                data=[
                    {"doc_id": 1, "chunk_hash": "a", "foo": "aat"},
                    {"doc_id": 1, "chunk_hash": "b", "foo": "coo"},
                    {"doc_id": 1, "chunk_hash": "c", "foo": "loot"},
                ]
            )
            .sort_values(by=["doc_id", "chunk_hash", "foo"])
            .reset_index(drop=True)
        )

        root_table_name = client.make_qualified_table_name("root")  # type: ignore[attr-defined]
        tbl = client.db_client.open_table(root_table_name)  # type: ignore[attr-defined]

        actual_root_df: DataFrame = (
            tbl.to_pandas().sort_values(by=["doc_id", "chunk_hash", "foo"]).reset_index(drop=True)
        )[["doc_id", "chunk_hash", "foo"]]

    assert_frame_equal(actual_root_df, expected_root_table_df)


def test_must_provide_at_least_primary_key_on_merge_disposition() -> None:
    """We need upsert merge's deterministic _dlt_id to perform orphan removal.
    Hence, we require at least the primary key required (raises exception if missing).
    Specify a merge key for custom orphan identification."""
    generator_instance1 = sequence_generator()

    @dlt.resource(write_disposition={"disposition": "merge", "strategy": "upsert"})
    def some_data() -> Generator[DictStrStr, Any, None]:
        yield from next(generator_instance1)

    pipeline = dlt.pipeline(
        pipeline_name="test_must_provide_both_primary_and_merge_key_on_merge_disposition",
        destination="lancedb",
        dataset_name=(
            f"test_must_provide_both_primary_and_merge_key_on_merge_disposition{uniq_id()}"
        ),
    )
    with pytest.raises(Exception):
        load_info = pipeline.run(
            some_data(),
        )
        assert_load_info(load_info)
