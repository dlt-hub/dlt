from typing import Iterator, List, Generator

import pandas as pd
import pytest
from pandas import DataFrame
from pandas.testing import assert_frame_equal

import dlt
from dlt.common.typing import DictStrAny
from dlt.common.utils import uniq_id
from tests.load.utils import (
    drop_active_pipeline_data,
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


def test_lancedb_remove_orphaned_records() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_lancedb_remove_orphaned_records",
        destination="lancedb",
        dataset_name=f"test_lancedb_remove_orphaned_records_{uniq_id()}",
        dev_mode=True,
    )

    @dlt.resource(
        table_name="parent",
        write_disposition="merge",
        primary_key="id",
    )
    def identity_resource(
        data: List[DictStrAny],
    ) -> Generator[List[DictStrAny], None, None]:
        yield data

    run_1 = [
        {"id": 1, "child": [{"bar": 1}, {"bar": 2}]},
        {"id": 2, "child": [{"bar": 3}]},
        {"id": 3, "child": [{"bar": 10}, {"bar": 11}]},
    ]
    info = pipeline.run(identity_resource(run_1))
    assert_load_info(info)

    run_2 = [
        {"id": 1, "child": [{"bar": 1}]},  # Removed one child.
        {"id": 2, "child": [{"bar": 4}]},  # Changed child.
    ]
    info = pipeline.run(identity_resource(run_2))
    assert_load_info(info)

    with pipeline.destination_client() as client:
        expected_child_data = pd.DataFrame(
            data=[
                {"bar": 1},
                {"bar": 4},
                {"bar": 10},
                {"bar": 11},
            ]
        )

        embeddings_table_name = client.make_qualified_table_name("parent__child")  # type: ignore[attr-defined]

        tbl = client.db_client.open_table(embeddings_table_name)  # type: ignore[attr-defined]
        actual_df = tbl.to_pandas()

        expected_child_data = expected_child_data.sort_values(by="bar")
        actual_df = actual_df.sort_values(by="bar").reset_index(drop=True)

        assert_frame_equal(actual_df[["bar"]], expected_child_data)


def test_lancedb_remove_orphaned_records_root_table() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_lancedb_remove_orphaned_records_root_table",
        destination="lancedb",
        dataset_name=f"test_lancedb_remove_orphaned_records_root_table_{uniq_id()}",
        dev_mode=True,
    )

    @dlt.resource(
        table_name="root",
        write_disposition="merge",
        primary_key="doc_id",
    )
    def identity_resource(
        data: List[DictStrAny],
    ) -> Generator[List[DictStrAny], None, None]:
        yield data

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
        expected_root_table_df = pd.DataFrame(
            data=[
                {"doc_id": 1, "chunk_hash": "1a"},
                {"doc_id": 2, "chunk_hash": "2d"},
                {"doc_id": 2, "chunk_hash": "2e"},
                {"doc_id": 3, "chunk_hash": "3b"},
            ]
        ).sort_values(by=["doc_id", "chunk_hash"])

        root_table_name = (
            client.make_qualified_table_name("root")  # type: ignore[attr-defined]
        )
        tbl = client.db_client.open_table(root_table_name)  # type: ignore[attr-defined]

        actual_root_df: DataFrame = (
            tbl.to_pandas()
            .sort_values(by=["doc_id", "chunk_hash"])
            .reset_index(drop=True)
        )
        assert_frame_equal(actual_root_df, expected_root_table_df)
