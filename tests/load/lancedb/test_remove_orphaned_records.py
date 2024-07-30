from typing import Iterator, List, Generator

import pytest

import dlt
from dlt.common.schema.typing import TLoaderMergeStrategy
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


@pytest.mark.parametrize("merge_strategy", ("delete-insert", "upsert"))
def test_lancedb_remove_orphaned_records(
    merge_strategy: TLoaderMergeStrategy,
) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_pipeline_append",
        destination="lancedb",
        dataset_name=f"TestPipelineAppendDataset{uniq_id()}",
    )

    @dlt.resource(
        table_name="parent",
        write_disposition={"disposition": "merge", "strategy": merge_strategy},
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
        expected_child_data = [
            1,
            4,
            10,
            11,
        ]

        embeddings_table_name = client.make_qualified_table_name("parent__child")  # type: ignore[attr-defined]

        tbl = client.db_client.open_table(embeddings_table_name)  # type: ignore[attr-defined]
        df = tbl.to_pandas()
        assert sorted(df["bar"].to_list()) == expected_child_data
