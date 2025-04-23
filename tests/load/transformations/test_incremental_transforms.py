import pytest
from typing import Any, Iterator
import dlt
from dlt.extract.hints import TDataItem
from dlt.common.destination.dataset import SupportsReadableDataset

from tests.load.utils import DestinationTestConfiguration
from tests.load.transformations.utils import transformation_configs, setup_transformation_pipelines
from dlt.pipeline.exceptions import PipelineNeverRan


@pytest.fixture
def inc_p() -> dlt.Pipeline:
    pipeline = dlt.pipeline(
        pipeline_name="incremental_pipeline",
        destination="duckdb",
        dataset_name="incremental_dataset",
        dev_mode=True,
    )
    return pipeline


@dlt.resource(table_name="items")
def first_load() -> Iterator[TDataItem]:
    yield from [
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ]


@dlt.resource(table_name="items")
def inc_load() -> Iterator[TDataItem]:
    yield from [
        {"id": 4},
        {"id": 5},
        {"id": 6},
    ]


EXPECTED_TRANSFORMED_DATA_FIRST_LOAD = [
    {"id": 1, "double_items": 2},
    {"id": 2, "double_items": 4},
    {"id": 3, "double_items": 6},
]

EXPECTED_TRANSFORMED_DATA_SECOND_LOAD = [
    {"id": 1, "double_items": 2},
    {"id": 2, "double_items": 4},
    {"id": 3, "double_items": 6},
    {"id": 4, "double_items": 8},
    {"id": 5, "double_items": 10},
    {"id": 6, "double_items": 12},
]


def _assert_transformed_data(inc_p: dlt.Pipeline, expected_data: list[Any]) -> None:
    assert (
        inc_p.dataset()
        .transformed_items.select("id", "double_items")
        .df()
        .to_dict(orient="records")
        == expected_data
    )


@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("transformation_type", ["python", "sql"])
def test_state_based_incremental_transform(
    destination_config: DestinationTestConfiguration, transformation_type: Any
) -> None:
    """
    Here we demonstrate how to use the resource state for transform incrementals
    """
    # get pipelines and populate fruit pipeline
    inc_p, dest_p = setup_transformation_pipelines(destination_config, transformation_type)

    @dlt.transformation(
        transformation_type=transformation_type,
        write_disposition="append",
        columns={"double_items": {"data_type": "bigint"}},
    )
    def transformed_items(dataset: SupportsReadableDataset[Any]) -> Any:
        # get last stored processed load id
        LAST_PROCESSED_LOAD_ID = "last_processed_load_id"
        last_processed_load_id = dlt.current.resource_state().get(LAST_PROCESSED_LOAD_ID, "0")
        items_table = dataset.items

        # NOTE: getting this one value is a bit complicated.., probably there is a better way to
        # do this
        max_load_id = (
            items_table._dlt_load_id.max().df().to_dict(orient="records")[0]["Max(_dlt_load_id)"]
        )
        dlt.current.resource_state()[LAST_PROCESSED_LOAD_ID] = max_load_id

        # return filtered transformation
        return items_table.filter(items_table._dlt_load_id > last_processed_load_id).mutate(
            double_items=items_table.id * 2
        )

    # first round
    inc_p.run(first_load())
    dest_p.run(transformed_items(inc_p.dataset()))
    _assert_transformed_data(dest_p, EXPECTED_TRANSFORMED_DATA_FIRST_LOAD)

    # second round
    inc_p.run(inc_load())
    dest_p.run(transformed_items(inc_p.dataset()))
    _assert_transformed_data(dest_p, EXPECTED_TRANSFORMED_DATA_SECOND_LOAD)


@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("transformation_type", ["python", "sql"])
def test_primary_key_based_incremental_transform(
    destination_config: DestinationTestConfiguration, transformation_type: Any
) -> None:
    """
    Here we demonstrate how to look up the newest primary key in the output dataset
    and use it to filter the rows that were already processed, only works if primary key
    is incrementing reliably
    """
    # get pipelines and populate fruit pipeline
    inc_p, dest_p = setup_transformation_pipelines(destination_config, transformation_type)

    @dlt.transformation(
        transformation_type=transformation_type,
        write_disposition="append",
        columns={"double_items": {"data_type": "bigint"}},
    )
    def transformed_items(dataset: SupportsReadableDataset[Any]) -> Any:
        # get newest primary key but only if table exists

        max_pimary_key = 0

        try:
            output_dataset = dlt.current.pipeline().dataset()
            if output_dataset.schema.tables.get("transformed_items"):
                max_pimary_key = (
                    output_dataset.transformed_items.id.max()
                    .df()
                    .to_dict(orient="records")[0]["Max(id)"]
                )
        except PipelineNeverRan:
            pass

        # return filtered transformation
        items_table = dataset.items
        return items_table.filter(items_table.id > max_pimary_key).mutate(
            double_items=items_table.id * 2
        )

    # first round
    inc_p.run(first_load())
    dest_p.run(transformed_items(inc_p.dataset()))
    _assert_transformed_data(dest_p, EXPECTED_TRANSFORMED_DATA_FIRST_LOAD)

    # second round
    inc_p.run(inc_load())
    dest_p.run(transformed_items(inc_p.dataset()))
    _assert_transformed_data(dest_p, EXPECTED_TRANSFORMED_DATA_SECOND_LOAD)


@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("transformation_type", ["python", "sql"])
def test_load_id_based_incremental_transform(
    destination_config: DestinationTestConfiguration, transformation_type: Any
) -> None:
    """
    Here we demonstrate how to look up the newest load_id in the output dataset
    and use it to filter the rows that were already processed, very similar to
    the above
    """
    # get pipelines and populate fruit pipeline
    inc_p, dest_p = setup_transformation_pipelines(destination_config, transformation_type)

    @dlt.transformation(
        transformation_type=transformation_type,
        write_disposition="append",
        columns={"double_items": {"data_type": "bigint"}},
    )
    def transformed_items(dataset: SupportsReadableDataset[Any]) -> Any:
        # get newest primary key but only if table exists

        max_load_id = "0"

        try:
            output_dataset = dlt.current.pipeline().dataset()
            if output_dataset.schema.tables.get("transformed_items"):
                max_load_id = (
                    output_dataset.transformed_items._dlt_load_id.max()
                    .df()
                    .to_dict(orient="records")[0]["Max(_dlt_load_id)"]
                )
        except PipelineNeverRan:
            pass

        # return filtered transformation
        items_table = dataset.items
        return items_table.filter(items_table._dlt_load_id > max_load_id).mutate(
            double_items=items_table.id * 2
        )

    # first round
    inc_p.run(first_load())
    dest_p.run(transformed_items(inc_p.dataset()))
    _assert_transformed_data(dest_p, EXPECTED_TRANSFORMED_DATA_FIRST_LOAD)

    # second round
    inc_p.run(inc_load())
    dest_p.run(transformed_items(inc_p.dataset()))
    _assert_transformed_data(dest_p, EXPECTED_TRANSFORMED_DATA_SECOND_LOAD)
