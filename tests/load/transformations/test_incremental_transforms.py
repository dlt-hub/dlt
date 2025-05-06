import pytest
from typing import Any, Iterator
import dlt
import os
from dlt.extract.hints import TDataItem

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
    found_data = (
        inc_p.dataset()
        .transformed_items.select("id", "double_items")
        .df()
        .to_dict(orient="records")
    )

    # make all dict keys lowercase for our favorite snowflake
    found_data = [{k.lower(): v for k, v in d.items()} for d in found_data]
    sorted_found_data = sorted(found_data, key=lambda x: x["id"])

    assert sorted_found_data == expected_data


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(),
    ids=lambda x: x.name,
)
def test_state_based_incremental_transform(
    destination_config: DestinationTestConfiguration,
) -> None:
    """
    Here we demonstrate how to use the resource state for transform incrementals
    """
    # get pipelines and populate fruit pipeline
    inc_p, dest_p = setup_transformation_pipelines(destination_config)

    @dlt.transformation(
        write_disposition="append",
    )
    def transformed_items(dataset: dlt.Dataset, last_loaded_load_id: str) -> Any:
        # get last stored processed load id
        LAST_PROCESSED_LOAD_ID = "last_processed_load_id"
        last_processed_load_id = dlt.current.resource_state().get(LAST_PROCESSED_LOAD_ID, "0")
        items_table = dataset.items

        max_load_id = items_table._dlt_load_id.max().scalar()
        dlt.current.resource_state()[LAST_PROCESSED_LOAD_ID] = max_load_id

        # return filtered transformation
        return items_table.filter(
            items_table._dlt_load_id > last_processed_load_id,
            items_table._dlt_load_id <= last_loaded_load_id,
        ).mutate(double_items=items_table.id * 2)

    # first round
    inc_p.run(first_load())
    last_loaded_load_id = inc_p.dataset()._dlt_loads.load_id.max().scalar()

    dest_p.run(transformed_items(inc_p.dataset(), last_loaded_load_id))
    _assert_transformed_data(dest_p, EXPECTED_TRANSFORMED_DATA_FIRST_LOAD)

    # second round
    inc_p.run(inc_load())
    last_loaded_load_id = inc_p.dataset()._dlt_loads.load_id.max().scalar()

    dest_p.run(transformed_items(inc_p.dataset(), last_loaded_load_id))
    _assert_transformed_data(dest_p, EXPECTED_TRANSFORMED_DATA_SECOND_LOAD)


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(),
    ids=lambda x: x.name,
)
def test_primary_key_based_incremental_transform(
    destination_config: DestinationTestConfiguration,
) -> None:
    """
    Here we demonstrate how to look up the newest primary key in the output dataset
    and use it to filter the rows that were already processed, only works if primary key
    is incrementing reliably
    """
    # get pipelines and populate fruit pipeline
    inc_p, dest_p = setup_transformation_pipelines(destination_config)

    @dlt.transformation(
        write_disposition="append",
    )
    def transformed_items(dataset: dlt.Dataset) -> Any:
        # get newest primary key but only if table exists
        max_pimary_key = 0
        try:
            output_dataset = dlt.current.pipeline().dataset()
            if output_dataset.schema.tables.get("transformed_items"):
                max_pimary_key = output_dataset.transformed_items.id.max().scalar()
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


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(),
    ids=lambda x: x.name,
)
def test_load_id_based_incremental_transform(
    destination_config: DestinationTestConfiguration,
) -> None:
    """
    Here we demonstrate how to look up the newest load_id in the output dataset
    and use it to filter the rows that were already processed, very similar to
    the above. We also test injecting the config value from env vars.
    """
    # get pipelines and populate fruit pipeline
    inc_p, dest_p = setup_transformation_pipelines(destination_config)

    @dlt.source(name="transformation_source")
    def transformation_source(dataset: dlt.Dataset, last_loaded_load_id: str = dlt.config.value):
        @dlt.transformation(
            write_disposition="append",
        )
        def transformed_items(dataset: dlt.Dataset) -> Any:
            # get newest primary key but only if table exists
            max_load_id = "0"
            try:
                output_dataset = dlt.current.pipeline().dataset()
                if output_dataset.schema.tables.get("transformed_items"):
                    max_load_id = output_dataset.transformed_items._dlt_load_id.max().scalar()
            except PipelineNeverRan:
                pass

            # return filtered transformation
            items_table = dataset.items
            return items_table.filter(
                items_table._dlt_load_id > max_load_id,
                items_table._dlt_load_id <= last_loaded_load_id,
            ).mutate(double_items=items_table.id * 2)

        return transformed_items(dataset)

    # first round
    inc_p.run(first_load())
    os.environ["LAST_LOADED_LOAD_ID"] = inc_p.dataset()._dlt_loads.load_id.max().scalar()
    dest_p.run(transformation_source(inc_p.dataset()))
    _assert_transformed_data(dest_p, EXPECTED_TRANSFORMED_DATA_FIRST_LOAD)

    # second round
    inc_p.run(inc_load())
    os.environ["LAST_LOADED_LOAD_ID"] = inc_p.dataset()._dlt_loads.load_id.max().scalar()
    dest_p.run(transformation_source(inc_p.dataset()))
    _assert_transformed_data(dest_p, EXPECTED_TRANSFORMED_DATA_SECOND_LOAD)


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(),
    ids=lambda x: x.name,
)
def test_merge_based_incremental_transform(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Here we technically don't transform incrementally but transform all the data and merge it on the primary key"""

    if destination_config.destination_type == "clickhouse":
        pytest.skip("Clickhouse fails because supposedly the sort column contains nulls?")

    # get pipelines and populate fruit pipeline
    inc_p, dest_p = setup_transformation_pipelines(destination_config)

    @dlt.transformation(
        write_disposition="merge",
        primary_key="id",
    )
    def transformed_items(dataset: dlt.Dataset) -> Any:
        # return filtered transformation
        items_table = dataset.items
        return items_table.mutate(double_items=items_table.id * 2)

    # first round
    inc_p.run(first_load())
    dest_p.run(transformed_items(inc_p.dataset()))
    _assert_transformed_data(dest_p, EXPECTED_TRANSFORMED_DATA_FIRST_LOAD)

    # second round
    inc_p.run(inc_load())
    dest_p.run(transformed_items(inc_p.dataset()))
    _assert_transformed_data(dest_p, EXPECTED_TRANSFORMED_DATA_SECOND_LOAD)
