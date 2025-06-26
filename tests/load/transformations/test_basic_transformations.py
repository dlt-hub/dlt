import pytest

from typing import Any, List

import dlt

from dlt.pipeline.exceptions import PipelineStepFailed


from tests.load.utils import (
    DestinationTestConfiguration,
)

from tests.pipeline.utils import load_table_counts

from tests.load.transformations.utils import (
    get_job_types,
    transformation_configs,
    setup_transformation_pipelines,
)

from dlt.sources._single_file_templates.fruitshop_pipeline import (
    fruitshop as fruitshop_source,
)


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(),
    ids=lambda x: x.name,
)
def test_simple_query_transformations(destination_config: DestinationTestConfiguration) -> None:
    # get pipelines andpopulate fruit pipeline
    fruit_p, dest_p = setup_transformation_pipelines(destination_config)
    fruit_p.run(fruitshop_source())

    @dlt.transformation()
    def copied_customers(dataset: dlt.Dataset) -> Any:
        yield dataset["customers"].limit(5)

    # transform into transformed dataset
    dest_p.run(copied_customers(fruit_p.dataset()))

    assert load_table_counts(dest_p, "copied_customers") == {
        "copied_customers": 5,
    }

    # verify the right transformation was run
    assert (
        list(get_job_types(dest_p)["copied_customers"].keys())[0] == "parquet"
        if destination_config.destination_type == "filesystem"
        else "model"
    )


# NOTE: move to duckdb only transformation tests
@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(),
    ids=lambda x: x.name,
)
def test_grouped_transformations(destination_config: DestinationTestConfiguration) -> None:
    # get pipelines and populate fruit pipeline
    fruit_p, dest_p = setup_transformation_pipelines(destination_config)
    fruit_p.run(fruitshop_source())

    @dlt.transformation()
    def copied_customers(dataset: dlt.Dataset) -> Any:
        yield dataset["customers"].limit(5)

    @dlt.transformation()
    def copied_customers2(dataset: dlt.Dataset) -> Any:
        yield dataset["customers"].limit(7)

    @dlt.source()
    def transformations(dataset: dlt.Dataset) -> List[Any]:
        return [copied_customers(dataset), copied_customers2(dataset)]

    dest_p.run(transformations(fruit_p.dataset()))

    assert load_table_counts(dest_p, "copied_customers", "copied_customers2") == {
        "copied_customers": 5,
        "copied_customers2": 7,
    }

    # verify the right transformation was run
    assert (
        list(get_job_types(dest_p)["copied_customers"].keys())[0] == "parquet"
        if destination_config.destination_type == "filesystem"
        else "model"
    )

    assert (
        list(get_job_types(dest_p)["copied_customers2"].keys())[0] == "parquet"
        if destination_config.destination_type == "filesystem"
        else "model"
    )


# NOTE: move to duckdb only transformation tests
@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(),
    ids=lambda x: x.name,
)
def test_replace_sql_transformations(destination_config: DestinationTestConfiguration) -> None:
    # get pipelines and populate fruit pipeline
    fruit_p, dest_p = setup_transformation_pipelines(destination_config)
    fruit_p.run(fruitshop_source())

    @dlt.transformation(write_disposition="replace")
    def copied_customers(dataset: dlt.Dataset) -> Any:
        yield dataset["customers"].limit(5)

    # transform into same dataset
    dest_p.run(copied_customers(fruit_p.dataset()))

    assert load_table_counts(dest_p, "copied_customers") == {
        "copied_customers": 5,
    }

    # overwrite with different setting
    @dlt.transformation(
        write_disposition="replace",
        table_name="copied_customers",
    )
    def copied_customers_updated(dataset: dlt.Dataset) -> Any:
        yield dataset["customers"].limit(3)

    # transform into same dataset
    dest_p.run(copied_customers_updated(fruit_p.dataset()))
    assert load_table_counts(dest_p, "copied_customers") == {
        "copied_customers": 3,
    }


# NOTE: move to duckdb only transformation tests
@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(),
    ids=lambda x: x.name,
)
def test_append_sql_transformations(destination_config: DestinationTestConfiguration) -> None:
    # get pipelines and populate fruit pipeline
    fruit_p, dest_p = setup_transformation_pipelines(destination_config)
    fruit_p.run(fruitshop_source())

    @dlt.transformation(write_disposition="append")
    def copied_customers(dataset: dlt.Dataset) -> Any:
        yield dataset["customers"].limit(5)

    # transform into same dataset
    dest_p.run(copied_customers(fruit_p.dataset()))

    assert load_table_counts(dest_p, "copied_customers") == {
        "copied_customers": 5,
    }

    @dlt.transformation(write_disposition="append", table_name="copied_customers")
    def copied_table_updated(dataset: dlt.Dataset) -> Any:
        yield dataset["customers"].limit(7)

    # transform into same dataset
    dest_p.run(copied_table_updated(fruit_p.dataset()))

    assert load_table_counts(dest_p, "copied_customers") == {
        "copied_customers": 12,
    }


@pytest.mark.skip(
    reason=(
        "TODO: lineage got better, so I need a better way to figure out an unknown column type for"
        " this test. @zilto says all types are always known now, so maybe it is not possible."
    )
)
# NOTE: move to duckdb only transformation tests
@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(),
    ids=lambda x: x.name,
)
def test_sql_transformation_with_unknown_column_types(
    destination_config: DestinationTestConfiguration,
) -> None:
    # get pipelines and populate fruit pipeline
    fruit_p, dest_p = setup_transformation_pipelines(destination_config)
    fruit_p.run(fruitshop_source())

    @dlt.transformation()
    def mutated_purchases(dataset: dlt.Dataset) -> Any:
        yield dataset["customers"].mutate(new_col=5).limit(5)

    # problem should already be detected at extraction time
    with pytest.raises(PipelineStepFailed):
        dest_p.extract(mutated_purchases(fruit_p.dataset()))

    @dlt.transformation()
    def mutated_purchases_with_hints(dataset: dlt.Dataset) -> Any:
        yield dataset["customers"].mutate(new_col=5).limit(5)

    dest_p.run(mutated_purchases_with_hints(fruit_p.dataset()))
    assert load_table_counts(dest_p, "mutated_purchases_with_hints") == {
        "mutated_purchases_with_hints": 5,
    }
