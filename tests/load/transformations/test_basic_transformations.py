import pytest

from typing import Any, List

import dlt

from dlt.pipeline.exceptions import PipelineStepFailed

from dlt.transformations.typing import TTransformationType

from tests.load.utils import (
    DestinationTestConfiguration,
)

from tests.load.transformations.utils import (
    row_counts,
    get_job_types,
    transformation_configs,
    load_fruit_dataset,
    setup_transformation_pipelines,
)


@pytest.mark.essential
@pytest.mark.parametrize("transformation_type", ["sql", "python"])
@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(),
    ids=lambda x: x.name,
)
def test_simple_query_transformations(
    transformation_type: TTransformationType, destination_config: DestinationTestConfiguration
) -> None:
    # get pipelines andpopulate fruit pipeline
    fruit_p, dest_p = setup_transformation_pipelines(destination_config, transformation_type)
    load_fruit_dataset(fruit_p)

    @dlt.transformation(transformation_type=transformation_type)
    def copied_purchases(dataset: dlt.Dataset) -> Any:
        return dataset["purchases"].limit(5)

    # transform into transformed dataset
    dest_p.run(copied_purchases(fruit_p.dataset()))

    assert row_counts(dest_p.dataset(), ["copied_purchases"]) == {
        "copied_purchases": 5,
    }

    # verify the right transformation was run
    if transformation_type == "sql":
        assert list(get_job_types(dest_p)["copied_purchases"].keys())[0] == "model"
    else:
        assert list(get_job_types(dest_p)["copied_purchases"].keys())[0] in [
            "parquet",
            "insert_values",
            "csv",
        ]


# NOTE: move to duckdb only transformation tests
@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(),
    ids=lambda x: x.name,
)
def test_simple_python_transformations(destination_config: DestinationTestConfiguration) -> None:
    # get pipelines and populate fruit pipeline
    fruit_p, dest_p = setup_transformation_pipelines(destination_config, "python")
    load_fruit_dataset(fruit_p)

    @dlt.transformation(transformation_type="python")
    def copied_purchases(dataset: dlt.Dataset) -> Any:
        yield from dataset["purchases"].limit(5).iter_arrow(500)

    dest_p.run(copied_purchases(fruit_p.dataset()))

    assert row_counts(dest_p.dataset(), ["copied_purchases"]) == {
        "copied_purchases": 5,
    }

    # verify the right transformation was run
    assert list(get_job_types(dest_p)["copied_purchases"].keys())[0] in [
        "parquet",
        "insert_values",
        "csv",
    ]


# NOTE: move to duckdb only transformation tests
@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(),
    ids=lambda x: x.name,
)
def test_grouped_sql_transformations(destination_config: DestinationTestConfiguration) -> None:
    # get pipelines and populate fruit pipeline
    fruit_p, dest_p = setup_transformation_pipelines(destination_config, "sql")
    load_fruit_dataset(fruit_p)

    @dlt.transformation(transformation_type="sql")
    def copied_purchases(dataset: dlt.Dataset) -> Any:
        return dataset["purchases"].limit(5)

    @dlt.transformation(transformation_type="python")
    def copied_purchases2(dataset: dlt.Dataset) -> Any:
        return dataset["purchases"].limit(7)

    @dlt.source()
    def transformations(dataset: dlt.Dataset) -> List[Any]:
        return [copied_purchases(dataset), copied_purchases2(dataset)]

    dest_p.run(transformations(fruit_p.dataset()))

    assert row_counts(dest_p.dataset(), ["copied_purchases", "copied_purchases2"]) == {
        "copied_purchases": 5,
        "copied_purchases2": 7,
    }

    # verify the right transformation was run
    assert list(get_job_types(dest_p)["copied_purchases"].keys()) == ["model"]
    assert list(get_job_types(dest_p)["copied_purchases2"].keys())[0] in [
        "parquet",
        "insert_values",
        "csv",
    ]


# NOTE: move to duckdb only transformation tests
@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(),
    ids=lambda x: x.name,
)
def test_replace_sql_transformations(destination_config: DestinationTestConfiguration) -> None:
    # get pipelines and populate fruit pipeline
    fruit_p, dest_p = setup_transformation_pipelines(destination_config, "sql")
    load_fruit_dataset(fruit_p)

    @dlt.transformation(write_disposition="replace", transformation_type="sql")
    def copied_purchases(dataset: dlt.Dataset) -> Any:
        return dataset["purchases"].limit(5)

    # transform into same dataset
    dest_p.run(copied_purchases(fruit_p.dataset()))

    assert row_counts(dest_p.dataset(), ["copied_purchases"]) == {
        "copied_purchases": 5,
    }

    # overwrite with different setting
    @dlt.transformation(
        write_disposition="replace",
        table_name="copied_purchases",
        transformation_type="sql",
    )
    def copied_purchases_updated(dataset: dlt.Dataset) -> Any:
        return dataset["purchases"].limit(3)

    # transform into same dataset
    dest_p.run(copied_purchases_updated(fruit_p.dataset()))
    assert row_counts(dest_p.dataset(), ["copied_purchases"]) == {
        "copied_purchases": 3,
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
    fruit_p, dest_p = setup_transformation_pipelines(destination_config, "sql")
    load_fruit_dataset(fruit_p)

    @dlt.transformation(write_disposition="append", transformation_type="sql")
    def copied_purchases(dataset: dlt.Dataset) -> Any:
        return dataset["purchases"].limit(5)

    # transform into same dataset
    dest_p.run(copied_purchases(fruit_p.dataset()))

    assert row_counts(dest_p.dataset(), ["copied_purchases"]) == {
        "copied_purchases": 5,
    }

    @dlt.transformation(
        write_disposition="append", table_name="copied_purchases", transformation_type="sql"
    )
    def copied_table_updated(dataset: dlt.Dataset) -> Any:
        return dataset["purchases"].limit(7)

    # transform into same dataset
    dest_p.run(copied_table_updated(fruit_p.dataset()))

    assert row_counts(dest_p.dataset(), ["copied_purchases"]) == {
        "copied_purchases": 12,
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
    fruit_p, dest_p = setup_transformation_pipelines(destination_config, "sql")
    load_fruit_dataset(fruit_p)

    @dlt.transformation(transformation_type="sql")
    def mutated_purchases(dataset: dlt.Dataset) -> Any:
        return dataset["purchases"].mutate(new_col=5).limit(5)

    # problem should already be detected at extraction time
    with pytest.raises(PipelineStepFailed):
        dest_p.extract(mutated_purchases(fruit_p.dataset()))

    @dlt.transformation(transformation_type="sql")
    def mutated_purchases_with_hints(dataset: dlt.Dataset) -> Any:
        return dataset["purchases"].mutate(new_col=5).limit(5)

    dest_p.run(mutated_purchases_with_hints(fruit_p.dataset()))
    assert row_counts(dest_p.dataset(), ["mutated_purchases_with_hints"]) == {
        "mutated_purchases_with_hints": 5,
    }
