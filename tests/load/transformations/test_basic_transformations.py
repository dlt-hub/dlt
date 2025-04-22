import pytest

from typing import Any, List, Dict

import dlt
from dlt.destinations import duckdb

from dlt.common.destination.dataset import SupportsReadableDataset
from dlt.destinations.exceptions import DatabaseUndefinedRelation

from dlt.transformations.reference import TTransformationType
from dlt.pipeline.exceptions import PipelineStepFailed

from tests.load.utils import (
    FILE_BUCKET,
    destinations_configs,
    SFTP_BUCKET,
    MEMORY_BUCKET,
    DestinationTestConfiguration,
)

# from dlt.transformations.exceptions import MaterializationTypeMismatch
from tests.load.transformations.utils import (
    row_counts,
    transformation_configs,
    load_fruit_dataset,
    EXPECTED_FRUIT_ROW_COUNTS,
)


def _get_job_types(p: dlt.Pipeline, stage: str = "loaded") -> Dict[str, Dict[str, Any]]:
    """
    gets a list of loaded jobs by type for each table
    this is useful for testing to check that the correct transformations were used
    """

    jobs = []
    if stage == "loaded":
        jobs = p.last_trace.last_load_info.load_packages[0].jobs["completed_jobs"]
    elif stage == "extracted":
        jobs = p.last_trace.last_extract_info.load_packages[0].jobs["new_jobs"]

    tables: Dict[str, Dict[str, Any]] = {}

    for j in jobs:
        file_format = j.job_file_info.file_format
        table_name = j.job_file_info.table_name
        if table_name.startswith("_dlt"):
            continue
        tables.setdefault(table_name, {})[file_format] = (
            tables.get(table_name, {}).get(file_format, 0) + 1
        )

    return tables


@pytest.mark.parametrize("transformation_type", ["sql", "python"])
@pytest.mark.parametrize(
    "destination_config",
    transformation_configs,
    ids=lambda x: x.name,
)
def test_simple_query_transformations(
    transformation_type: TTransformationType, destination_config: DestinationTestConfiguration
) -> None:
    if destination_config.destination_type == "filesystem" and transformation_type == "sql":
        pytest.skip("Filesystem destination does not support sql transformations")

    # setup incoming data and transformed data pipelines
    fruit_p = destination_config.setup_pipeline(
        "fruit_pipeline", dataset_name="source", use_single_dataset=False, dev_mode=True
    )
    dest_p = destination_config.setup_pipeline(
        "fruit_pipeline",
        dataset_name="transformed",
        use_single_dataset=False,
        # for filesystem destination we load to a duckdb on python transformations
        destination="duckdb" if destination_config.destination_type == "filesystem" else None,
        dev_mode=True,
    )

    # populate fruit pipeline
    load_fruit_dataset(fruit_p)

    @dlt.transformation(transformation_type=transformation_type)
    def copied_purchases(dataset: SupportsReadableDataset[Any]) -> Any:
        return dataset["purchases"].limit(5)

    # transform into transformed dataset
    dest_p.run(copied_purchases(fruit_p.dataset()))

    assert row_counts(dest_p.dataset(), ["copied_purchases"]) == {
        "copied_purchases": 5,
    }

    # verify the right transformation was run
    item_format = (
        "parquet"
        if destination_config.destination_type not in ["mssql", "synapse"]
        else "insert_values"
    )
    assert _get_job_types(dest_p) == {
        "copied_purchases": {"model": 1} if transformation_type == "sql" else {item_format: 1}
    }


def test_simple_python_transformations(
    fruit_p: dlt.Pipeline,
) -> None:
    @dlt.transformation(transformation_type="python")
    def copied_purchases(dataset: SupportsReadableDataset[Any]) -> Any:
        yield from dataset["purchases"].limit(5).iter_arrow(500)

    # transform into same dataset
    fruit_p.run(copied_purchases(fruit_p.dataset()))

    assert row_counts(fruit_p.dataset(), ["purchases", "copied_purchases"]) == {
        "purchases": EXPECTED_FRUIT_ROW_COUNTS["purchases"],
        "copied_purchases": 5,
    }

    # verify the right transformation was run
    assert _get_job_types(fruit_p) == {"copied_purchases": {"parquet": 1}}


@pytest.mark.parametrize("transformation_type", ["sql", "python", "mixed"])
def test_grouped_sql_transformations(
    fruit_p: dlt.Pipeline, transformation_type: TTransformationType
) -> None:
    @dlt.transformation(
        transformation_type=transformation_type if transformation_type != "mixed" else "sql"
    )
    def copied_purchases(dataset: SupportsReadableDataset[Any]) -> Any:
        return dataset["purchases"].limit(5)

    @dlt.transformation(
        transformation_type=transformation_type if transformation_type != "mixed" else "python"
    )
    def copied_purchases2(dataset: SupportsReadableDataset[Any]) -> Any:
        return dataset["purchases"].limit(7)

    @dlt.source()
    def transformations(dataset: SupportsReadableDataset[Any]) -> List[Any]:
        return [copied_purchases(dataset), copied_purchases2(dataset)]

    fruit_p.run(transformations(fruit_p.dataset()))

    assert row_counts(
        fruit_p.dataset(), ["purchases", "copied_purchases", "copied_purchases2"]
    ) == {
        "purchases": EXPECTED_FRUIT_ROW_COUNTS["purchases"],
        "copied_purchases": 5,
        "copied_purchases2": 7,
    }

    # verify the right transformation was run
    assert _get_job_types(fruit_p) == {
        "copied_purchases": (
            {"model": 1} if transformation_type in ["sql", "mixed"] else {"parquet": 1}
        ),
        "copied_purchases2": {"model": 1} if transformation_type == "sql" else {"parquet": 1},
    }


@pytest.mark.parametrize("transformation_type", ["sql", "python"])
def test_replace_sql_transformations(
    fruit_p: dlt.Pipeline, transformation_type: TTransformationType
) -> None:
    @dlt.transformation(write_disposition="replace", transformation_type=transformation_type)
    def copied_purchases(dataset: SupportsReadableDataset[Any]) -> Any:
        return dataset["purchases"].limit(5)

    # transform into same dataset
    fruit_p.run(copied_purchases(fruit_p.dataset()))

    assert row_counts(fruit_p.dataset(), ["purchases", "copied_purchases"]) == {
        "purchases": EXPECTED_FRUIT_ROW_COUNTS["purchases"],
        "copied_purchases": 5,
    }

    # overwrite with different setting
    @dlt.transformation(
        write_disposition="replace",
        table_name="copied_purchases",
        transformation_type=transformation_type,
    )
    def copied_purchases_updated(dataset: SupportsReadableDataset[Any]) -> Any:
        return dataset["purchases"].limit(3)

    # transform into same dataset
    fruit_p.run(copied_purchases_updated(fruit_p.dataset()))

    assert row_counts(fruit_p.dataset(), ["purchases", "copied_purchases"]) == {
        "purchases": EXPECTED_FRUIT_ROW_COUNTS["purchases"],
        "copied_purchases": 3,
    }


@pytest.mark.parametrize("transformation_type", ["sql", "python"])
def test_append_sql_transformations(
    fruit_p: dlt.Pipeline, transformation_type: TTransformationType
) -> None:
    @dlt.transformation(write_disposition="append", transformation_type=transformation_type)
    def copied_purchases(dataset: SupportsReadableDataset[Any]) -> Any:
        return dataset["purchases"].limit(5)

    # transform into same dataset
    fruit_p.run(copied_purchases(fruit_p.dataset()))

    assert row_counts(fruit_p.dataset(), ["purchases", "copied_purchases"]) == {
        "purchases": EXPECTED_FRUIT_ROW_COUNTS["purchases"],
        "copied_purchases": 5,
    }

    @dlt.transformation(
        write_disposition="append", table_name="copied_purchases", transformation_type="sql"
    )
    def copied_table_updated(dataset: SupportsReadableDataset[Any]) -> Any:
        return dataset["purchases"].limit(7)

    # transform into same dataset
    fruit_p.run(copied_table_updated(fruit_p.dataset()))

    assert row_counts(fruit_p.dataset(), ["purchases", "copied_purchases"]) == {
        "purchases": EXPECTED_FRUIT_ROW_COUNTS["purchases"],
        "copied_purchases": 12,
    }


@pytest.mark.parametrize("transformation_type", ["sql", "python"])
def test_transform_to_other_dataset_same_dest(
    fruit_p: dlt.Pipeline, transformation_type: TTransformationType
) -> None:
    @dlt.transformation(transformation_type=transformation_type)
    def copied_purchases(dataset: SupportsReadableDataset[Any]) -> Any:
        return dataset["purchases"].limit(5)

    pipeline_2 = dlt.pipeline(
        pipeline_name=fruit_p.pipeline_name,
        destination=duckdb(credentials="fruits.db"),
        dataset_name=fruit_p.dataset_name + "_clone",
        dev_mode=True,
    )

    # transform into other dataset same destination
    pipeline_2.run(copied_purchases(fruit_p.dataset()))
    transformed_dataset = pipeline_2.dataset()

    # only exists in other dataset
    with pytest.raises(DatabaseUndefinedRelation):
        row_counts(fruit_p.dataset(), ["copied_purchases"])

    assert row_counts(transformed_dataset, ["copied_purchases"]) == {
        "copied_purchases": 5,
    }


@pytest.mark.skip(
    reason=(
        "TODO: lineage got better, so I need a better way to figure out an unknown column type for"
        " this test. @zilto says all types are always known now, so maybe it is not possible."
    )
)
def test_sql_transformation_with_unknown_column_types(
    fruit_p: dlt.Pipeline,
) -> None:
    @dlt.transformation(transformation_type="sql")
    def mutated_purchases(dataset: SupportsReadableDataset[Any]) -> Any:
        return dataset["purchases"].mutate(new_col=5).limit(5)

    # problem should already be detected at extraction time
    with pytest.raises(PipelineStepFailed):
        fruit_p.extract(mutated_purchases(fruit_p.dataset()))

    @dlt.transformation(transformation_type="sql", columns={"new_col": {"data_type": "double"}})
    def mutated_purchases_with_hints(dataset: SupportsReadableDataset[Any]) -> Any:
        return dataset["purchases"].mutate(new_col=5).limit(5)

    fruit_p.run(mutated_purchases_with_hints(fruit_p.dataset()))
    assert row_counts(fruit_p.dataset(), ["purchases", "mutated_purchases_with_hints"]) == {
        "purchases": EXPECTED_FRUIT_ROW_COUNTS["purchases"],
        "mutated_purchases_with_hints": 5,
    }


# def test_view_creation(fruit_p: dlt.Pipeline) -> None:
#     """only runs on sql..."""

#     @dlt.transformation(materialization="view", transformation_type="sql")
#     def copied_view(dataset: SupportsReadableDataset[Any]) -> Any:
#         return dataset["purchases"].limit(5)

#     @dlt.transformation(materialization="table", transformation_type="sql")
#     def copied_table(dataset: SupportsReadableDataset[Any]) -> Any:
#         return dataset["purchases"].limit(5)

#     @dlt.source()
#     def transformations(dataset: SupportsReadableDataset[Any]) -> List[Any]:
#         return [copied_table(dataset), copied_view(dataset)]

#     fruit_p.run(transformations(fruit_p.dataset()))
#     dest_dataset = fruit_p.dataset()

#     # one table and one view
#     assert dest_dataset(
#         "SELECT table_name FROM information_schema.tables where "
#         + "table_name = 'copied_table' AND table_type = 'BASE TABLE'"
#     ).fetchall() == [("copied_table",)]
#     assert dest_dataset(
#         "SELECT table_name FROM information_schema.tables where "
#         + "table_name = 'copied_view' AND table_type = 'VIEW'"
#     ).fetchall() == [("copied_view",)]

#     # they don't exist vice versa
#     assert (
#         dest_dataset(
#             "SELECT table_name FROM information_schema.tables where "
#             + "table_name = 'copied_table' AND table_type = 'VIEW'"
#         ).fetchall()
#         == []
#     )
#     assert (
#         dest_dataset(
#             "SELECT table_name FROM information_schema.tables where "
#             + "table_name = 'copied_view' AND table_type = 'BASE TABLE'"
#         ).fetchall()
#         == []
#     )


# def test_view_append_exception(
#     populated_dataset: WritableDataset, pipeline_1: dlt.Pipeline
# ) -> None:
#     @dlt.transformation(
#         write_disposition="append", materialization="view", transformation_type="sql"
#     )
#     def copied_table(dataset: SupportsReadableDataset[Any]) -> Any:
#         return dataset["example_table"].limit(5)

#     with pytest.raises(MaterializationTypeMismatch):
#         list(copied_table(populated_dataset)())
