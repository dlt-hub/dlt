import pytest

from typing import Any

import dlt, os, sys

from dlt.common.destination.dataset import SupportsReadableDataset
from dlt.common.destination.dataset import SupportsReadableRelation
from tests.pipeline.utils import load_table_counts
from dlt.extract.hints import SqlModel

from tests.load.utils import DestinationTestConfiguration
from tests.load.transformations.utils import (
    transformation_configs,
    setup_transformation_pipelines,
    get_job_types,
)

from dlt.extract.hints import make_hints

from dlt.sources._single_file_templates.fruitshop_pipeline import (
    fruitshop as fruitshop_source,
)


@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(only_duckdb=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("always_materialize", [True, False])
@pytest.mark.parametrize("transformation_type", ["sql", "relation"])
def test_simple_query_transformations(
    destination_config: DestinationTestConfiguration,
    always_materialize: bool,
    transformation_type: str,
) -> None:
    # get pipelines and populate fruit pipeline
    fruit_p, dest_p = setup_transformation_pipelines(destination_config)
    fruit_p.run(fruitshop_source())

    if transformation_type == "sql":

        @dlt.transformation()
        def copied_purchases(dataset: SupportsReadableDataset[Any]) -> Any:
            yield """SELECT * FROM purchases LIMIT 3"""

    elif transformation_type == "relation":

        @dlt.transformation()
        def copied_purchases(dataset: SupportsReadableDataset[Any]) -> Any:
            yield dataset["purchases"].limit(3)

    # transform into transformed dataset
    os.environ["ALWAYS_MATERIALIZE"] = str(always_materialize)
    dest_p.run(copied_purchases(fruit_p.dataset()))

    assert load_table_counts(dest_p, "copied_purchases") == {
        "copied_purchases": 3,
    }

    # all transformations are sql, except for filesystem destination
    assert get_job_types(dest_p) == {
        "copied_purchases": {"model": 1} if not always_materialize else {"parquet": 1}
    }


@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(only_duckdb=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "always_materialize", [False]
)  # , True]) # TODO: reenable once we have figured this out
def test_transformations_with_supplied_hints(
    destination_config: DestinationTestConfiguration, always_materialize: bool
) -> None:
    fruit_p, dest_p = setup_transformation_pipelines(destination_config)

    s = fruitshop_source()
    s.inventory.apply_hints(columns={"price": {"precision": 10, "scale": 2}})
    fruit_p.run(s)

    os.environ["ALWAYS_MATERIALIZE"] = str(always_materialize)

    assert fruit_p.default_schema.tables["inventory"]["columns"]["price"]["precision"] == 10
    assert fruit_p.default_schema.tables["inventory"]["columns"]["price"]["scale"] == 2

    # we can now transform this table twice, one with changed hints and once with the original hints
    @dlt.transformation()
    def inventory_original(dataset: SupportsReadableDataset[Any]) -> Any:
        yield dataset["inventory"]

    @dlt.transformation()
    def inventory_more_precise(dataset: SupportsReadableDataset[Any]) -> Any:
        hints = make_hints(columns=[{"name": "price", "precision": 20, "scale": 2}])
        yield dlt.mark.with_hints(dataset["inventory"], hints=hints)

    dest_p.run([inventory_original(fruit_p.dataset()), inventory_more_precise(fruit_p.dataset())])

    assert dest_p.default_schema.tables["inventory_original"]["columns"]["price"]["precision"] == 10
    assert dest_p.default_schema.tables["inventory_original"]["columns"]["price"]["scale"] == 2
    assert (
        dest_p.default_schema.tables["inventory_more_precise"]["columns"]["price"]["precision"]
        == 20
    )
    assert dest_p.default_schema.tables["inventory_more_precise"]["columns"]["price"]["scale"] == 2


@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(only_duckdb=True),
    ids=lambda x: x.name,
)
def test_extract_without_source_name_or_pipeline(
    destination_config: DestinationTestConfiguration,
) -> None:
    # get pipelines and populate fruit pipeline
    fruit_p, dest_p = setup_transformation_pipelines(destination_config)
    fruit_p.run(fruitshop_source())

    @dlt.transformation()
    def buffer_size_test(dataset: SupportsReadableDataset[Any]) -> Any:
        yield dataset["customers"]

    # transformations switch to model extraction
    fruit_p.deactivate()
    model_rows = list(buffer_size_test(fruit_p.dataset()))
    assert len(model_rows) == 1
    assert isinstance(model_rows[0], SupportsReadableRelation)


@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(only_duckdb=True),
    ids=lambda x: x.name,
)
def test_extract_without_destination(destination_config: DestinationTestConfiguration) -> None:
    fruit_p, dest_p = setup_transformation_pipelines(destination_config)
    fruit_p.run(fruitshop_source())

    @dlt.transformation()
    def extract_test(dataset: SupportsReadableDataset[Any]) -> Any:
        yield dataset["customers"]

    pipeline_no_destination = dlt.pipeline(pipeline_name="no_destination")
    pipeline_no_destination._destination = None
    extract_info = pipeline_no_destination.extract(extract_test(fruit_p.dataset()))

    # there is no destination, so we should have arrow extraction
    found_job = False
    for job in extract_info.load_packages[0].jobs["new_jobs"]:
        if job.job_file_info.table_name == "extract_test":
            assert job.job_file_info.file_format == "model"
            found_job = True
    assert found_job


@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(only_duckdb=True),
    ids=lambda x: x.name,
)
def test_materializable_sql_model(destination_config: DestinationTestConfiguration) -> None:
    fruit_p, dest_p = setup_transformation_pipelines(destination_config)
    fruit_p.run(fruitshop_source())

    @dlt.transformation()
    def materializable_sql_model(dataset: SupportsReadableDataset[Any]) -> Any:
        yield "SELECT id, name FROM customers"

    model = list(materializable_sql_model(fruit_p.dataset()))[0]
    assert model.arrow().column_names == ["id", "name"]
    assert model.compute_hints()["columns"] == {
        "id": {"name": "id", "data_type": "bigint", "nullable": False},
        "name": {"name": "name", "x-annotation-pii": True, "data_type": "text", "nullable": True},
    }


@pytest.mark.skipif(sys.version_info < (3, 10), reason="Ibis Requires Python 3.10 or higher")
@pytest.mark.parametrize(
    "destination_config",
    transformation_configs(only_duckdb=True),
    ids=lambda x: x.name,
)
def test_ibis_unbound_table_transformation(
    destination_config: DestinationTestConfiguration,
) -> None:
    fruit_p, dest_p = setup_transformation_pipelines(destination_config)
    fruit_p.run(fruitshop_source())

    @dlt.transformation()
    def materializable_sql_model(dataset: SupportsReadableDataset[Any]) -> Any:
        purchases = dataset.table("purchases", table_type="ibis")
        customers = dataset.table("customers", table_type="ibis")
        yield purchases.join(customers, purchases.customer_id == customers.id)[
            ["id", "customer_id", "inventory_id", "quantity", "name"]
        ]

    model = list(materializable_sql_model(fruit_p.dataset()))[0]
    assert model.arrow().column_names == [
        "id",
        "customer_id",
        "inventory_id",
        "quantity",
        "name",
    ]
    assert model.arrow().shape == (3, 5)
