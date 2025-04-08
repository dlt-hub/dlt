import pytest
import dlt
import os
from typing import Any, cast

import ibis
import ibis.expr.types as ir
import pandas as pd
from dlt import Pipeline
from dlt.common import Decimal
from dlt.common.schema.schema import Schema
from dlt.common.storages.file_storage import FileStorage
from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    GCS_BUCKET,
    SFTP_BUCKET,
    MEMORY_BUCKET,
)
from dlt.destinations import filesystem
from tests.utils import clean_test_storage
from tests.load.utils import drop_pipeline_data

from dlt.destinations.dataset.ibis_relation import DltBackend


def _total_records(p: Pipeline) -> int:
    """how many records to load for a given pipeline"""
    if p.destination.destination_type == "dlt.destinations.bigquery":
        return 80
    elif p.destination.destination_type == "dlt.destinations.mssql":
        return 1000
    return 3000


# this also disables autouse_test_storage on function level which destroys some tests here
@pytest.fixture(scope="session")
def autouse_test_storage() -> FileStorage:
    return clean_test_storage()


@pytest.fixture(scope="session")
def populated_pipeline(request, autouse_test_storage) -> Any:
    """fixture that returns a pipeline object populated with the example data"""

    destination_config = cast(DestinationTestConfiguration, request.param)

    if (
        destination_config.file_format not in ["parquet", "jsonl"]
        and destination_config.destination_type == "filesystem"
    ):
        pytest.skip(
            "Test only works for jsonl and parquet on filesystem destination, given:"
            f" {destination_config.file_format}"
        )

    pipeline = destination_config.setup_pipeline(
        "read_pipeline", dataset_name="read_test", dev_mode=True
    )
    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "700"
    total_records = _total_records(pipeline)

    @dlt.source()
    def source():
        @dlt.resource(
            table_format=destination_config.table_format,
            write_disposition="replace",
            columns={
                "id": {"data_type": "bigint"},
                # we add a decimal with precision to see wether the hints are preserved
                "decimal": {"data_type": "decimal", "precision": 10, "scale": 3},
                "other_decimal": {"data_type": "decimal", "precision": 12, "scale": 3},
            },
        )
        def items():
            yield from [
                {
                    "id": i,
                    "children": [{"id": i + 100}, {"id": i + 1000}],
                    "decimal": Decimal("10.433"),
                    "other_decimal": Decimal("10.433"),
                }
                for i in range(total_records)
            ]

        @dlt.resource(
            table_format=destination_config.table_format,
            write_disposition="replace",
            columns={
                "id": {"data_type": "bigint"},
                "double_id": {"data_type": "bigint"},
                "di_decimal": {"data_type": "decimal", "precision": 7, "scale": 3},
            },
        )
        def double_items():
            yield from [
                {
                    "id": i,
                    "double_id": i * 2,
                    "di_decimal": Decimal("10.433"),
                }
                for i in range(total_records)
            ]

        return [items, double_items]

    # run source
    s = source()
    pipeline.run(s, loader_file_format=destination_config.file_format)
    # create a second schema in the pipeline
    # NOTE: that generates additional load package and then another one for the state
    # NOTE: "aleph" schema is now the newest schema in the dataset and we assume that later in the tests
    # TODO: we need some kind of idea for multi-schema datasets
    pipeline.run([1, 2, 3], table_name="digits", schema=Schema("aleph"))

    # in case of delta on gcs we use the s3 compat layer for reading
    # for writing we still need to use the gc authentication, as delta_rs seems to use
    # methods on the s3 interface that are not implemented by gcs
    if destination_config.bucket_url == GCS_BUCKET and destination_config.table_format == "delta":
        gcp_bucket = filesystem(
            GCS_BUCKET.replace("gs://", "s3://"), destination_name="filesystem_s3_gcs_comp"
        )
        access_pipeline = destination_config.setup_pipeline(
            "read_pipeline", dataset_name="read_test", destination=gcp_bucket
        )

        pipeline.destination = access_pipeline.destination

    # return pipeline to test
    yield pipeline

    # NOTE: we need to drop pipeline data here since we are keeping the pipelines around for the whole module
    drop_pipeline_data(pipeline)


# NOTE: we collect all destination configs centrally, this way the session based
# pipeline population per fixture setup will work and save a lot of time
configs = destinations_configs(
    default_sql_configs=True,
    all_buckets_filesystem_configs=False,
    table_format_filesystem_configs=False,
    bucket_exclude=[SFTP_BUCKET, MEMORY_BUCKET],
)
duckdb_conf = [c for c in configs if c[0][0].destination_type=="duckdb" and c[0][0].file_format is None]


@pytest.mark.no_load
@pytest.mark.essential
def test_instantiate_backend():
    DltBackend()


# TODO test for all destinations
@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    duckdb_conf,
    indirect=True,
    ids=lambda x: x.name,
)
def test_connect_to_backend(populated_pipeline: Pipeline):
    backend1 = DltBackend.from_dataset(populated_pipeline.dataset())
    backend2 = DltBackend.from_pipeline(populated_pipeline)

    assert isinstance(backend1, DltBackend)
    assert isinstance(backend2, DltBackend)
    assert backend1 is not backend2


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    duckdb_conf,
    indirect=True,
    ids=lambda x: x.name,
)
def test_list_tables(populated_pipeline: Pipeline):
    backend = DltBackend.from_pipeline(populated_pipeline)
    expected_table_names = [
        '_dlt_version', 
        '_dlt_loads',
        'items',
        'double_items',
        '_dlt_pipeline_state',
        'items__children'
    ]
    
    assert backend.list_tables() == expected_table_names


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    duckdb_conf,
    indirect=True,
    ids=lambda x: x.name,
)
def test_get_schema(populated_pipeline: Pipeline):
    backend = DltBackend.from_pipeline(populated_pipeline)
    expected_schema = ibis.Schema(
        {
            "id": ibis.dtype("int64", nullable=True),
            "decimal": ibis.dtype("decimal", nullable=True),
            "other_decimal": ibis.dtype("decimal", nullable=True),
            "_dlt_load_id": ibis.dtype("string", nullable=False),
            "_dlt_id": ibis.dtype("string", nullable=False),
        }  # type: ignore
    )

    ibis_schema = backend.get_schema("items")
    
    assert expected_schema.equals(ibis_schema)


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    duckdb_conf,
    indirect=True,
    ids=lambda x: x.name,
)
def test_get_bound_table(populated_pipeline: Pipeline):
    backend = DltBackend.from_pipeline(populated_pipeline)
    expected_schema = ibis.Schema(
        {
            "id": ibis.dtype("int64", nullable=True),
            "decimal": ibis.dtype("decimal", nullable=True),
            "other_decimal": ibis.dtype("decimal", nullable=True),
            "_dlt_load_id": ibis.dtype("string", nullable=False),
            "_dlt_id": ibis.dtype("string", nullable=False),
        }  # type: ignore
    )

    table = backend.table("items")
    
    assert isinstance(table, ir.Table)
    assert table.schema().equals(expected_schema)


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    duckdb_conf,
    indirect=True,
    ids=lambda x: x.name,
)
def test_execute_expression(populated_pipeline: Pipeline):
    backend = DltBackend.from_pipeline(populated_pipeline)
    expected_schema = ibis.Schema(
        {
            "_dlt_id": ibis.dtype("string", nullable=False),
            "id": ibis.dtype("int64", nullable=True),
        }  # type: ignore
    )

    table = backend.table("items")
    expr = table.select("_dlt_id", "id")
    table2 = backend.execute(expr)
    
    assert isinstance(table2, pd.DataFrame)
    assert expr.schema().equals(expected_schema)
    assert set(table2.columns) == set(expected_schema.names)


@pytest.mark.no_load
@pytest.mark.essential
@pytest.mark.parametrize(
    "populated_pipeline",
    duckdb_conf,
    indirect=True,
    ids=lambda x: x.name,
)
def test_user_workflow(populated_pipeline: Pipeline):
    expected_columns = ["_dlt_id", "id"]

    dataset = populated_pipeline.dataset()
    con = dataset.ibis()
    table = con.table("items")
    result = table.select("_dlt_id", "id").execute()
    
    assert isinstance(result, pd.DataFrame)
    assert set(result.columns) == set(expected_columns)