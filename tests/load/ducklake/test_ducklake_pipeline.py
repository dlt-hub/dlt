import pytest
import pathlib

import dlt
from dlt.common.destination.reference import TDestinationReferenceArg

from dlt.common.utils import uniq_id
from dlt.destinations.impl.ducklake.configuration import (
    DuckLakeCredentials,
    DUCKLAKE_STORAGE_PATTERN,
)
from dlt.destinations import ducklake

from dlt.pipeline.exceptions import PipelineStepFailed
from tests.load.utils import (
    ABFS_BUCKET,
    AWS_BUCKET,
    GCS_BUCKET,
    DestinationTestConfiguration,
    destinations_configs,
)
from tests.pipeline.utils import assert_load_info
from tests.utils import TEST_STORAGE_ROOT


@pytest.mark.parametrize(
    "catalog",
    (
        None,
        "sqlite:///catalog.sqlite",
        "duckdb:///catalog.duckdb",
        "postgres://loader:loader@localhost:5432/dlt_data",
    ),
)
def test_all_catalogs(catalog: str) -> None:
    """Check that catalog and storage are materialized at the right
    location and properly derive their name from the pipeline name.
    """
    if catalog is None:
        # use destination alias
        ducklake_name = "ducklake"
        destination: TDestinationReferenceArg = "ducklake"
    else:
        # pick random catalog name so we isolate via postgres schema
        ducklake_name = "ducklake_" + uniq_id(4)
        destination = ducklake(
            credentials=DuckLakeCredentials(ducklake_name=ducklake_name, catalog=catalog)
        )
    pipeline = dlt.pipeline(
        "destination_defaults", destination=destination, dataset_name="lake_schema", dev_mode=True
    )

    try:
        load_info = pipeline.run(
            [{"foo": 1}, {"foo": 2}], table_name="table_foo", loader_file_format="parquet"
        )
    except PipelineStepFailed as conn_ex:
        # skip test gracefully if local postgres is not running. allows to run on ci when only
        # remote postgres is available. TODO: allow to use configured catalog if present
        if "postgres" not in catalog or "localhost" not in str(conn_ex):
            raise
        pytest.skip(f"Requires localhost postgres running: {catalog}")

    assert_load_info(load_info)

    # test basic data access
    ds = pipeline.dataset()
    assert ds.table_foo["foo"].fetchall() == [(1,), (2,)]

    # test lake location
    expected_location = pathlib.Path(TEST_STORAGE_ROOT, DUCKLAKE_STORAGE_PATTERN % ducklake_name)
    assert expected_location.exists()
    # test dataset in lake
    assert (expected_location / pipeline.dataset_name).exists()

    # test catalog location if applicable
    catalog_location = pipeline.destination_client().config.credentials.catalog.database  # type: ignore
    if "." in catalog_location:
        # it is a file
        assert pathlib.Path(TEST_STORAGE_ROOT, catalog_location).exists()


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        all_buckets_filesystem_configs=True, bucket_subset=(GCS_BUCKET, ABFS_BUCKET, AWS_BUCKET)
    ),
    ids=lambda x: x.name,
)
def test_all_buckets(destination_config: DestinationTestConfiguration) -> None:
    filesystem = destination_config.setup_pipeline("filesystem_config")
    destination = ducklake(
        credentials=DuckLakeCredentials(
            "bucket_cat", storage=filesystem.destination_client().config  # type: ignore
        )
    )

    pipeline = dlt.pipeline(
        "destination_defaults",
        destination=destination,
        dataset_name="lake_schema",
        dev_mode=True,
    )

    import duckdb

    # set per thread catalog option
    with pipeline.sql_client() as client:
        con: duckdb.DuckDBPyConnection = client.native_connection
        con.sql("CALL bucket_cat.set_option('target_file_size', '1GB')")

    load_info = pipeline.run(
        [{"foo": 1}, {"foo": 2}], table_name="table_foo", loader_file_format="parquet"
    )
    assert_load_info(load_info)
    # get data
    assert pipeline.dataset().table_foo["foo"].fetchall() == [(1,), (2,)]

    # make sure that data is really in the bucket
    all_metrics = load_info.metrics[load_info.loads_ids[0]][0]
    for job_id, metrics in all_metrics["job_metrics"].items():
        remote_url = metrics.remote_url
        print(remote_url)
        assert remote_url.startswith(destination_config.bucket_url)
        table_name = job_id.split(".")[0]
        with pipeline.sql_client() as sql:
            with sql.execute_query(
                f"FROM ducklake_list_files('bucket_cat', '{table_name}');"
            ) as cur:
                for row in cur.fetchall():
                    assert row[0].startswith(remote_url)

    # verify options
    with pipeline.sql_client() as client:
        opt_val = client.execute_sql(
            "SELECT value FROM bucket_cat.options() WHERE option_name = 'target_file_size'"
        )
        assert opt_val[0][0] == "1000000000"
        print(client.execute_sql("CALL bucket_cat.merge_adjacent_files()"))
