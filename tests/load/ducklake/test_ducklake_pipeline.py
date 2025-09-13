import pytest
import pathlib

import dlt
from dlt.common.destination.reference import TDestinationReferenceArg

from dlt.destinations.impl.ducklake.configuration import (
    DuckLakeCredentials,
    DUCKLAKE_STORAGE_PATTERN,
)
from dlt.destinations import ducklake

from tests.load.utils import ABFS_BUCKET, AWS_BUCKET, GCS_BUCKET, DestinationTestConfiguration
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
        destination: TDestinationReferenceArg = "ducklake"
    else:
        destination = ducklake(credentials=DuckLakeCredentials(catalog=catalog))
    pipeline = dlt.pipeline(
        "destination_defaults", destination=destination, dataset_name="lake_schema", dev_mode=True
    )

    load_info = pipeline.run(
        [{"foo": 1}, {"foo": 2}], table_name="table_foo", loader_file_format="parquet"
    )
    assert_load_info(load_info)

    # test basic data access
    ds = pipeline.dataset()
    assert ds.table_foo["foo"].fetchall() == [(1,), (2,)]

    # test lake location
    expected_location = pathlib.Path(
        TEST_STORAGE_ROOT, DUCKLAKE_STORAGE_PATTERN % pipeline.pipeline_name
    )
    assert expected_location.exists()
    # test dataset in lake
    assert (expected_location / pipeline.dataset_name).exists()

    # test catalog location if applicable
    catalog_location = pipeline.destination_client().config.credentials.catalog.database  # type: ignore
    if "." in catalog_location:
        # it is a file
        assert pathlib.Path(TEST_STORAGE_ROOT, catalog_location).exists()


@pytest.mark.parametrize("bucket_url", (GCS_BUCKET, ABFS_BUCKET, AWS_BUCKET))
def test_all_buckets(bucket_url: str) -> None:
    # create filesystem destination to get configuration
    setup = DestinationTestConfiguration(
        destination_type="filesystem",
        bucket_url=bucket_url,
        supports_merge=False,
    )

    filesystem = setup.setup_pipeline("filesystem_config")
    with filesystem.destination_client() as client:
        destination = ducklake(credentials=DuckLakeCredentials("bucket_cat", storage=client.config))  # type: ignore
        pipeline = dlt.pipeline(
            "destination_defaults",
            destination=destination,
            dataset_name="lake_schema",
            dev_mode=True,
        )

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
            assert remote_url.startswith(bucket_url)
            table_name = job_id.split(".")[0]
            with pipeline.sql_client() as sql:
                with sql.execute_query(
                    f"FROM ducklake_list_files('bucket_cat', '{table_name}');"
                ) as cur:
                    for row in cur.fetchall():
                        assert row[0].startswith(remote_url)
