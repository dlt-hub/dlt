import pytest
import os

from dlt.common.utils import uniq_id
from tests.load.utils import (
    GCS_BUCKET,
    DestinationTestConfiguration,
    destinations_configs,
    AZ_BUCKET,
)
from tests.pipeline.utils import assert_load_info


# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True, bucket_subset=(AZ_BUCKET,), subset=("databricks",)
    ),
    ids=lambda x: x.name,
)
def test_databricks_external_location(destination_config: DestinationTestConfiguration) -> None:
    # do not interfere with state
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    # let the package complete even with failed jobs
    os.environ["RAISE_ON_FAILED_JOBS"] = "false"

    dataset_name = "test_databricks_external_location" + uniq_id()

    from dlt.destinations import databricks, filesystem
    from dlt.destinations.impl.databricks.databricks import DatabricksLoadJob

    abfss_bucket_url = DatabricksLoadJob.ensure_databricks_abfss_url(AZ_BUCKET, "dltdata")
    stage = filesystem(abfss_bucket_url)

    # should load abfss formatted url just fine
    bricks = databricks(is_staging_external_location=False)
    pipeline = destination_config.setup_pipeline(
        "test_databricks_external_location",
        dataset_name=dataset_name,
        destination=bricks,
        staging=stage,
    )
    info = pipeline.run([1, 2, 3], table_name="digits")
    assert_load_info(info)
    # get metrics
    metrics = info.metrics[info.loads_ids[0]][0]
    remote_url = list(metrics["job_metrics"].values())[0].remote_url
    # abfss form was preserved
    assert remote_url.startswith(abfss_bucket_url)

    # should fail on internal config error as external location is not configured
    bricks = databricks(is_staging_external_location=True)
    pipeline = destination_config.setup_pipeline(
        "test_databricks_external_location",
        dataset_name=dataset_name,
        destination=bricks,
        staging=stage,
    )
    info = pipeline.run([1, 2, 3], table_name="digits", **destination_config.run_kwargs)
    assert info.has_failed_jobs is True
    assert (
        "Invalid configuration value detected"
        in pipeline.list_failed_jobs_in_package(info.loads_ids[0])[0].failed_message
    )

    # should fail on non existing stored credentials
    bricks = databricks(is_staging_external_location=False, staging_credentials_name="CREDENTIAL_X")
    pipeline = destination_config.setup_pipeline(
        "test_databricks_external_location",
        dataset_name=dataset_name,
        destination=bricks,
        staging=stage,
    )
    info = pipeline.run([1, 2, 3], table_name="digits", **destination_config.run_kwargs)
    assert info.has_failed_jobs is True
    assert (
        "credential_x" in pipeline.list_failed_jobs_in_package(info.loads_ids[0])[0].failed_message
    )

    # should fail on non existing stored credentials
    # auto stage with regular az:// used
    principal_az_stage = filesystem(destination_name="fsazureprincipal")
    pipeline = destination_config.setup_pipeline(
        "test_databricks_external_location",
        dataset_name=dataset_name,
        destination=bricks,
        staging=principal_az_stage,
    )
    info = pipeline.run([1, 2, 3], table_name="digits", **destination_config.run_kwargs)
    assert info.has_failed_jobs is True
    assert (
        "credential_x" in pipeline.list_failed_jobs_in_package(info.loads_ids[0])[0].failed_message
    )


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True, bucket_subset=(AZ_BUCKET,), subset=("databricks",)
    ),
    ids=lambda x: x.name,
)
def test_databricks_gcs_external_location(destination_config: DestinationTestConfiguration) -> None:
    # do not interfere with state
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    # let the package complete even with failed jobs
    os.environ["RAISE_ON_FAILED_JOBS"] = "false"

    dataset_name = "test_databricks_gcs_external_location" + uniq_id()

    # swap AZ bucket for GCS_BUCKET
    from dlt.destinations import databricks, filesystem

    stage = filesystem(GCS_BUCKET)

    # explicit cred handover should fail
    bricks = databricks()
    pipeline = destination_config.setup_pipeline(
        "test_databricks_gcs_external_location",
        dataset_name=dataset_name,
        destination=bricks,
        staging=stage,
    )
    info = pipeline.run([1, 2, 3], table_name="digits", **destination_config.run_kwargs)
    assert info.has_failed_jobs is True
    assert (
        "You need to use Databricks named credential"
        in pipeline.list_failed_jobs_in_package(info.loads_ids[0])[0].failed_message
    )

    # should fail on non existing stored credentials
    bricks = databricks(is_staging_external_location=False, staging_credentials_name="CREDENTIAL_X")
    pipeline = destination_config.setup_pipeline(
        "test_databricks_external_location",
        dataset_name=dataset_name,
        destination=bricks,
        staging=stage,
    )
    info = pipeline.run([1, 2, 3], table_name="digits", **destination_config.run_kwargs)
    assert info.has_failed_jobs is True
    assert (
        "credential_x" in pipeline.list_failed_jobs_in_package(info.loads_ids[0])[0].failed_message
    )
