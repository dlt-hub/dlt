from typing import Tuple, List, Generator, Optional

from mypy_boto3_lakeformation import LakeFormationClient
from mypy_boto3_lakeformation.type_defs import (
    GetResourceLFTagsResponseTypeDef,
    GrantPermissionsRequestTypeDef,
)
import pytest
import boto3

import dlt
from dlt.destinations import filesystem, athena
from dlt.common.utils import uniq_id
from dlt.destinations.impl.athena.configuration import LakeformationConfig

IAM_ARN = boto3.client("sts").get_caller_identity()["Arn"]
ATHENA_WORKGROUP = dlt.config.get("tests.athena_workgroup", str)
QUERY_RESULT_BUCKET = dlt.config.get("tests.query_results_bucket", str)

# These tests require Lakeformation permissions to be able to run.
# The tests have only been run as lakeformation admin so far, since the fixtures require
# permissions to create new tags, and grant lakeformation permissions to the current role

@pytest.fixture(scope="session", autouse=True)
def check_lakeformation_enabled(lf_client: LakeFormationClient) -> None:
    """Check if LakeFormation is enabled in the AWS account. Skip tests if it's not."""
    try:
        # Try to get LakeFormation settings
        settings = lf_client.get_data_lake_settings()

        # Check if data lake settings exist and are properly configured
        if not settings.get("DataLakeSettings"):
            pytest.skip("LakeFormation is not properly configured in this AWS account")

    except lf_client.exceptions.AccessDeniedException:
        pytest.skip(
            "No access to LakeFormation. Please ensure LakeFormation is enabled and you have proper permissions"
        )
    except Exception as e:
        pytest.skip(f"Failed to verify LakeFormation status: {str(e)}")


@pytest.fixture(scope="session")
def lf_client() -> LakeFormationClient:
    return boto3.client("lakeformation")


@pytest.fixture(scope="session")
def s3_bucket(lf_client) -> Generator[str, None, None]:
    """Create a new S3 bucket and register it in lakeformation."""
    s3_client = boto3.client("s3")
    bucket_name = "dlt-lakeformation-test-bucket-" + uniq_id(8)

    try:
        # Create bucket in the same region as Athena
        s3_client.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-north-1"}
        )

        # Wait for bucket to be available
        s3_client.get_waiter("bucket_exists").wait(Bucket=bucket_name)

        lf_client.register_resource(
            HybridAccessEnabled=False,
            ResourceArn=f"arn:aws:s3:::{bucket_name}",
            UseServiceLinkedRole=True,
        )

        yield f"s3://{bucket_name}"

    except Exception as e:
        pytest.fail(f"Failed to create S3 bucket or register it in lakeformation: {str(e)}")

    try:
        # Delete all objects in the bucket
        s3_resource = boto3.resource("s3")
        bucket = s3_resource.Bucket(bucket_name)
        bucket.objects.all().delete()

        # Delete the bucket
        s3_client.delete_bucket(Bucket=bucket_name)
    except Exception as e:
        print(f"Warning: Failed to clean up S3 bucket {bucket_name}: {str(e)}")


def _grant_lf_permissions(
    iam_arn: str, lf_client: LakeFormationClient, key: str, values: List[str]
) -> None:
    """Grant permissions to the iam principal to associate resources with tags and manage databases and tables with the tags."""
    grants = [
        GrantPermissionsRequestTypeDef(
            Principal={"DataLakePrincipalIdentifier": iam_arn},
            Resource={"LFTag": {"TagKey": key, "TagValues": values}},
            Permissions=["ASSOCIATE"],
        ),
        GrantPermissionsRequestTypeDef(
            Principal={"DataLakePrincipalIdentifier": iam_arn},
            Resource={
                "LFTagPolicy": {
                    "ResourceType": "DATABASE",
                    "Expression": [{"TagKey": key, "TagValues": values}],
                }
            },
            Permissions=["ALL"],
        ),
        GrantPermissionsRequestTypeDef(
            Principal={"DataLakePrincipalIdentifier": iam_arn},
            Resource={
                "LFTagPolicy": {
                    "ResourceType": "TABLE",
                    "Expression": [{"TagKey": key, "TagValues": values}],
                }
            },
            Permissions=["ALL"],
        ),
    ]

    for grant in grants:
        lf_client.grant_permissions(**grant)


@pytest.fixture(scope="session")
def lf_tags_config(lf_client: LakeFormationClient) -> Generator[LakeformationConfig, None, None]:
    """Setup lakeformation tags and permissions to the current user."""
    key = "test"
    values = ["true", "false"]
    lf_client.create_lf_tag(TagKey=key, TagValues=values)
    _grant_lf_permissions(IAM_ARN, lf_client, key, values)

    yield LakeformationConfig(enabled=True, tags={"test": "true"})

    lf_client.delete_lf_tag(
        TagKey="test",
    )


@pytest.fixture
def pipelines(
    lf_tags_config: LakeformationConfig, s3_bucket: str
) -> Generator[Tuple[dlt.Pipeline, dlt.Pipeline], None, None]:
    """Create two identical pipelines with and without lakeformation enabled."""
    uid = uniq_id()
    dataset_name = "lakeformation_test" + uid
    pipeline_name = "lakeformation_test" + uid

    staging_destination = filesystem(s3_bucket)

    lf_enabled_pipeline = dlt.pipeline(
        pipeline_name,
        destination=athena(
            lakeformation_config=lf_tags_config,
            athena_work_group=ATHENA_WORKGROUP,
            query_result_bucket=QUERY_RESULT_BUCKET,
        ),
        dataset_name=dataset_name,
        staging=staging_destination,
        dev_mode=False,
    )

    lf_disabled_pipeline = dlt.pipeline(
        pipeline_name,
        destination=athena(
            athena_work_group=ATHENA_WORKGROUP,
            query_result_bucket=QUERY_RESULT_BUCKET,
        ),
        dataset_name=dataset_name,
        staging=staging_destination,
        dev_mode=False,
    )

    yield lf_enabled_pipeline, lf_disabled_pipeline

    lf_enabled_pipeline.drop()
    lf_disabled_pipeline.drop()


def _verify_tags_on_database(
    lf_client: LakeFormationClient,
    dataset_name: str,
    key: Optional[str],
    values: Optional[List[str]],
) -> None:
    """Verify that the database has the expected tags."""
    db_tags: GetResourceLFTagsResponseTypeDef = lf_client.get_resource_lf_tags(
        Resource={"Database": {"Name": dataset_name}}
    )
    if key is None:
        assert "LFTagOnDatabase" not in db_tags
    else:
        assert db_tags["LFTagOnDatabase"][0]["TagKey"] == key
        assert db_tags["LFTagOnDatabase"][0]["TagValues"] == values


def _verify_tags_on_table(
    lf_client: LakeFormationClient,
    dataset_name: str,
    table_name: str,
    key: Optional[str],
    values: Optional[List[str]],
) -> None:
    """Verify that the table has the expected tags."""
    db_tags: GetResourceLFTagsResponseTypeDef = lf_client.get_resource_lf_tags(
        Resource={"Table": {"DatabaseName": dataset_name, "Name": table_name}}
    )
    if key is None:
        assert "LFTagsOnTable" not in db_tags
    else:
        assert db_tags["LFTagsOnTable"][0]["TagKey"] == key
        assert db_tags["LFTagsOnTable"][0]["TagValues"] == values


def test_new_pipeline_with_lakeformation_tags(
    lf_client: LakeFormationClient, pipelines: Tuple[dlt.Pipeline, dlt.Pipeline]
) -> None:
    """Test that a new pipeline with LakeFormation configuration has tags applied to database and tables."""
    lf_enabled_pipeline, _ = pipelines

    # Create a simple table
    table_name = "test_table"

    @dlt.resource(name=table_name)
    def test_data():
        yield {"id": 1, "name": "test"}

    # Run the pipeline
    lf_enabled_pipeline.run(test_data)

    # Verify tags are applied to the database and tables
    _verify_tags_on_database(lf_client, lf_enabled_pipeline.dataset_name, "test", ["true"])
    _verify_tags_on_table(
        lf_client, lf_enabled_pipeline.dataset_name, table_name, "test", ["true"]
    )

    # Verify that we can run the pipeline again without errors
    lf_enabled_pipeline.run(test_data)


def test_apply_tags_to_existing_pipeline_resource(
    lf_client: LakeFormationClient, pipelines: Tuple[dlt.Pipeline, dlt.Pipeline]
) -> None:
    """Test applying LakeFormation tags schema/tables from a pipeline with lakeformation disabled."""
    lf_enabled_pipeline, lf_disabled_pipeline = pipelines

    # Create a table without LakeFormation
    table_name = "test_table"

    @dlt.resource(name=table_name)
    def test_data():
        yield {"id": 1, "name": "test"}

    # Run the pipeline without LakeFormation
    lf_disabled_pipeline.run(test_data)

    # Verify no tags exists
    _verify_tags_on_database(lf_client, lf_disabled_pipeline.dataset_name, None, None)
    _verify_tags_on_table(lf_client, lf_disabled_pipeline.dataset_name, table_name, None, None)

    # Run the pipeline again with LakeFormation enabled
    lf_enabled_pipeline.run(test_data)

    # Verify tags are applied to the database and tables
    _verify_tags_on_database(lf_client, lf_enabled_pipeline.dataset_name, "test", ["true"])
    _verify_tags_on_table(
        lf_client, lf_enabled_pipeline.dataset_name, table_name, "test", ["true"]
    )


def test_remove_lakeformation_tags_from_resource(
    lf_client: LakeFormationClient, pipelines: Tuple[dlt.Pipeline, dlt.Pipeline]
) -> None:
    """Test removing lakeformation tags from a resource"""
    lf_enabled_pipeline, lf_disabled_pipeline = pipelines

    # Create a table with LakeFormation enabled
    table_name = "test_table"

    @dlt.resource(name=table_name)
    def test_data():
        yield {"id": 1, "name": "test"}

    # Run the pipeline with LakeFormation enabled
    lf_enabled_pipeline.run(test_data)

    # Verify tags are applied to the database and tables
    _verify_tags_on_database(lf_client, lf_enabled_pipeline.dataset_name, "test", ["true"])
    _verify_tags_on_table(
        lf_client, lf_enabled_pipeline.dataset_name, table_name, "test", ["true"]
    )

    # Run the pipeline with LakeFormation disabled
    lf_disabled_pipeline.run(test_data)

    # Verify tags are removed
    _verify_tags_on_database(lf_client, lf_enabled_pipeline.dataset_name, None, None)
    _verify_tags_on_table(lf_client, lf_enabled_pipeline.dataset_name, table_name, None, None)
