from typing import Iterator, Tuple, List, Generator, Optional

from mypy_boto3_lakeformation import LakeFormationClient
from mypy_boto3_lakeformation.type_defs import (
    GetResourceLFTagsResponseTypeDef,
    GrantPermissionsRequestTypeDef,
)
import pytest
import boto3
from botocore.session import Session

import dlt
from dlt.common.utils import uniq_id

from dlt.sources.credentials import AwsCredentials

from dlt.destinations import filesystem, athena
from dlt.destinations.impl.athena.configuration import LakeformationConfig

from tests.load.utils import destinations_configs, DestinationTestConfiguration
from tests.utils import (
    preserve_module_environ,
    auto_module_test_storage,
    auto_module_test_run_context,
)

# These tests require Lakeformation permissions to be able to run.
# The tests have only been run as lakeformation admin so far, since the fixtures require
# permissions to create new tags, and grant lakeformation permissions to the current role


@pytest.fixture(scope="module")
def botocore_session(
    auto_module_test_storage, preserve_module_environ, auto_module_test_run_context
) -> Iterator[Session]:
    # with dlt.pipeline("lf_client_pipeline", destination="athena").destination_client() as client:
    credentials = dlt.config.get("destination.athena.credentials", AwsCredentials)
    session: Session = credentials._to_botocore_session()
    yield session


@pytest.fixture(scope="module")
def lf_client(botocore_session: Session) -> LakeFormationClient:
    return botocore_session.create_client("lakeformation")  # type: ignore[return-value]


@pytest.fixture(scope="module", autouse=True)
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
            "No access to LakeFormation. Please ensure LakeFormation is enabled and you have proper"
            " permissions"
        )
    except Exception as e:
        pytest.skip(f"Failed to verify LakeFormation status: {str(e)}")


@pytest.fixture(scope="module")
def s3_bucket(
    lf_client: LakeFormationClient, botocore_session: Session
) -> Generator[str, None, None]:
    """Create a new S3 bucket and register it in lakeformation."""
    from mypy_boto3_s3.client import S3Client

    s3_client: S3Client = botocore_session.create_client("s3")  # type: ignore[assignment]
    bucket_name = "dlt-lakeformation-test-bucket-" + uniq_id(8)

    try:
        # Create bucket in the same region as Athena
        region = botocore_session.get_config_variable("region")
        assert region, "Must have region in botocore session"
        s3_client.create_bucket(
            Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": region}
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
        boto3_session = boto3.Session(botocore_session=botocore_session)
        s3_resource = boto3_session.resource("s3")
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


@pytest.fixture(scope="module")
def lf_tags_config(
    lf_client: LakeFormationClient, botocore_session: Session
) -> Iterator[LakeformationConfig]:
    """Setup lakeformation tags and permissions to the current user."""
    user_arn = botocore_session.create_client("sts").get_caller_identity()["Arn"]  # type: ignore

    key = "test"
    values = ["true", "false"]
    lf_client.create_lf_tag(TagKey=key, TagValues=values)
    _grant_lf_permissions(user_arn, lf_client, key, values)

    yield LakeformationConfig(enabled=True, tags={"test": "true"})

    lf_client.delete_lf_tag(
        TagKey="test",
    )


def create_pipelines(
    lf_tags_config: LakeformationConfig,
    s3_bucket: str,
    destination_config: DestinationTestConfiguration,
) -> Tuple[dlt.Pipeline, dlt.Pipeline]:
    """Create two identical pipelines with and without lakeformation enabled. Test fixtures will
    drop pipelines at the end
    """
    uid = uniq_id()
    dataset_name = "lakeformation_test" + uid
    pipeline_name = "lakeformation_test" + uid

    staging_destination = filesystem(s3_bucket)

    lf_enabled_pipeline = destination_config.setup_pipeline(
        pipeline_name,
        destination=destination_config.destination_factory(lakeformation_config=lf_tags_config),
        dataset_name=dataset_name,
        staging=staging_destination,
    )
    lf_disabled_pipeline = destination_config.setup_pipeline(
        pipeline_name,
        destination=destination_config.destination_factory(),
        dataset_name=dataset_name,
        staging=staging_destination,
    )

    return lf_enabled_pipeline, lf_disabled_pipeline


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


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        subset=["athena"],
    ),
    ids=lambda x: x.name,
)
def test_new_pipeline_with_lakeformation_tags(
    lf_client: LakeFormationClient,
    lf_tags_config: LakeformationConfig,
    s3_bucket: str,
    destination_config: DestinationTestConfiguration,
) -> None:
    """Test that a new pipeline with LakeFormation configuration has tags applied to database and tables."""
    lf_enabled_pipeline, _ = create_pipelines(lf_tags_config, s3_bucket, destination_config)

    # Create a simple table
    table_name = "test_table"

    @dlt.resource(name=table_name)
    def test_data():
        yield {"id": 1, "name": "test"}

    # Run the pipeline
    lf_enabled_pipeline.run(test_data)

    # Verify tags are applied to the database and tables
    _verify_tags_on_database(lf_client, lf_enabled_pipeline.dataset_name, "test", ["true"])
    _verify_tags_on_table(lf_client, lf_enabled_pipeline.dataset_name, table_name, "test", ["true"])

    # Verify that we can run the pipeline again without errors
    lf_enabled_pipeline.run(test_data)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        subset=["athena"],
    ),
    ids=lambda x: x.name,
)
def test_apply_tags_to_existing_pipeline_resource(
    lf_client: LakeFormationClient,
    lf_tags_config: LakeformationConfig,
    s3_bucket: str,
    destination_config: DestinationTestConfiguration,
) -> None:
    """Test applying LakeFormation tags schema/tables from a pipeline with lakeformation disabled."""
    lf_enabled_pipeline, lf_disabled_pipeline = create_pipelines(
        lf_tags_config, s3_bucket, destination_config
    )

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
    _verify_tags_on_table(lf_client, lf_enabled_pipeline.dataset_name, table_name, "test", ["true"])


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        subset=["athena"],
    ),
    ids=lambda x: x.name,
)
def test_remove_lakeformation_tags_from_resource(
    lf_client: LakeFormationClient,
    lf_tags_config: LakeformationConfig,
    s3_bucket: str,
    destination_config: DestinationTestConfiguration,
) -> None:
    """Test removing lakeformation tags from a resource"""
    lf_enabled_pipeline, lf_disabled_pipeline = create_pipelines(
        lf_tags_config, s3_bucket, destination_config
    )

    # Create a table with LakeFormation enabled
    table_name = "test_table"

    @dlt.resource(name=table_name)
    def test_data():
        yield {"id": 1, "name": "test"}

    # Run the pipeline with LakeFormation enabled
    lf_enabled_pipeline.run(test_data)

    # Verify tags are applied to the database and tables
    _verify_tags_on_database(lf_client, lf_enabled_pipeline.dataset_name, "test", ["true"])
    _verify_tags_on_table(lf_client, lf_enabled_pipeline.dataset_name, table_name, "test", ["true"])

    # Run the pipeline with LakeFormation disabled
    lf_disabled_pipeline.run(test_data)

    # Verify tags are removed
    _verify_tags_on_database(lf_client, lf_enabled_pipeline.dataset_name, None, None)
    _verify_tags_on_table(lf_client, lf_enabled_pipeline.dataset_name, table_name, None, None)
