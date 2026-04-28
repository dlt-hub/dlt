from typing import Any, Iterator

import pytest

import dlt
from dlt.common.configuration.specs.aws_credentials import AwsCredentials
from dlt.common.destination.exceptions import UnsupportedDataType
from dlt.common.utils import custom_environ, uniq_id
from dlt.destinations import filesystem, redshift
from dlt.pipeline.exceptions import PipelineStepFailed
from tests.load.utils import destinations_configs, DestinationTestConfiguration, AWS_BUCKET
from tests.cases import table_update_and_row

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["redshift"]),
    ids=lambda x: x.name,
)
def test_redshift_blocks_time_column(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline("redshift_" + uniq_id(), dev_mode=True)

    column_schemas, data_types = table_update_and_row()

    # apply the exact columns definitions so we process nested and wei types correctly!
    @dlt.resource(table_name="data_types", write_disposition="append", columns=column_schemas)
    def my_resource() -> Iterator[Any]:
        nonlocal data_types
        yield [data_types] * 10

    @dlt.source(max_table_nesting=0)
    def my_source() -> Any:
        return my_resource

    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(my_source(), **destination_config.run_kwargs)
    assert isinstance(pip_ex.value.__cause__, UnsupportedDataType)
    if destination_config.file_format == "parquet":
        assert pip_ex.value.__cause__.data_type == "time"
    else:
        assert pip_ex.value.__cause__.data_type in ("time", "binary")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["redshift"], with_file_format="parquet"),
    ids=lambda x: x.name,
)
def test_redshift_staging_with_default_chain_credentials(
    destination_config: DestinationTestConfiguration,
    mocker: Any,
) -> None:
    """Redshift loads data via S3 staging using frozen credentials from botocore default chain."""
    if destination_config.staging_iam_role:
        pytest.skip("test requires credential-forwarding, not IAM role")

    fs_creds = dlt.secrets.get("destination.filesystem.credentials", AwsCredentials)
    sts_creds = fs_creds.to_sts_credentials()
    fs_creds.aws_secret_access_key = sts_creds["aws_secret_access_key"]
    fs_creds.aws_access_key_id = sts_creds["aws_access_key_id"]
    fs_creds.aws_session_token = sts_creds["aws_session_token"]
    boto_session = fs_creds._to_botocore_session()
    assert boto_session.get_credentials().token == fs_creds.aws_session_token

    spy = mocker.spy(AwsCredentials, "to_session_credentials")

    staging_destination = dlt.destinations.filesystem(AWS_BUCKET, credentials=boto_session)
    pipeline = destination_config.setup_pipeline(
        "redshift_staging_" + uniq_id(), dev_mode=True, staging=staging_destination
    )
    pipeline.run(
        [{"id": i, "value": f"row_{i}"} for i in range(5)],
        table_name="default_chain_test",
        loader_file_format="jsonl",
    )

    with pipeline.sql_client() as c:
        rows = c.execute_sql("SELECT count(*) FROM default_chain_test")
        assert rows[0][0] == 5

    # verify to_session_credentials was called and returned frozen STS credentials with token
    assert spy.call_count > 0
    for call in spy.spy_return_list:
        # assert call["aws_access_key_id"] == sts_resp["AccessKeyId"]
        # assert call["aws_secret_access_key"] == sts_resp["SecretAccessKey"]
        assert call["aws_session_token"] is not None
        assert call["aws_session_token"] == fs_creds.aws_session_token
