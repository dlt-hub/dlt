from typing import Any, Iterator
import os

import pytest

import dlt
from dlt.common.destination.exceptions import UnsupportedDataType
from dlt.common.utils import uniq_id
from dlt.pipeline.exceptions import PipelineStepFailed
from tests.load.utils import destinations_configs, DestinationTestConfiguration
from dlt.common.configuration.specs import AwsCredentialsWithoutDefaults
from tests.cases import table_update_and_row, assert_all_data_types_row
from tests.pipeline.utils import assert_load_info

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

AWS_BUCKET = dlt.config.get("tests.bucket_url_s3", str)
redshift_with_staging_configs = [
    DestinationTestConfiguration(
        destination_type="redshift",
        staging="filesystem",
        file_format="jsonl",
        bucket_url=AWS_BUCKET,
        extra_info="credential-forwarding",
    ),
    DestinationTestConfiguration(
        destination_type="redshift",
        staging="filesystem",
        file_format="jsonl",
        bucket_url=AWS_BUCKET,
        staging_iam_role="arn:aws:iam::267388281016:role/redshift_s3_read",
        extra_info="s3-role",
    ),
    ## for parquet, region should be unset
    DestinationTestConfiguration(
        destination_type="redshift",
        staging="filesystem",
        file_format="parquet",
        bucket_url=AWS_BUCKET,
        staging_iam_role="arn:aws:iam::267388281016:role/redshift_s3_read",
        extra_info="s3-role",
    ),
]
@pytest.mark.parametrize( 
    "destination_config", 
    redshift_with_staging_configs,
    ids=lambda x: x.name,
)
def test_copy_from_staging_with_region(destination_config: DestinationTestConfiguration ) -> None:
    """
    Tests if copy-command is constructed correctly for both iam-role and aws-credentials
    when REGION is set. It should be set for jsonl, but unset for parquet
    """
    # initialize pipeline
    pipeline : dlt.Pipeline = destination_config.setup_pipeline(
        "redshift_region_test_" + uniq_id(), dataset_name="redshift_region_test_" + uniq_id()
    )
    # TODO assert that region is actually configured
    # staging_credentials = ## how to get those??
    # assert  isinstance(staging_credentials, AwsCredentialsWithoutDefaults), "staging not set up correctly"
    # assert staging_credentials.region_name, "region not set"

    @dlt.resource(primary_key="id")
    def some_data():
        yield [{"id": 1}, {"id": 2}, {"id": 3}]

    @dlt.resource(write_disposition="replace")
    def other_data():
        yield [1, 2, 3, 4, 5]

    @dlt.source(max_table_nesting=0)
    def some_source():
        return [some_data(), other_data()]

    info = pipeline.run(some_source(), **destination_config.run_kwargs)
    package_info = pipeline.get_load_package_info(info.loads_ids[0])
    # print(package_info.asstr(verbosity=2))
    assert package_info.state == "loaded"
    # all three jobs succeeded
    assert len(package_info.jobs["failed_jobs"]) == 0

