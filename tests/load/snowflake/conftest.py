import pytest

from dlt.common.configuration.specs import (
    AwsCredentialsWithoutDefaults,
    AzureCredentialsWithoutDefaults,
)
from dlt.common.destination.configuration import CsvFormatConfiguration


@pytest.fixture
def test_table() -> str:
    """Fixture for test table name."""
    return "test_schema.test_table"


@pytest.fixture
def stage_bucket_url() -> str:
    """Fixture for stage bucket URL."""
    return "s3://test-stage-bucket/"


@pytest.fixture
def stage_bucket_url_with_prefix() -> str:
    """Fixture for stage bucket URL."""
    return "s3://test-stage-bucket/with/prefix"


@pytest.fixture
def local_file_path() -> str:
    """Fixture for local file path."""
    return "/tmp/data.csv"


@pytest.fixture
def stage_name() -> str:
    """Fixture for stage name."""
    return "test_stage"


@pytest.fixture
def local_stage_path() -> str:
    """Fixture for local stage file path."""
    return "@%temp_stage/data.csv"


@pytest.fixture
def aws_credentials() -> AwsCredentialsWithoutDefaults:
    """Fixture for AWS credentials."""
    return AwsCredentialsWithoutDefaults(
        aws_access_key_id="test_aws_key", aws_secret_access_key="test_aws_secret"
    )


@pytest.fixture
def azure_credentials() -> AzureCredentialsWithoutDefaults:
    """Fixture for Azure credentials."""
    return AzureCredentialsWithoutDefaults(
        azure_storage_account_name="teststorage",
        azure_storage_sas_token="test_sas_token",
        azure_account_host="teststorage.blob.core.windows.net",
    )


@pytest.fixture
def default_csv_format() -> CsvFormatConfiguration:
    """Fixture for default CSV format."""
    return CsvFormatConfiguration(
        include_header=True, delimiter=",", encoding="UTF-8", on_error_continue=False
    )
