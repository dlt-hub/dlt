from tests.utils import skip_if_not_active

skip_if_not_active("snowflake")

import pytest
from urllib.parse import urlparse

from dlt.common.configuration.specs import (
    AzureCredentialsWithoutDefaults,
    AwsCredentialsWithoutDefaults,
)
from dlt.common.destination.configuration import CsvFormatConfiguration
from dlt.common.exceptions import TerminalValueError
from dlt.destinations.exceptions import LoadJobTerminalException
from dlt.destinations.impl.snowflake.utils import (
    gen_copy_sql,
    ensure_snowflake_azure_url,
)


# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential

# ----------------------------------
# Helper Functions
# ----------------------------------


def assert_sql_contains(sql: str, *phrases):
    """Assert that SQL contains all the given phrases."""
    for phrase in phrases:
        assert phrase in sql, f"Expected '{phrase}' in SQL, but not found:\n{sql}"


def assert_sql_not_contains(sql: str, *phrases):
    """Assert that SQL does not contain any of the given phrases."""
    for phrase in phrases:
        assert phrase not in sql, f"Found unexpected '{phrase}' in SQL:\n{sql}"


# ----------------------------------
# Unit Tests for File Format Handling
# ----------------------------------


def test_file_format_jsonl(test_table: str, local_file_path: str, local_stage_path: str):
    """Test JSONL format handling in gen_copy_sql."""
    sql = gen_copy_sql(
        file_url=local_file_path,
        qualified_table_name=test_table,
        loader_file_format="jsonl",
        is_case_sensitive=True,
        local_stage_file_path=local_stage_path,
    )

    assert_sql_contains(
        sql,
        "TYPE = 'JSON'",
        "BINARY_FORMAT = 'BASE64'",
        "MATCH_BY_COLUMN_NAME='CASE_SENSITIVE'",
    )


def test_file_format_parquet(test_table: str, local_file_path: str, local_stage_path: str):
    """Test Parquet format handling in gen_copy_sql."""
    sql = gen_copy_sql(
        file_url=local_file_path,
        qualified_table_name=test_table,
        loader_file_format="parquet",
        is_case_sensitive=True,
        local_stage_file_path=local_stage_path,
    )

    assert_sql_contains(
        sql,
        "TYPE = 'PARQUET'",
        "BINARY_AS_TEXT = FALSE",
        "USE_LOGICAL_TYPE = TRUE",
        "MATCH_BY_COLUMN_NAME='CASE_SENSITIVE'",
    )


def test_file_format_parquet_vectorized(
    test_table: str, local_file_path: str, local_stage_path: str
):
    """Test Parquet format with vectorized scanner in gen_copy_sql."""
    sql = gen_copy_sql(
        file_url=local_file_path,
        qualified_table_name=test_table,
        loader_file_format="parquet",
        is_case_sensitive=True,
        local_stage_file_path=local_stage_path,
        use_vectorized_scanner=True,
    )

    assert_sql_contains(
        sql,
        "TYPE = 'PARQUET'",
        "BINARY_AS_TEXT = FALSE",
        "USE_LOGICAL_TYPE = TRUE",
        "USE_VECTORIZED_SCANNER = TRUE",
        "ON_ERROR = ABORT_STATEMENT",
        "MATCH_BY_COLUMN_NAME='CASE_SENSITIVE'",
    )


@pytest.mark.parametrize(
    "include_header,delimiter,encoding,on_error_continue",
    [
        (True, ",", "UTF-8", False),
        (False, "|", "UTF-8", False),
        (True, "\t", "UTF-16", True),
        (False, ";", "ASCII", True),
    ],
)
def test_file_format_csv(
    include_header: bool,
    delimiter: str,
    encoding: str,
    on_error_continue: bool,
    test_table: str,
    local_file_path: str,
    local_stage_path: str,
):
    """Test CSV format handling in gen_copy_sql with various options."""
    csv_config = CsvFormatConfiguration(
        include_header=include_header,
        delimiter=delimiter,
        encoding=encoding,
        on_error_continue=on_error_continue,
    )

    sql = gen_copy_sql(
        file_url=local_file_path,
        qualified_table_name=test_table,
        loader_file_format="csv",
        is_case_sensitive=True,
        local_stage_file_path=local_stage_path,
        csv_format=csv_config,
    )

    # Check format clause
    assert_sql_contains(
        sql,
        "TYPE = 'CSV'",
        f"PARSE_HEADER = {include_header}",
        f"FIELD_DELIMITER = '{delimiter}'",
        f"ENCODING = '{encoding}'",
    )

    # Check column match setting
    if include_header:
        assert_sql_contains(sql, "MATCH_BY_COLUMN_NAME='CASE_SENSITIVE'")
    else:
        assert_sql_not_contains(sql, "MATCH_BY_COLUMN_NAME")

    # Check on_error_continue setting
    if on_error_continue:
        assert_sql_contains(sql, "ON_ERROR = CONTINUE")
    else:
        assert_sql_not_contains(sql, "ON_ERROR = CONTINUE")


# ----------------------------------
# Unit Tests for gen_copy_sql
# ----------------------------------


def test_gen_copy_sql_local_file(test_table: str, local_file_path: str, local_stage_path: str):
    """Test generating COPY command for local files."""
    sql = gen_copy_sql(
        file_url=local_file_path,
        qualified_table_name=test_table,
        loader_file_format="jsonl",
        is_case_sensitive=True,
        local_stage_file_path=local_stage_path,
    )

    assert_sql_contains(
        sql,
        f"COPY INTO {test_table}",
        f"FROM {local_stage_path}",
        "FILE_FORMAT = (TYPE = 'JSON'",
        "MATCH_BY_COLUMN_NAME='CASE_SENSITIVE'",
    )


def test_gen_copy_sql_with_stage(test_table: str, stage_name: str, stage_bucket_url: str):
    """Test generating COPY command with a named stage."""
    file_url = f"{stage_bucket_url}path/to/file.parquet"

    sql = gen_copy_sql(
        file_url=file_url,
        qualified_table_name=test_table,
        loader_file_format="parquet",
        is_case_sensitive=False,
        stage_name=stage_name,
        stage_bucket_url=stage_bucket_url,
    )

    assert_sql_contains(
        sql,
        f"COPY INTO {test_table}",
        f"FROM @{stage_name}",
        "FILES = ('path/to/file.parquet')",
        "FILE_FORMAT = (TYPE = 'PARQUET'",
        "MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'",
    )


def test_gen_copy_sql_with_stage_with_prefix_no_slash(
    test_table: str, stage_name: str, stage_bucket_url_with_prefix: str
):
    """Test generating COPY command with a named stage and bucket url without a forward slash."""
    file_url = f"{stage_bucket_url_with_prefix}path/to/file.parquet"

    sql = gen_copy_sql(
        file_url=file_url,
        qualified_table_name=test_table,
        loader_file_format="parquet",
        is_case_sensitive=False,
        stage_name=stage_name,
        stage_bucket_url=stage_bucket_url_with_prefix,
    )

    assert_sql_contains(
        sql,
        f"COPY INTO {test_table}",
        f"FROM @{stage_name}",
        "FILES = ('path/to/file.parquet')",
        "FILE_FORMAT = (TYPE = 'PARQUET'",
        "MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'",
    )


def test_gen_copy_sql_with_stage_with_prefix_slash(
    test_table: str, stage_name: str, stage_bucket_url_with_prefix: str
):
    """Test generating COPY command with a named stage abd bucket url with a forward slash."""
    stage_bucket_url_with_prefix_slash = f"{stage_bucket_url_with_prefix}/"
    file_url = f"{stage_bucket_url_with_prefix_slash}path/to/file.parquet"

    sql = gen_copy_sql(
        file_url=file_url,
        qualified_table_name=test_table,
        loader_file_format="parquet",
        is_case_sensitive=False,
        stage_name=stage_name,
        stage_bucket_url=stage_bucket_url_with_prefix_slash,
    )

    assert_sql_contains(
        sql,
        f"COPY INTO {test_table}",
        f"FROM @{stage_name}",
        "FILES = ('path/to/file.parquet')",
        "FILE_FORMAT = (TYPE = 'PARQUET'",
        "MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'",
    )


def test_gen_copy_sql_s3_with_credentials(
    test_table: str, aws_credentials: AwsCredentialsWithoutDefaults
):
    """Test generating COPY command for S3 with AWS credentials."""
    s3_url = "s3://bucket/path/to/file.jsonl"

    sql = gen_copy_sql(
        file_url=s3_url,
        qualified_table_name=test_table,
        loader_file_format="jsonl",
        is_case_sensitive=True,
        staging_credentials=aws_credentials,
    )

    assert_sql_contains(
        sql,
        f"COPY INTO {test_table}",
        f"FROM '{s3_url}'",
        f"CREDENTIALS=(AWS_KEY_ID='{aws_credentials.aws_access_key_id}'"
        f" AWS_SECRET_KEY='{aws_credentials.aws_secret_access_key}')",
        "FILE_FORMAT = (TYPE = 'JSON'",
    )


def test_gen_copy_sql_s3_without_credentials_or_stage(test_table: str):
    """Test that using S3 without credentials or stage raises an error."""
    s3_url = "s3://bucket/path/to/file.jsonl"

    with pytest.raises(LoadJobTerminalException) as excinfo:
        gen_copy_sql(
            file_url=s3_url,
            qualified_table_name=test_table,
            loader_file_format="jsonl",
            is_case_sensitive=True,
        )

    assert "Cannot load from S3 path" in str(excinfo.value)
    assert "without either credentials or a stage name" in str(excinfo.value)


def test_gen_copy_sql_azure_with_credentials(
    test_table: str,
    azure_credentials: AzureCredentialsWithoutDefaults,
    default_csv_format: CsvFormatConfiguration,
):
    """Test generating COPY command for Azure Blob with credentials."""
    azure_url = "azure://teststorage.blob.core.windows.net/container/file.csv"

    sql = gen_copy_sql(
        file_url=azure_url,
        qualified_table_name=test_table,
        loader_file_format="csv",
        is_case_sensitive=True,
        staging_credentials=azure_credentials,
        csv_format=default_csv_format,
    )

    assert_sql_contains(
        sql,
        f"COPY INTO {test_table}",
        "FROM '",  # Partial match since we don't know exact URL
        f"CREDENTIALS=(AZURE_SAS_TOKEN='?{azure_credentials.azure_storage_sas_token}')",
        "FILE_FORMAT = (TYPE = 'CSV'",
    )

    # Extract the URL from the SQL for verification
    import re

    from_clause_match = re.search(r"FROM '([^']+)'", sql)
    assert from_clause_match, "FROM clause with URL not found in SQL"

    transformed_url = from_clause_match.group(1)
    parsed_url = urlparse(transformed_url)

    # Basic validations on the transformed URL
    assert parsed_url.netloc == "teststorage.blob.core.windows.net", "Hostname is incorrect"
    assert "teststorage" in parsed_url.path, "Path doesn't contain account name"
    assert "container" in parsed_url.path, "Path doesn't contain container name"


def test_gen_copy_sql_azure_without_credentials_or_stage(test_table: str):
    """Test that using Azure Blob without credentials or stage raises an error."""
    azure_url = "azure://account.blob.core.windows.net/container/file.csv"

    with pytest.raises(LoadJobTerminalException) as excinfo:
        gen_copy_sql(
            file_url=azure_url,
            qualified_table_name=test_table,
            loader_file_format="csv",
            is_case_sensitive=True,
        )

    assert "Cannot load from Azure path" in str(excinfo.value)
    assert "without either credentials or a stage name" in str(excinfo.value)


def test_gen_copy_sql_gcs_without_stage(test_table: str):
    """Test that using GCS without stage raises an error."""
    gcs_url = "gs://bucket/path/to/file.jsonl"

    with pytest.raises(LoadJobTerminalException) as excinfo:
        gen_copy_sql(
            file_url=gcs_url,
            qualified_table_name=test_table,
            loader_file_format="jsonl",
            is_case_sensitive=True,
        )

    assert "Cannot load from bucket path" in str(excinfo.value)
    assert "without a stage name" in str(excinfo.value)


@pytest.mark.parametrize(
    "is_case_sensitive,expected_case", [(True, "CASE_SENSITIVE"), (False, "CASE_INSENSITIVE")]
)
def test_gen_copy_sql_case_sensitivity(
    is_case_sensitive: bool,
    expected_case: str,
    test_table: str,
    local_file_path: str,
    local_stage_path: str,
):
    """Test case sensitivity setting in COPY command."""
    sql = gen_copy_sql(
        file_url=local_file_path,
        qualified_table_name=test_table,
        loader_file_format="jsonl",
        is_case_sensitive=is_case_sensitive,
        local_stage_file_path=local_stage_path,
    )

    assert_sql_contains(sql, f"MATCH_BY_COLUMN_NAME='{expected_case}'")


@pytest.mark.parametrize(
    "on_error_continue,include_header", [(True, True), (False, True), (True, False), (False, False)]
)
def test_gen_copy_sql_csv_options(
    on_error_continue: bool,
    include_header: bool,
    test_table: str,
    local_file_path: str,
    local_stage_path: str,
):
    """Test CSV options in COPY command."""
    csv_format = CsvFormatConfiguration(
        include_header=include_header,
        delimiter=",",
        encoding="UTF-8",
        on_error_continue=on_error_continue,
    )

    sql = gen_copy_sql(
        file_url=local_file_path,
        qualified_table_name=test_table,
        loader_file_format="csv",
        is_case_sensitive=True,
        local_stage_file_path=local_stage_path,
        csv_format=csv_format,
    )

    # Check ON_ERROR clause
    if on_error_continue:
        assert_sql_contains(sql, "ON_ERROR = CONTINUE")
    else:
        assert_sql_not_contains(sql, "ON_ERROR = CONTINUE")

    # Check column matching based on header
    if include_header:
        assert_sql_contains(sql, "MATCH_BY_COLUMN_NAME='CASE_SENSITIVE'")
    else:
        assert_sql_not_contains(sql, "MATCH_BY_COLUMN_NAME")


def test_full_workflow_s3_with_aws_credentials(
    test_table: str, aws_credentials: AwsCredentialsWithoutDefaults
):
    """Test the full workflow for S3 with AWS credentials."""
    # This test verifies that all components work together correctly
    s3_url = "s3://test-bucket/path/to/data.jsonl"

    # Generate the complete SQL directly
    sql = gen_copy_sql(
        file_url=s3_url,
        qualified_table_name=test_table,
        loader_file_format="jsonl",
        is_case_sensitive=True,
        staging_credentials=aws_credentials,
    )

    # Verify the final SQL
    assert_sql_contains(
        sql,
        f"COPY INTO {test_table}",
        f"FROM '{s3_url}'",
        f"CREDENTIALS=(AWS_KEY_ID='{aws_credentials.aws_access_key_id}'"
        f" AWS_SECRET_KEY='{aws_credentials.aws_secret_access_key}')",
        "FILE_FORMAT = (TYPE = 'JSON'",
        "BINARY_FORMAT = 'BASE64'",
        "MATCH_BY_COLUMN_NAME='CASE_SENSITIVE'",
    )


def test_full_workflow_azure_with_credentials(
    test_table: str, azure_credentials: AzureCredentialsWithoutDefaults
):
    """Test the full workflow for Azure Blob with credentials."""
    # This test verifies that all components work together correctly
    azure_url = "azure://teststorage.blob.core.windows.net/container/file.parquet"

    # Generate the complete SQL directly
    sql = gen_copy_sql(
        file_url=azure_url,
        qualified_table_name=test_table,
        loader_file_format="parquet",
        is_case_sensitive=False,
        staging_credentials=azure_credentials,
    )

    # Verify the final SQL
    assert_sql_contains(
        sql,
        f"COPY INTO {test_table}",
        f"CREDENTIALS=(AZURE_SAS_TOKEN='?{azure_credentials.azure_storage_sas_token}')",
        "FILE_FORMAT = (TYPE = 'PARQUET'",
        "BINARY_AS_TEXT = FALSE",
        "USE_LOGICAL_TYPE = TRUE",
        "MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'",
    )

    # Extract and verify the URL from the SQL
    import re

    from_clause_match = re.search(r"FROM '([^']+)'", sql)
    assert from_clause_match, "FROM clause with URL not found in SQL"

    transformed_url = from_clause_match.group(1)
    parsed_url = urlparse(transformed_url)

    # Basic validations on the transformed URL
    assert parsed_url.netloc == "teststorage.blob.core.windows.net", "Hostname is incorrect"
    assert "teststorage" in parsed_url.path, "Path doesn't contain account name"
    assert "container" in parsed_url.path, "Path doesn't contain container name"


def test_snowflake_azure_converter() -> None:
    with pytest.raises(TerminalValueError):
        ensure_snowflake_azure_url("az://dlt-ci-test-bucket")

    azure_url = ensure_snowflake_azure_url("az://dlt-ci-test-bucket", "my_account")
    assert azure_url == "azure://my_account.blob.core.windows.net/dlt-ci-test-bucket"

    azure_url = ensure_snowflake_azure_url(
        "az://dlt-ci-test-bucket/path/to/file.parquet", "my_account"
    )
    assert (
        azure_url
        == "azure://my_account.blob.core.windows.net/dlt-ci-test-bucket/path/to/file.parquet"
    )

    azure_url = ensure_snowflake_azure_url(
        "abfss://dlt-ci-test-bucket@my_account.blob.core.windows.net/path/to/file.parquet"
    )
    assert (
        azure_url
        == "azure://my_account.blob.core.windows.net/dlt-ci-test-bucket/path/to/file.parquet"
    )
