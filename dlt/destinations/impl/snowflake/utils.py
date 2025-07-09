from typing import Optional
from urllib.parse import urlparse, urlunparse

from dlt.common.configuration.specs import (
    CredentialsConfiguration,
    AwsCredentialsWithoutDefaults,
    AzureCredentialsWithoutDefaults,
)
from dlt.common.destination.configuration import CsvFormatConfiguration
from dlt.common.storages import FilesystemConfiguration
from dlt.common.storages.configuration import ensure_canonical_az_url
from dlt.common.storages.fsspec_filesystem import (
    AZURE_BLOB_STORAGE_PROTOCOLS,
    S3_PROTOCOLS,
)

from dlt.common.typing import TLoaderFileFormat
from dlt.destinations.exceptions import LoadJobTerminalException


def ensure_snowflake_azure_url(
    file_url: str, account_name: str = None, account_host: str = None
) -> str:
    # Explicit azure credentials are needed to load from bucket without a named stage
    if not account_host and account_name:
        account_host = f"{account_name}.blob.core.windows.net"
    # get canonical url first to convert it into snowflake form
    canonical_url = ensure_canonical_az_url(
        file_url,
        "azure",
        account_name,
        account_host,
    )
    parsed_file_url = urlparse(canonical_url)
    return urlunparse(
        parsed_file_url._replace(
            path=f"/{parsed_file_url.username}{parsed_file_url.path}",
            netloc=parsed_file_url.hostname,
        )
    )


def _build_format_clause(format_clause: list[str]) -> str:
    joined = ", ".join(format_clause)
    return f"({joined})"


def gen_copy_sql(
    file_url: str,
    qualified_table_name: str,
    loader_file_format: TLoaderFileFormat,
    is_case_sensitive: bool,
    stage_name: Optional[str] = None,
    stage_bucket_url: Optional[str] = None,
    local_stage_file_path: Optional[str] = None,
    staging_credentials: Optional[CredentialsConfiguration] = None,
    csv_format: Optional[CsvFormatConfiguration] = None,
    use_vectorized_scanner: Optional[bool] = False,
) -> str:
    """
    Generates a Snowflake COPY command to load data from a file.

    Args:
        use_vectorized_scanner: Whether to use the vectorized scanner in COPY INTO
        file_url: URL of the file to load
        qualified_table_name: Fully qualified name of the target table
        loader_file_format: Format of the source file (jsonl, parquet, csv)
        is_case_sensitive: Whether column matching should be case-sensitive
        stage_name: Optional name of a predefined Snowflake stage
        stage_bucket_url: Optional URL of the bucket containing the file
        local_stage_file_path: Path to use for local files
        staging_credentials: Optional credentials for accessing cloud storage
        csv_format: Optional configuration for CSV format

    Returns:
        A SQL string containing the COPY command
    """
    # Determine file location type and get potentially modified URL
    parsed_file_url = urlparse(file_url)

    # Initialize clause components
    credentials_clause = ""
    files_clause = ""

    # Handle different file location types
    if parsed_file_url.scheme == "file" or FilesystemConfiguration.is_local_path(file_url):
        from_clause = f"FROM {local_stage_file_path}"

    elif stage_name:
        relative_url = parsed_file_url.path.lstrip("/")

        # If stage bucket URL has a path, remove it from the beginning of the relative URL because this is already specified
        # in the Snowflake stage location.
        if stage_bucket_url:
            parsed_bucket_url = urlparse(stage_bucket_url)
            stage_bucket_path = parsed_bucket_url.path.lstrip("/")
            if stage_bucket_path:
                relative_url = relative_url.removeprefix(stage_bucket_path)

        from_clause = f"FROM @{stage_name}"
        files_clause = f"FILES = ('{relative_url}')"

    elif parsed_file_url.scheme in S3_PROTOCOLS:
        if staging_credentials and isinstance(staging_credentials, AwsCredentialsWithoutDefaults):
            credentials_clause = (
                f"CREDENTIALS=(AWS_KEY_ID='{staging_credentials.aws_access_key_id}' "
                f"AWS_SECRET_KEY='{staging_credentials.aws_secret_access_key}')"
            )
            from_clause = f"FROM '{file_url}'"
        else:
            raise LoadJobTerminalException(
                file_url,
                f"Cannot load from S3 path `{file_url}` without either credentials or a stage"
                " name.",
            )

    elif parsed_file_url.scheme in AZURE_BLOB_STORAGE_PROTOCOLS:
        if staging_credentials and isinstance(staging_credentials, AzureCredentialsWithoutDefaults):
            credentials_clause = (
                f"CREDENTIALS=(AZURE_SAS_TOKEN='?{staging_credentials.azure_storage_sas_token}')"
            )
            converted_az_url = ensure_snowflake_azure_url(
                file_url,
                staging_credentials.azure_storage_account_name,
                staging_credentials.azure_account_host,
            )
            from_clause = f"FROM '{converted_az_url}'"
        else:
            raise LoadJobTerminalException(
                file_url,
                f"Cannot load from Azure path `{file_url}` without either credentials or a stage"
                " name.",
            )
    else:
        raise LoadJobTerminalException(
            file_url,
            f"Cannot load from bucket path `{file_url}` without a stage name. See "
            "https://dlthub.com/docs/dlt-ecosystem/destinations/snowflake for "
            "instructions on setting up the `stage_name`",
        )

    # Generate file format clause
    if is_case_sensitive:
        column_match_clause = "MATCH_BY_COLUMN_NAME='CASE_SENSITIVE'"
    else:
        column_match_clause = "MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'"

    if loader_file_format == "jsonl":
        on_error_clause = ""
        format_opts = [
            "TYPE = 'JSON'",
            "BINARY_FORMAT = 'BASE64'",
        ]
    elif loader_file_format == "parquet":
        format_opts = [
            "TYPE = 'PARQUET'",
            "BINARY_AS_TEXT = FALSE",
            "USE_LOGICAL_TYPE = TRUE",
        ]
        if use_vectorized_scanner:
            format_opts.append("USE_VECTORIZED_SCANNER = TRUE")
            on_error_clause = "ON_ERROR = ABORT_STATEMENT"
        else:
            on_error_clause = ""
    elif loader_file_format == "csv":
        csv_config = csv_format or CsvFormatConfiguration()
        if not csv_config.include_header:
            column_match_clause = ""
        if csv_config.on_error_continue:
            on_error_clause = "ON_ERROR = CONTINUE"
        else:
            on_error_clause = ""
        format_opts = [
            "TYPE = 'CSV'",
            "BINARY_FORMAT = 'UTF-8'",
            f"PARSE_HEADER = {csv_config.include_header}",
            "FIELD_OPTIONALLY_ENCLOSED_BY = '\"'",
            "NULL_IF = ('')",
            "ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE",
            f"FIELD_DELIMITER = '{csv_config.delimiter}'",
            f"ENCODING = '{csv_config.encoding}'",
        ]
    else:
        raise ValueError(f"`{loader_file_format=:}` not supported for Snowflake COPY command.")

    source_format = _build_format_clause(format_opts)

    # Construct the final SQL statement
    return f"""COPY INTO {qualified_table_name}
        {from_clause}
        {files_clause}
        {credentials_clause}
        FILE_FORMAT = {source_format}
        {column_match_clause}
        {on_error_clause}
    """
