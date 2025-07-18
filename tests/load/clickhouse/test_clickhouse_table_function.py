from os import environ
from urllib.parse import urlparse

import pytest

from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs.aws_credentials import AwsCredentials
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseClientConfiguration,
    ClickHouseCredentials,
)
from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseLoadJob


def test_clickhouse_table_function_aws_credentials() -> None:
    config = resolve_configuration(
        ClickHouseClientConfiguration(
            credentials=ClickHouseCredentials(
                {
                    "host": "host",
                }
            )
        )
    )
    staging_credentials = AwsCredentials(
        aws_access_key_id="access_key",
        aws_secret_access_key="secret_key",
    )
    client = ClickHouseLoadJob(
        "s3://bucket-name/path/x.y.1.parquet",
        config=config,
        staging_credentials=staging_credentials,
    )

    result = client._get_table_function(
        bucket_scheme="s3",
        bucket_url=urlparse("s3://bucket-name/path/to/data"),
        ext=".parquet",
        compression="gz",
        clickhouse_format="parquet",
    )

    parts = [
        "'https://bucket-name.s3.amazonaws.com/path/to/data'",
        "'access_key'",
        "'secret_key'",
        "'parquet'",
        "'auto'",
        "'gz'",
    ]
    expected = f"s3({','.join(parts)})"
    assert result == expected


def test_clickhouse_table_function_aws_s3_extra_credentials() -> None:
    config = resolve_configuration(
        ClickHouseClientConfiguration(
            credentials=ClickHouseCredentials(
                {
                    "host": "host",
                    "s3_extra_credentials": {"role_arn": "my_role_arn"},
                }
            )
        )
    )
    staging_credentials = AwsCredentials(
        aws_access_key_id="access_key",
        aws_secret_access_key="secret_key",
    )
    client = ClickHouseLoadJob(
        "s3://bucket-name/path/x.y.1.parquet",
        config=config,
        staging_credentials=staging_credentials,
    )

    result = client._get_table_function(
        bucket_scheme="s3",
        bucket_url=urlparse("s3://bucket-name/path/to/data"),
        ext=".parquet",
        compression="gz",
        clickhouse_format="parquet",
    )

    parts = [
        "'https://bucket-name.s3.amazonaws.com/path/to/data'",
        "extra_credentials(role_arn = 'my_role_arn')",
        "'parquet'",
        "'auto'",
        "'gz'",
    ]
    expected = f"s3({','.join(parts)})"
    assert result == expected
