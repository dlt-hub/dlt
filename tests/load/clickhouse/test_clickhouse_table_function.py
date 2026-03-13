from typing import Any, Dict
from urllib.parse import urlparse

import pytest

from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs.aws_credentials import AwsCredentials
from dlt.destinations.impl.clickhouse.configuration import (
    ClickHouseClientConfiguration,
    ClickHouseCredentials,
)
from dlt.destinations.impl.clickhouse.clickhouse import ClickHouseLoadJob

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "s3_extra_credentials,aws_access_key_id,aws_secret_access_key,expected_auth",
    [
        # standard AWS key/secret auth
        (
            None,
            "access_key",
            "secret_key",
            "'access_key','secret_key'",
        ),
        # extra_credentials takes precedence over key/secret
        (
            {"role_arn": "my_role_arn"},
            "access_key",
            "secret_key",
            "extra_credentials(role_arn = 'my_role_arn')",
        ),
        # NOSIGN fallback when no credentials are provided
        (
            None,
            None,
            None,
            "NOSIGN",
        ),
        # values with special chars are escaped to prevent SQL injection
        (
            {"role_arn": "arn:aws:iam::role/it's tricky\\"},
            "access_key",
            "secret_key",
            "extra_credentials(role_arn = 'arn:aws:iam::role/it''s tricky\\\\')",
        ),
    ],
    ids=[
        "aws_key_secret",
        "s3_extra_credentials",
        "nosign_fallback",
        "s3_extra_credentials_escaped",
    ],
)
def test_clickhouse_s3_table_function(
    s3_extra_credentials: Dict[str, str],
    aws_access_key_id: str,
    aws_secret_access_key: str,
    expected_auth: str,
) -> None:
    creds_dict: Dict[str, Any] = {"host": "host"}
    if s3_extra_credentials:
        creds_dict["s3_extra_credentials"] = s3_extra_credentials

    config = resolve_configuration(
        ClickHouseClientConfiguration(credentials=ClickHouseCredentials(creds_dict))
    )
    staging_credentials = AwsCredentials(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    job = ClickHouseLoadJob(
        "s3://bucket-name/path/x.y.1.parquet",
        config=config,
        staging_credentials=staging_credentials,
    )

    result = job._get_table_function(
        bucket_scheme="s3",
        bucket_url=urlparse("s3://bucket-name/path/to/data"),
        compression="gz",
        clickhouse_format="parquet",
    )

    expected = (
        "s3('https://bucket-name.s3.amazonaws.com/path/to/data'"
        f",{expected_auth},'parquet','auto','gz')"
    )
    assert result == expected
