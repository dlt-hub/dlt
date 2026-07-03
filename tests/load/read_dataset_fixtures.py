"""Shared fixtures for tests that read datasets and relations across destinations."""
from typing import Any, cast

import pytest

from tests.load.utils import (
    DestinationTestConfiguration,
    MEMORY_BUCKET,
    SFTP_BUCKET,
    destinations_configs,
)
from tests.utils import _preserve_environ


@pytest.fixture(
    scope="module",
    params=destinations_configs(
        default_sql_configs=True,
        read_only_sqlclient_configs=True,
        bucket_exclude=[SFTP_BUCKET, MEMORY_BUCKET],
    ),
    ids=lambda x: x.name,
)
def destination_config(request: pytest.FixtureRequest) -> DestinationTestConfiguration:
    return cast(DestinationTestConfiguration, request.param)


@pytest.fixture(scope="module")
def preserve_module_environ_per_destination_config(
    destination_config: DestinationTestConfiguration,
) -> Any:
    yield from _preserve_environ()


def skip_if_unsupported_filesystem_format(
    destination_config: DestinationTestConfiguration,
) -> None:
    if (
        destination_config.file_format not in ["parquet", "jsonl"]
        and destination_config.destination_type == "filesystem"
    ):
        pytest.skip(
            "filesystem read-only sql_client requires jsonl or parquet; got"
            f" {destination_config.file_format}"
        )
