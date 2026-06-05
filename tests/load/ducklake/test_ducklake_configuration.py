from typing import Optional

import pytest

from dlt.common.utils import digest128
from dlt.destinations.impl.ducklake.configuration import (
    DuckLakeClientConfiguration,
    DuckLakeCredentials,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "credentials,expected_fingerprint",
    [
        pytest.param(None, "", id="empty"),
        pytest.param(
            DuckLakeCredentials(storage="ducklake.files"),
            digest128(""),
            id="storage_local",
        ),
        pytest.param(
            DuckLakeCredentials(
                "my_ducklake",
                catalog="postgresql://loader:loader@localhost:5432/dlt_data",
                storage="s3://dlt-ci-test-bucket/lake",
            ),
            digest128("s3://dlt-ci-test-bucket"),
            id="storage_remote_bucket_only",
        ),
    ],
)
def test_ducklake_fingerprint(
    credentials: Optional[DuckLakeCredentials], expected_fingerprint: str
) -> None:
    config = DuckLakeClientConfiguration(credentials=credentials)

    assert config.fingerprint() == expected_fingerprint


def test_ducklake_fingerprint_uses_storage_not_physical_location() -> None:
    config = DuckLakeClientConfiguration(
        credentials=DuckLakeCredentials(
            "my_ducklake",
            catalog="postgresql://loader:loader@localhost:5432/dlt_data",
            storage="s3://dlt-ci-test-bucket/lake",
        )
    )

    assert config.physical_location() == "postgres://localhost:5432/dlt_data#my_ducklake"
    assert config.fingerprint() == digest128("s3://dlt-ci-test-bucket")
    assert config.fingerprint() != digest128(config.physical_location())
