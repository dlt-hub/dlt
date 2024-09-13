import pytest

import dlt
from dlt.destinations import filesystem
from dlt.sources.credentials import GcpOAuthCredentials
from tests.load.utils import ALL_FILESYSTEM_DRIVERS

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential

if "gs" not in ALL_FILESYSTEM_DRIVERS:
    pytest.skip("gcs filesystem driver not configured", allow_module_level=True)


def test_explicit_filesystem_credentials() -> None:
    # resolve gcp oauth
    p = dlt.pipeline(
        pipeline_name="postgres_pipeline",
        destination=filesystem(
            "gcs://test",
            destination_name="uniq_gcs_bucket",
            credentials={
                "project_id": "pxid",
                "refresh_token": "123token",
                "client_id": "cid",
                "client_secret": "s",
            },
        ),
    )
    config = p.destination_client().config
    assert config.credentials.is_resolved()
    assert isinstance(config.credentials, GcpOAuthCredentials)
