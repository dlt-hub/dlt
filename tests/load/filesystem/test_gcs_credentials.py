from typing import Any, Dict
import pytest

import dlt
from dlt.common.configuration.specs.exceptions import UnsupportedAuthenticationMethodException
from dlt.common.configuration.specs.gcp_credentials import (
    GcpOAuthCredentialsWithoutDefaults,
    GcpServiceAccountCredentialsWithoutDefaults,
)
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


def test_gcp_oauth_credentials_pyiceberg_export_import() -> None:
    """test that GCP OAuth credentials can be exported to PyIceberg config and imported back."""
    import dlt

    # get GCP OAuth credentials
    oauth_config: Dict[str, Any] = dlt.secrets.get("destination.fsgcpoauth.credentials")
    if not oauth_config:
        pytest.skip("GCP OAuth credentials not configured")

    # create original credentials
    original_creds = GcpOAuthCredentialsWithoutDefaults(
        project_id=oauth_config.get("project_id", "test-project"),
        client_id=oauth_config.get("client_id"),
        client_secret=oauth_config.get("client_secret"),
        refresh_token=oauth_config.get("refresh_token"),
    )

    # set a test token value to bypass auth flow in tests
    original_creds.token = "test-token"

    # export to PyIceberg config
    pyiceberg_config = original_creds.to_pyiceberg_fileio_config()

    # config should contain required fields with correct values
    assert "gcs.project-id" in pyiceberg_config
    assert pyiceberg_config["gcs.project-id"] == original_creds.project_id
    assert "gcs.oauth2.token" in pyiceberg_config
    assert pyiceberg_config["gcs.oauth2.token"] == original_creds.token
    assert "gcs.oauth2.token-expires-at" in pyiceberg_config

    # import back from PyIceberg config
    imported_creds = GcpOAuthCredentialsWithoutDefaults.from_pyiceberg_fileio_config(
        pyiceberg_config
    )

    # verify credentials were restored correctly
    assert imported_creds.project_id == original_creds.project_id
    assert imported_creds.token == original_creds.token


def test_gcp_service_account_credentials_pyiceberg_not_supported() -> None:
    """test that GCP Service Account credentials raise the expected exception with PyIceberg."""
    import dlt

    # get GCP Service Account credentials
    sa_config: Dict[str, Any] = dlt.secrets.get("destination.filesystem.credentials")
    if not sa_config.get("private_key"):
        pytest.skip("GCP Service Account credentials not configured")

    # create service account credentials
    sa_creds = GcpServiceAccountCredentialsWithoutDefaults(
        project_id=sa_config["project_id"],
        private_key=sa_config["private_key"],
        private_key_id=sa_config["private_key_id"],
        client_email=sa_config["client_email"],
    )

    # should raise exception when calling to_pyiceberg_fileio_config
    with pytest.raises(UnsupportedAuthenticationMethodException):
        sa_creds.to_pyiceberg_fileio_config()

    # should also raise exception when calling from_pyiceberg_fileio_config
    with pytest.raises(UnsupportedAuthenticationMethodException):
        GcpServiceAccountCredentialsWithoutDefaults.from_pyiceberg_fileio_config({})
