from datetime import datetime, timezone
from functools import partial
from typing import Any, Callable, Dict
from unittest.mock import MagicMock, patch
import pytest

import dlt
from dlt.common.configuration.specs.exceptions import UnsupportedAuthenticationMethodException
from dlt.common.configuration.specs.mixins import WithPyicebergConfig
from dlt.common.configuration.specs.gcp_credentials import (
    GcpOAuthCredentialsWithoutDefaults,
    GcpServiceAccountCredentials,
    GcpServiceAccountCredentialsWithoutDefaults,
)
from dlt.destinations import filesystem
from dlt.sources.credentials import GcpOAuthCredentials
from tests.load.utils import ALL_FILESYSTEM_DRIVERS
from tests.utils import local_timezone

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

    # export to PyIceberg config, the refresh token mints the access token
    pyiceberg_config = original_creds.to_pyiceberg_fileio_config()

    # config should contain required fields with correct values
    assert "gcs.project-id" in pyiceberg_config
    assert pyiceberg_config["gcs.project-id"] == original_creds.project_id
    token = pyiceberg_config["gcs.oauth2.token"]
    assert token
    assert "gcs.oauth2.token-expires-at" in pyiceberg_config

    # import back from PyIceberg config
    imported_creds = GcpOAuthCredentialsWithoutDefaults.from_pyiceberg_fileio_config(
        pyiceberg_config
    )

    # verify credentials were restored correctly
    assert imported_creds.project_id == original_creds.project_id
    assert imported_creds.token == token
    # the imported token has no refresh token, so it is passed on as is
    assert imported_creds.to_gcs_bearer_token(cached=False) == (token, None)
    imported_config = imported_creds.to_pyiceberg_fileio_config()
    assert imported_config["gcs.oauth2.token"] == token
    assert "gcs.oauth2.token-expires-at" in imported_config


def test_gcp_service_account_credentials_pyiceberg_export() -> None:
    """test that GCP Service Account credentials can be exported to PyIceberg config."""
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

    # export to PyIceberg config - now this should work
    pyiceberg_config = sa_creds.to_pyiceberg_fileio_config()

    # config should contain required fields
    assert "gcs.project-id" in pyiceberg_config
    assert pyiceberg_config["gcs.project-id"] == sa_creds.project_id
    assert "gcs.oauth2.token" in pyiceberg_config
    assert pyiceberg_config["gcs.oauth2.token"] is not None

    # from_pyiceberg_fileio_config should still raise exception for service account
    with pytest.raises(UnsupportedAuthenticationMethodException):
        GcpServiceAccountCredentialsWithoutDefaults.from_pyiceberg_fileio_config({})


@pytest.mark.parametrize(
    "creds_factory",
    [
        partial(GcpServiceAccountCredentialsWithoutDefaults, project_id="project"),
        partial(GcpOAuthCredentialsWithoutDefaults, project_id="project", refresh_token="refresh"),
    ],
    ids=["service_account", "oauth"],
)
def test_gcp_pyiceberg_token(creds_factory: Callable[[], WithPyicebergConfig]) -> None:
    """pyiceberg keeps the token, so it gets a new one. The token expiry does not depend on the
    local timezone."""
    creds = creds_factory()
    # google-auth keeps the expiry as naive utc
    native = MagicMock(valid=True, token="tok", expiry=datetime(2030, 1, 1, 12))
    with patch.object(type(creds), "to_native_credentials", return_value=native):
        with local_timezone("Europe/Berlin"):
            config = creds.to_pyiceberg_fileio_config()

    # the token was valid, still pyiceberg gets a new one
    native.refresh.assert_called_once()
    assert config["gcs.oauth2.token"] == "tok"
    expires_at = datetime(2030, 1, 1, 12, tzinfo=timezone.utc)
    assert config["gcs.oauth2.token-expires-at"] == str(int(expires_at.timestamp() * 1000))


@pytest.mark.parametrize(
    "creds_cls",
    [GcpServiceAccountCredentials, GcpOAuthCredentials],
    ids=["service_account", "oauth"],
)
def test_gcp_pyiceberg_hands_over_default(creds_cls: Any) -> None:
    """Default (ADC) credentials are handed over to pyarrow's GcsFileSystem, which refreshes via
    Google ADC - no frozen oauth token is emitted (it would otherwise expire on long loads)."""
    creds = creds_cls()
    creds.project_id = "test-project"
    # handover does not touch the credential, so a placeholder is enough
    creds._set_default_credentials(object())
    assert creds.has_default_credentials()

    config = creds.to_pyiceberg_fileio_config()
    assert config == {"gcs.project-id": "test-project"}
