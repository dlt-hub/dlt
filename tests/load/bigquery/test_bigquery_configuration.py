from typing import Optional

import pytest

from dlt.common.configuration.specs import GcpServiceAccountCredentials
from dlt.common.utils import digest128
from dlt.destinations.impl.bigquery.bigquery import BigQueryClientConfiguration

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


def _credentials(project_id: str) -> GcpServiceAccountCredentials:
    credentials = GcpServiceAccountCredentials()
    credentials.project_id = project_id
    return credentials


@pytest.mark.parametrize(
    "credentials_project_id,expected_fingerprint",
    [
        pytest.param(None, "", id="empty"),
        pytest.param(
            "credentials-project",
            digest128("credentials-project"),
            id="legacy_credentials_project_id",
        ),
    ],
)
def test_bigquery_fingerprint(
    credentials_project_id: Optional[str], expected_fingerprint: str
) -> None:
    credentials = _credentials(credentials_project_id) if credentials_project_id else None
    config = BigQueryClientConfiguration(credentials=credentials)

    assert config.fingerprint() == expected_fingerprint


def test_bigquery_fingerprint_uses_credentials_project_id_not_config_project_id() -> None:
    config = BigQueryClientConfiguration(
        credentials=_credentials("credentials-project"),
        project_id="configured-project",
        location="EU",
    )

    assert config.physical_location() == "EU"
    assert config.fingerprint() == digest128("credentials-project")
