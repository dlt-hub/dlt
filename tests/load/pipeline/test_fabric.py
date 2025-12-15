"""Integration tests for Fabric Warehouse destination"""
from typing import Dict, Any, Union

import pytest

import dlt
from dlt.destinations import filesystem, fabric
from dlt.common.configuration.specs.azure_credentials import (
    AzureCredentialsWithoutDefaults,
    AzureServicePrincipalCredentialsWithoutDefaults,
)

from tests.utils import skip_if_not_active
from tests.pipeline.utils import assert_load_info
from tests.load.utils import AZ_BUCKET


skip_if_not_active("fabric")


@pytest.mark.skip(reason="Requires Azure Blob Storage setup - OneLake only in this environment")
def test_copy_file_load_job_credentials() -> None:
    """Test COPY INTO with Azure Blob staging - skipped as we use OneLake"""
    pass


def test_copy_into_onelake_service_principal() -> None:
    """Test COPY INTO with OneLake staging using Service Principal credentials
    
    Note: OneLake has an API quirk where delete operations return HTTP 200 instead of HTTP 202,
    which the Azure SDK treats as an error. The filesystem client now handles this gracefully.
    """
    # Get Fabric warehouse and filesystem credentials
    fabric_creds: Dict[str, Any] = dlt.secrets.get("destination.fabric.credentials")
    fs_creds: Dict[str, Any] = dlt.secrets.get("destination.filesystem.credentials")

    # OneLake bucket URL format: abfss://workspace_guid@onelake.dfs.fabric.microsoft.com/lakehouse_guid/Files/path
    onelake_bucket: str = dlt.secrets.get("destination.fabric.onelake_bucket_url")

    # OneLake staging with Service Principal authentication
    pipeline = dlt.pipeline(
        staging=filesystem(
            bucket_url=onelake_bucket,
            credentials=AzureServicePrincipalCredentialsWithoutDefaults(
                azure_storage_account_name=fs_creds["azure_storage_account_name"],
                azure_account_host=fs_creds["azure_account_host"],
                azure_tenant_id=fs_creds["azure_tenant_id"],
                azure_client_id=fs_creds["azure_client_id"],
                azure_client_secret=fs_creds["azure_client_secret"],
            ),
        ),
        destination=fabric(credentials=fabric_creds),
    )

    # Test with simple data using 'replace' disposition to handle reruns
    info = pipeline.run(
        [{"id": 1, "name": "test"}],
        table_name="onelake_test",
        write_disposition="replace",
    )
    assert_load_info(info)


@pytest.mark.skip(reason="Requires Azure Blob Storage with SAS token - not available")
def test_copy_into_azure_blob_sas() -> None:
    """Test COPY INTO with Azure Blob Storage staging using SAS token - skipped"""
    pass


@pytest.mark.skip(reason="Requires Azure Blob Storage setup - OneLake only in this environment")
def test_copy_into_azure_blob_service_principal() -> None:
    """Test COPY INTO with Azure Blob Storage staging using Service Principal - skipped"""
    pass


@pytest.mark.skip(reason="Requires Azure Blob Storage with SAS token - not available")
def test_fabric_varchar_with_copy_into() -> None:
    """Test that Fabric uses varchar (not nvarchar) when loading via COPY INTO - skipped"""
    pass
