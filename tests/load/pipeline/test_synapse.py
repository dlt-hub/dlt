from typing import Dict, Any, Union

import pytest

import dlt
from dlt.destinations import filesystem, synapse
from dlt.common.configuration.specs.azure_credentials import (
    AzureCredentialsWithoutDefaults,
    AzureServicePrincipalCredentialsWithoutDefaults,
)

from tests.utils import skip_if_not_active
from tests.pipeline.utils import assert_load_info
from tests.load.utils import AZ_BUCKET


skip_if_not_active("synapse")


@pytest.mark.parametrize("credentials_type", ("sas", "service_principal", "managed_identity"))
def test_copy_file_load_job_credentials(credentials_type: str) -> None:
    staging_creds: Union[
        AzureCredentialsWithoutDefaults, AzureServicePrincipalCredentialsWithoutDefaults
    ]
    if credentials_type == "service_principal":
        staging_creds = AzureServicePrincipalCredentialsWithoutDefaults(
            **dlt.secrets.get("destination.fsazureprincipal.credentials")
        )
    else:
        FS_CREDS: Dict[str, Any] = dlt.secrets.get("destination.filesystem.credentials")
        staging_creds = AzureCredentialsWithoutDefaults(
            azure_storage_account_name=FS_CREDS["azure_storage_account_name"],
            azure_storage_account_key=FS_CREDS["azure_storage_account_key"],
        )

    pipeline = dlt.pipeline(
        staging=filesystem(bucket_url=AZ_BUCKET, credentials=staging_creds),
        destination=synapse(
            staging_use_msi=(True if credentials_type == "managed_identity" else False)
        ),
    )

    info = pipeline.run([{"foo": "bar"}], table_name="abstract")
    assert_load_info(info)
