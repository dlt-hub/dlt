# https://learn.microsoft.com/en-us/fabric/data-warehouse/service-principals#token-renewal-and-initialization-requirements

import os

import requests

from azure.identity import ClientSecretCredential

import dlt
from dlt.destinations.impl.fabric.configuration import FabricCredentials


def main() -> None:
    # get SPN credentials
    os.environ["DLT_PROJECT_DIR"] = "tests"
    credentials = dlt.secrets.get("destination.fabric.credentials", FabricCredentials)
    if credentials is None:
        raise RuntimeError("`destination.fabric.credentials` is not configured.")

    # get bearer token
    token = (
        ClientSecretCredential(
            tenant_id=credentials.azure_tenant_id,
            client_id=credentials.azure_client_id,
            client_secret=credentials.azure_client_secret,
        )
        .get_token("https://api.fabric.microsoft.com/.default")
        .token
    )

    # call public Fabric API endpoint to refresh token (any endpoint will do)
    response = requests.get(
        "https://api.fabric.microsoft.com/v1/workspaces",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=30,
    )
    response.raise_for_status()
    print("Refreshed Fabric SPN token successfully.")


if __name__ == "__main__":
    main()
