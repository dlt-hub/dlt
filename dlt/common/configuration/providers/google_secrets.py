import base64

from dlt.common import json
from dlt.common.configuration.specs import GcpServiceAccountCredentials
from dlt.common.exceptions import MissingDependencyException

from .toml import VaultTomlProvider
from .provider import get_key_name


class GoogleSecretsProvider(VaultTomlProvider):
    def __init__(self, credentials: GcpServiceAccountCredentials, only_secrets: bool = True, only_toml_fragments: bool = True) -> None:
        self.credentials = credentials
        super().__init__(only_secrets, only_toml_fragments)

    @staticmethod
    def get_key_name(key: str, *sections: str) -> str:
        return get_key_name(key, "-", *sections)

    @property
    def name(self) -> str:
        return "Google Secrets"

    def _look_vault(self, full_key: str, hint: type) -> str:
        try:
            from googleapiclient.discovery import build
            from googleapiclient.errors import HttpError
        except ModuleNotFoundError:
            raise MissingDependencyException("GoogleSecretsProvider", ["google-api-python-client"], "We need google-api-python-client to build client for secretmanager v1")
        from dlt.common import logger

        resource_name = f"projects/{self.credentials.project_id}/secrets/{full_key}/versions/latest"
        client = build("secretmanager", "v1", credentials=self.credentials.to_native_credentials())
        try:
            response = client.projects().secrets().versions().access(name=resource_name).execute()
            secret_value = response["payload"]["data"]
            decoded_value = base64.b64decode(secret_value).decode("utf-8")
            return decoded_value
        except HttpError as error:
            error_doc = json.loadb(error.content)["error"]
            if error.resp.status == 404:
                # logger.warning(f"{self.credentials.client_email} has roles/secretmanager.secretAccessor role but {full_key} not found in Google Secrets: {error_doc['message']}[{error_doc['status']}]")
                return None
            elif error.resp.status == 403:
                logger.warning(f"{self.credentials.client_email} does not have roles/secretmanager.secretAccessor role. It also does not have read permission to {full_key} or the key is not found in Google Secrets: {error_doc['message']}[{error_doc['status']}]")
                return None
            elif error.resp.status == 400:
                logger.warning(f"Unable to read {full_key} : {error_doc['message']}[{error_doc['status']}]")
                return None
            raise

    # def _verify_secret_access(self) -> None:
    #     try:
    #         from googleapiclient.discovery import build
    #         from googleapiclient.errors import HttpError
    #     except ImportError:
    #         raise MissingDependencyException("GoogleSecretsProvider", ["google-api-python-client"], "We need google-api-python-client to build client for secretmanager v1")
    #     client = build("iam", "v1", credentials=self.credentials.to_native_credentials())
    #     resource_name = f"projects/-/serviceAccounts/{self.credentials.client_email}"
    #     response = client.projects().serviceAccounts().getIamPolicy(resource=resource_name).execute()
    #     bindings = response.get("bindings", [])

    #     has_required_role = False
    #     required_role = "roles/secretmanager.secretAccessor"

    #     for binding in bindings:
    #         if binding["role"] == required_role and f"serviceAccount:{self.credentials.client_email}" in binding["members"]:
    #             has_required_role = True
    #             break
    #     if not has_required_role:
    #         print("no secrets read access")