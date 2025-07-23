import base64
import string
import re
from typing import Sequence, Set, Optional, Any, Dict, Tuple

from dlt.common import logger
from dlt.common.json import json
from dlt.common.configuration.specs import GcpServiceAccountCredentials
from dlt.common.configuration.exceptions import ConfigProviderException
from dlt.common.exceptions import MissingDependencyException
from .vault import VaultDocProvider
from .provider import get_key_name

# Create a translation table to replace punctuation with ""
# since google secrets allow "-"" and "_" we need to exclude them
punctuation = "".join(set(string.punctuation) - {"-", "_"})
translator = str.maketrans("", "", punctuation)


def normalize_key(in_string: str) -> str:
    """Replaces punctuation characters in a string

    Note: We exclude `_` and `-` from punctuation characters

    Args:
        in_string(str): input string

    Returns:
        (str): a string without punctuation characters and whitespaces
    """

    # Strip punctuation from the string
    stripped_text = in_string.translate(translator)
    whitespace = re.compile(r"\s+")
    stripped_whitespace = whitespace.sub("", stripped_text)
    return stripped_whitespace


class GoogleSecretsProvider(VaultDocProvider):
    def __init__(
        self,
        credentials: GcpServiceAccountCredentials,
        only_secrets: bool = True,
        only_toml_fragments: bool = True,
        list_secrets: bool = False,
    ) -> None:
        """Initialize a Google Secrets Provider to access secrets stored in Google Secret Manager

        Args:
            credentials: Google Cloud credentials to access Secret Manager
            only_secrets: When True, only keys with secret hint types will be looked up
            only_toml_fragments: When True, only load known TOML fragments and ignore other lookups
            list_secrets: When True, list all secrets upfront to optimize vault access by
                          avoiding lookups for non-existent secrets. Requires additional
                          API calls and roles/secretmanager.secretViewer permission.
        """
        self.credentials = credentials
        super().__init__(only_secrets, only_toml_fragments, list_secrets)

    @staticmethod
    def get_key_name(key: str, *sections: str) -> str:
        """Make key name for the secret

        Per Google the secret name can contain, so we will use snake_case normalizer

            1. Uppercase and lowercase letters,
            2. Numerals,
            3. Hyphens,
            4. Underscores.
        """
        key = normalize_key(key)
        normalized_sections = [normalize_key(section) for section in sections if section]
        key_name = get_key_name(normalize_key(key), "-", *normalized_sections)
        return key_name

    @property
    def name(self) -> str:
        return "Google Secrets"

    @property
    def locations(self) -> Sequence[str]:
        if self.credentials:
            return [str(self.credentials)]
        else:
            return super().locations

    def _get_google_client(self, service_name: str = "secretmanager", version: str = "v1") -> Any:
        try:
            from googleapiclient.discovery import build
        except ModuleNotFoundError:
            raise MissingDependencyException(
                "GoogleSecretsProvider",
                ["google-api-python-client"],
                "We need google-api-python-client to build client for secretmanager v1",
            )

        return build(service_name, version, credentials=self.credentials.to_native_credentials())

    def _handle_http_error(self, error: Any) -> Tuple[Dict[str, Any], int, str, str]:
        error_doc = json.loadb(error.content)["error"]
        error_message = error_doc.get("message", "Unknown error")
        error_status = error_doc.get("status", "Unknown status")
        status_code = error.resp.status

        return error_doc, status_code, error_message, error_status

    def _look_vault(self, full_key: str, hint: type) -> Optional[str]:
        resource_name = f"projects/{self.credentials.project_id}/secrets/{full_key}/versions/latest"
        client = self._get_google_client()

        from googleapiclient.errors import HttpError

        try:
            response = client.projects().secrets().versions().access(name=resource_name).execute()
            secret_value = response["payload"]["data"]
            decoded_value = base64.b64decode(secret_value).decode("utf-8")
            return decoded_value
        except HttpError as error:
            _, status_code, error_message, error_status = self._handle_http_error(error)

            if status_code == 404:
                # logger.warning(f"{self.credentials.client_email} has roles/secretmanager.secretAccessor role but {full_key} not found in Google Secrets: {error_message}[{error_status}]")
                return None
            elif status_code == 403:
                logger.warning(
                    f"{self.credentials.client_email} does not have"
                    " roles/secretmanager.secretAccessor role. It also does not have read"
                    f" permission to {full_key} or the key is not found in Google Secrets:"
                    f" {error_message}[{error_status}]"
                )
                return None
            elif status_code == 400:
                logger.warning(f"Unable to read {full_key} : {error_message}[{error_status}]")
                return None
            raise

    def _list_vault(self) -> Set[str]:
        """Lists all available secrets in Google Secret Manager

        This method is called when list_secrets is enabled to prefetch all available
        secret names, which helps avoid unnecessary API calls for non-existent secrets.

        Returns:
            Set[str]: A set of available key names in the vault
        """
        available_keys: Set[str] = set()
        client = self._get_google_client()

        from googleapiclient.errors import HttpError

        try:
            parent = f"projects/{self.credentials.project_id}"
            request = client.projects().secrets().list(parent=parent)

            while request is not None:
                response = request.execute()
                secrets = response.get("secrets", [])

                for secret in secrets:
                    # Extract secret name from full resource path
                    name = secret.get("name", "").split("/")[-1]
                    if name:
                        available_keys.add(name)

                request = client.projects().secrets().list_next(request, response)

            logger.info(f"Listed {len(available_keys)} secrets from Google Secret Manager")
            return available_keys

        except HttpError as error:
            _, status_code, error_message, error_status = self._handle_http_error(error)

            if status_code == 403:
                raise ConfigProviderException(
                    self.name,
                    f"Cannot list secrets: `{self.credentials.client_email}` does not have "
                    "roles/secretmanager.secretViewer role. Secret listing is required when "
                    "list_secrets=True to optimize vault access by skipping lookups for "
                    f"non-existent secrets. Error: {error_message} [{error_status}]",
                )
            else:
                raise ConfigProviderException(
                    self.name,
                    "Failed to list secrets in Google Secret Manager. Secret listing is required"
                    " when list_secrets=True to optimize vault access by skipping lookups for"
                    f" non-existent secrets. Error: {error_message} [{error_status}]",
                )
