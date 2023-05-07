import base64
import contextlib
from typing import Any, Dict, Optional, Tuple

import tomlkit
from tomlkit.container import Container as TOMLContainer

from dlt.common import json, pendulum
from dlt.common.configuration.specs.base_configuration import is_secret_hint
from dlt.common.configuration.specs import GcpServiceAccountCredentials, known_sections
from dlt.common.configuration.utils import auto_cast
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import AnyType
from dlt.common.utils import update_dict_nested

from .toml import BaseTomlProvider
from .provider import get_key_name


class GoogleSecretsProvider(BaseTomlProvider):
    """Reads dlt secrets from Google Secret Manager

    To reduce number of calls to external vaults:
    - only keys with secret type hint (CredentialsConfiguration, TSecretValue) will be looked up by default.
    - provider gathers `toml` document fragments that contain source and destination credentials in path specified below
    - single values will not be retrieved, only toml fragments by default

    """

    def __init__(self, credentials: GcpServiceAccountCredentials, only_secrets: bool = True, only_toml_fragments: bool = True) -> None:
        """Initializes provider with service account credentials and loads `dlt_secrets_toml` as `toml` document.

        In order to enable service account to read google manager secrets:
        - enable Google Secrets Manager api in your project
        - add roles/secretmanager.secretAccessor to allow access to all secrets or enable access per secret

        """
        print("GoogleSecretsProvider INSTANCE CREATED!")
        self.only_secrets = only_secrets
        self.only_toml_fragments = only_toml_fragments
        self.credentials = credentials
        self._vault_lookups: Dict[str, pendulum.DateTime] = {}

        super().__init__(tomlkit.document())
        self._update_from_vault("dlt_secrets_toml", None, AnyType, None, ())

    @staticmethod
    def get_key_name(key: str, *sections: str) -> str:
        return get_key_name(key, "-", *sections)

    def get_value(self, key: str, hint: type, pipeline_name: str, *sections: str) -> Tuple[Optional[Any], str]:
        full_key = self.get_key_name(key, pipeline_name, *sections)

        value, _ = super().get_value(key, hint, pipeline_name, *sections)
        if value is None:

            # only secrets hints are handled
            if self.only_secrets and not is_secret_hint(hint) and hint is not AnyType:
                return None, full_key

            # generate auxiliary paths to get from vault
            for known_section in [known_sections.SOURCES, known_sections.DESTINATION]:

                def _look_at_idx(idx: int, full_path: Tuple[str, ...], pipeline_name: str) -> None:
                    lookup_key = full_path[idx]
                    lookup_sections = full_path[:idx]
                    lookup_fk = self.get_key_name(lookup_key, *lookup_sections)
                    self._update_from_vault(lookup_fk, lookup_key, AnyType, pipeline_name, lookup_sections)

                def _lookup_paths(pipeline_name_: str, known_section_: str) -> None:
                    with contextlib.suppress(ValueError):
                        full_path = sections + (key,)
                        if pipeline_name_:
                            full_path = (pipeline_name_,) + full_path
                        idx = full_path.index(known_section_)
                        _look_at_idx(idx, full_path, pipeline_name_)
                        # if there's element after index then also try it (destination name / source name)
                        if len(full_path) - 1 > idx:
                            _look_at_idx(idx + 1, full_path, pipeline_name_)

                # first query the shortest paths so the longer paths can override it
                _lookup_paths(None, known_section)  # check sources and sources.<source_name>
                if pipeline_name:
                    _lookup_paths(pipeline_name, known_section) # check <pipeline_name>.sources and <pipeline_name>.sources.<source_name>

        value, _ = super().get_value(key, hint, pipeline_name, *sections)
        # skip checking the exact path if we check only toml fragments
        if value is None and not self.only_toml_fragments:
            # look for key in the vault and update the toml document
            self._update_from_vault(full_key, key, hint, pipeline_name, sections)
            value, _ = super().get_value(key, hint, pipeline_name, *sections)

        # if value:
        #     print(f"GSM got value for {key} {pipeline_name}-{sections}")
        # else:
        #     print(f"GSM FAILED value for {key} {pipeline_name}-{sections}")
        return value, full_key

    @property
    def name(self) -> str:
        return "Google Secrets"

    @property
    def supports_secrets(self) -> bool:
        return True

    def _look_vault(self, full_key: str, hint: type) -> str:
        try:
            from googleapiclient.discovery import build
            from googleapiclient.errors import HttpError
        except ImportError:
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

    def _update_from_vault(self, full_key: str, key: str, hint: type, pipeline_name: str, sections: Tuple[str, ...]) -> None:
        if full_key in self._vault_lookups:
            return
        # print(f"tries '{key}' {pipeline_name} | {sections} at '{full_key}'")
        secret = self._look_vault(full_key, hint)
        self._vault_lookups[full_key] = pendulum.now()
        if secret is not None:
            self._add_node(secret, key, pipeline_name, sections)

    def _add_node(self, secret: str, key: str, pipeline_name: str, sections: Tuple[str, ...]) -> None:
        if pipeline_name:
            sections = (pipeline_name, ) + sections

        doc: Any = auto_cast(secret)
        if isinstance(doc, TOMLContainer):
            if key is None:
                self._toml = doc
            else:
                # always update the top document
                update_dict_nested(self._toml, doc)
        else:
            if key is None:
                raise ValueError("dlt_secrets_toml must contain toml document")

            master: TOMLContainer
            # descend from root, create tables if necessary
            master = self._toml
            for k in sections:
                if not isinstance(master, dict):
                    raise KeyError(k)
                if k not in master:
                    master[k] = tomlkit.table()
                master = master[k]  # type: ignore
            if isinstance(doc, dict):
                update_dict_nested(master[key], doc)  # type: ignore
            else:
                master[key] = doc

    @property
    def is_empty(self) -> bool:
        return False

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