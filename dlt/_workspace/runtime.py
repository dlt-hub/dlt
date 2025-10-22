import os
from typing import Optional

import jwt
from git import Union
from pydantic import BaseModel, ValidationError
from tomlkit.toml_file import TOMLFile

from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.configuration import WorkspaceRuntimeConfiguration
from dlt._workspace.exceptions import (
    LocalWorkspaceIdNotSet,
    RuntimeNotAuthenticated,
    RuntimeOperationNotAuthorized,
    WorkspaceIdMismatch,
    WorkspaceRunContextNotAvailable,
)
from dlt._workspace.runtime_clients.api.api.me import me
from dlt._workspace.runtime_clients.api.client import Client as ApiClient
from dlt._workspace.runtime_clients.api.models.me_response import MeResponse
from dlt._workspace.runtime_clients.auth.client import Client as AuthClient
from dlt._workspace.cli.config_toml_writer import WritableConfigValue, write_values
from dlt.common.configuration.providers.toml import (
    SECRETS_TOML,
    ConfigTomlProvider,
    SecretsTomlProvider,
)
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs.pluggable_run_context import RunContextBase
from dlt.common.runtime.run_context import active


class AuthInfo(BaseModel):
    user_id: str
    email: str
    jwt_token: str


class RuntimeAuthService:
    """
    Implements login, logout and auth check internals

    Authentication is performed based on the JWT token stored in the global secrets. On top of that,
    authorization uses organisation and workspace id stored in the local config. For that, depending on the usage,
    either workspace run context or base run context is required.
    """

    auth_info: Optional[AuthInfo] = None

    _run_context: RunContextBase
    _local_workspace_id: Optional[str] = None
    _remote_workspace_id: Optional[str] = None

    def __init__(self, run_context: RunContextBase):
        self._run_context = run_context

    @property
    def workspace_run_context(self) -> WorkspaceRunContext:
        if isinstance(self._run_context, WorkspaceRunContext):
            return self._run_context
        else:
            raise WorkspaceRunContextNotAvailable(self._run_context.run_dir)

    @property
    def run_context(self) -> RunContextBase:
        return self._run_context

    @property
    def workspace_id(self) -> str:
        if not self._remote_workspace_id or self._remote_workspace_id != self._local_workspace_id:
            raise RuntimeOperationNotAuthorized()
        return self._remote_workspace_id

    def authenticate(self) -> AuthInfo:
        self._read_token()
        return self.auth_info

    def login(self, token: str) -> AuthInfo:
        self._save_token(token)
        return self.auth_info

    def logout(self) -> None:
        self._delete_token()
        self._remote_workspace_id = None

    def authorize(self) -> str:
        # Currently, ensuring workspace id is the same as default workspace id of the user
        if not self._remote_workspace_id:
            client = get_api_client(self)
            me_response = me.sync(client=client)

            if isinstance(me_response, MeResponse):
                self._remote_workspace_id = str(me_response.default_workspace.id)
            else:
                raise RuntimeError("Failed to get me response")

        config = resolve_configuration(WorkspaceRuntimeConfiguration())
        self._local_workspace_id = config.workspace_id

        if not self._local_workspace_id:
            raise LocalWorkspaceIdNotSet(self._remote_workspace_id)
        elif self._local_workspace_id != self._remote_workspace_id:
            raise WorkspaceIdMismatch(self._local_workspace_id, self._remote_workspace_id)

        return self.workspace_id

    def overwrite_local_workspace_id(self) -> None:
        local_toml_config = ConfigTomlProvider(self.workspace_run_context.settings_dir)
        local_toml_config.set_value(
            "workspace_id",
            str(self._remote_workspace_id),
            None,
            WorkspaceRuntimeConfiguration.__section__,
        )
        local_toml_config.write_toml()
        self._local_workspace_id = self._remote_workspace_id

    def _read_token(self) -> AuthInfo:
        config = resolve_configuration(WorkspaceRuntimeConfiguration())
        if not config.auth_token:
            raise RuntimeNotAuthenticated("No token found")
        self.auth_info = self._validate_and_decode_jwt(config.auth_token)
        return self.auth_info

    def _save_token(self, token: str) -> AuthInfo:
        self.auth_info = self._validate_and_decode_jwt(token)
        value = [
            WritableConfigValue(
                "auth_token", str, token, (WorkspaceRuntimeConfiguration.__section__,)
            )
        ]
        # write global secrets
        global_path = self.run_context.global_dir
        os.makedirs(global_path, exist_ok=True)
        secrets = SecretsTomlProvider(settings_dir=global_path)
        write_values(secrets._config_toml, value, overwrite_existing=True)
        secrets.write_toml()
        return self.auth_info

    def _delete_token(self) -> None:
        # delete from global secrets directly, because in other cases config deletion is not supported
        secrets_path = os.path.join(self.run_context.global_dir, SECRETS_TOML)
        if not os.path.isfile(secrets_path):
            return
        toml = TOMLFile(secrets_path)
        doc = toml.read()

        # Safely check for structure and remove the key
        runtime_section = doc.get(WorkspaceRuntimeConfiguration.__section__)
        if not isinstance(runtime_section, dict):
            return
        if "auth_token" not in runtime_section:
            return
        runtime_section.pop("auth_token")
        toml.write(doc)

    def _validate_and_decode_jwt(self, token: Union[str, bytes]) -> AuthInfo:
        if isinstance(token, str):
            token = token.encode("utf-8")
        try:
            payload = jwt.decode(token, options={"verify_signature": False})
        except jwt.PyJWTError as e:
            raise RuntimeNotAuthenticated("Failed to decode JWT") from e

        try:
            auth_info = AuthInfo(jwt_token=token.decode("utf-8"), **payload)
        except ValidationError as e:
            raise RuntimeNotAuthenticated("Failed to validate JWT payload") from e

        return auth_info


def get_auth_client() -> AuthClient:
    config = resolve_configuration(WorkspaceRuntimeConfiguration())
    return AuthClient(base_url=config.auth_base_url, verify_ssl=False)


def get_api_client(auth_service: Optional["RuntimeAuthService"] = None) -> ApiClient:
    config = resolve_configuration(WorkspaceRuntimeConfiguration())
    if auth_service is None:
        auth_service = RuntimeAuthService(run_context=active())
        auth_service.authenticate()

    return ApiClient(
        base_url=config.api_base_url,
        verify_ssl=False,
        headers={"Authorization": f"Bearer {auth_service.auth_info.jwt_token}"},
    )
