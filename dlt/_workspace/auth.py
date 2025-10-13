from auth_client.api.default import github_oauth_complete
from pydantic import BaseModel, ValidationError
import jwt

from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.exceptions import RuntimeNotAuthenticated
from dlt.common.configuration.providers.toml import SecretsTomlProvider


import os

from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.exceptions import RuntimeNotAuthenticated

from dlt.cli.config_toml_writer import WritableConfigValue, write_values
from dlt.common.configuration.providers.toml import SecretsTomlProvider
from dlt.common.configuration.specs.runtime_configuration import RuntimeConfiguration



class AuthInfo(BaseModel):
    user_id: str
    email: str


class AuthService:
    workspace_run_context: WorkspaceRunContext
    auth_info: AuthInfo | None = None

    def __init__(self, workspace_run_context: WorkspaceRunContext):
        self.workspace_run_context = workspace_run_context

    def authenticate(self) -> AuthInfo:
        secrets = SecretsTomlProvider(settings_dir=self.workspace_run_context.global_dir)
        token, _ = secrets.get_value("dlthub_runtime_auth_token", str, None, RuntimeConfiguration.__section__)
        if not token:
            print("No token found")
            raise RuntimeNotAuthenticated()
        self.auth_info = self._validate_and_decode_jwt(token)
        return self.auth_info

    def save_token(self, token_response: github_oauth_complete.LoginResponse) -> AuthInfo:
        self.auth_info = self._validate_and_decode_jwt(token_response.jwt)
        telemetry_value = [
            WritableConfigValue("dlthub_runtime_auth_token", str, token_response.jwt, (RuntimeConfiguration.__section__,))
        ]
        # write global config
        global_path = self.workspace_run_context.global_dir
        os.makedirs(global_path, exist_ok=True)
        secrets = SecretsTomlProvider(settings_dir=global_path)
        write_values(secrets._config_toml, telemetry_value, overwrite_existing=True)
        secrets.write_toml()
        return self.auth_info
    
    def _validate_and_decode_jwt(self, token: str | bytes) -> AuthInfo:
        if isinstance(token, str):
            token = token.encode("utf-8")
        try:
            payload = jwt.decode(token, options={"verify_signature": False})
        except jwt.PyJWTError as e:
            print("Failed to decode JWT: ", e)
            raise RuntimeNotAuthenticated()

        try:
            auth_info = AuthInfo(**payload)
        except ValidationError as e:
            print("Failed to validate JWT payload: ", e)
            raise RuntimeNotAuthenticated()

        return auth_info
