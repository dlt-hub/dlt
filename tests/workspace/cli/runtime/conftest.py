import pytest

from dlt._workspace._workspace_context import active
from dlt._workspace.runtime import AuthInfo, RuntimeAuthService
from dlt._workspace.runtime_clients.api.client import Client as ApiClient


class _AuthServiceStub(RuntimeAuthService):
    def __init__(self, workspace_id: str = "11111111-2222-3333-4444-555555555555") -> None:
        super().__init__(run_context=active())
        # Set matching local/remote ids so property access does not raise
        self._local_workspace_id = workspace_id
        self._remote_workspace_id = workspace_id
        # Provide a dummy auth_info to satisfy potential header creation
        self.auth_info = AuthInfo(user_id="stub", email="stub@example.com", jwt_token="stub-token")


@pytest.fixture
def auth_service_stub() -> RuntimeAuthService:
    return _AuthServiceStub()


@pytest.fixture
def api_client_stub() -> ApiClient:
    # Minimal client instance; API calls are patched in tests, so this won't be used to make requests
    return ApiClient(base_url="http://localhost", verify_ssl=False, headers={})
