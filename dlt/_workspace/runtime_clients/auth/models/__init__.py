"""Contains all the data models used in inputs/outputs"""

from .github_device_flow_login_request import GithubDeviceFlowLoginRequest
from .github_device_flow_start_response import GithubDeviceFlowStartResponse
from .github_oauth_complete_response_400 import GithubOauthCompleteResponse400
from .github_oauth_complete_response_400_extra import GithubOauthCompleteResponse400Extra
from .login_response import LoginResponse
from .ping_response import PingResponse

__all__ = (
    "GithubDeviceFlowLoginRequest",
    "GithubDeviceFlowStartResponse",
    "GithubOauthCompleteResponse400",
    "GithubOauthCompleteResponse400Extra",
    "LoginResponse",
    "PingResponse",
)
