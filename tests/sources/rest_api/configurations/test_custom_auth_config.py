from base64 import b64encode
from typing import Any, Dict, cast

import pytest

from dlt.sources import rest_api
from dlt.sources.helpers.rest_client.auth import APIKeyAuth, OAuth2ClientCredentials
from dlt.sources.rest_api.typing import ApiKeyAuthConfig, AuthConfig


class CustomOAuth2(OAuth2ClientCredentials):
    def build_access_token_request(self) -> Dict[str, Any]:
        """Used e.g. by Zoom Zoom Video Communications, Inc."""
        authentication: str = b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        return {
            "headers": {
                "Authorization": f"Basic {authentication}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            "data": self.access_token_request_data,
        }


class TestCustomAuth:
    @pytest.fixture
    def custom_auth_config(self) -> AuthConfig:
        config: AuthConfig = {
            "type": "custom_oauth_2",  # type: ignore
            "access_token_url": "https://example.com/oauth/token",
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "access_token_request_data": {
                "grant_type": "account_credentials",
                "account_id": "test_account_id",
            },
        }
        return config

    def test_creates_builtin_auth_without_registering(self) -> None:
        config: ApiKeyAuthConfig = {
            "type": "api_key",
            "api_key": "test-secret",
            "location": "header",
        }
        auth = cast(APIKeyAuth, rest_api.config_setup.create_auth(config))
        assert auth.api_key == "test-secret"

    def test_not_registering_throws_error(self, custom_auth_config: AuthConfig) -> None:
        with pytest.raises(ValueError) as e:
            rest_api.config_setup.create_auth(custom_auth_config)

        assert e.match("Invalid authentication: custom_oauth_2.")

    def test_registering_adds_to_AUTH_MAP(self, custom_auth_config: AuthConfig) -> None:
        rest_api.config_setup.register_auth("custom_oauth_2", CustomOAuth2)
        cls = rest_api.config_setup.get_auth_class("custom_oauth_2")
        assert cls is CustomOAuth2

        # teardown test
        del rest_api.config_setup.AUTH_MAP["custom_oauth_2"]

    def test_registering_allows_usage(self, custom_auth_config: AuthConfig) -> None:
        rest_api.config_setup.register_auth("custom_oauth_2", CustomOAuth2)
        auth = cast(CustomOAuth2, rest_api.config_setup.create_auth(custom_auth_config))
        request = auth.build_access_token_request()
        assert request["data"]["account_id"] == "test_account_id"

        # teardown test
        del rest_api.config_setup.AUTH_MAP["custom_oauth_2"]

    def test_registering_not_auth_config_base_throws_error(self) -> None:
        class NotAuthConfigBase:
            pass

        with pytest.raises(ValueError) as e:
            rest_api.config_setup.register_auth(
                "not_an_auth_config_base", NotAuthConfigBase  # type: ignore
            )
        assert e.match("Invalid auth: NotAuthConfigBase.")
