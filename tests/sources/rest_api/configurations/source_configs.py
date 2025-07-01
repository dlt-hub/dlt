from collections import namedtuple
from typing import cast, List

import dlt
import dlt.common
from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.typing import TSecretStrValue
from dlt.common.exceptions import DictValidationException
from dlt.common.configuration.specs import configspec

import dlt.sources.helpers
import dlt.sources.helpers.requests
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator
from dlt.sources.helpers.rest_client.auth import OAuth2AuthBase, APIKeyAuth

from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator
from dlt.sources.helpers.rest_client.auth import HttpBasicAuth

from dlt.sources.rest_api.typing import RESTAPIConfig


ConfigTest = namedtuple("ConfigTest", ["expected_message", "exception", "config"])

INVALID_CONFIGS = [
    ConfigTest(
        expected_message="missing required fields `{'resources'}`",
        exception=DictValidationException,
        config={"client": {"base_url": ""}},
    ),
    ConfigTest(
        expected_message="missing required fields `{'client'}`",
        exception=DictValidationException,
        config={"resources": []},
    ),
    # expect missing api_key at the right config section coming from the shorthand auth notation
    ConfigTest(
        expected_message="SOURCES__REST_API__INVALID_CONFIG__CREDENTIALS__API_KEY",
        exception=ConfigFieldMissingException,
        config={
            "client": {
                "base_url": "https://api.example.com",
                "auth": "api_key",
            },
            "resources": ["posts"],
        },
    ),
    # expect missing api_key at the right config section coming from the explicit auth config base
    ConfigTest(
        expected_message="SOURCES__REST_API__INVALID_CONFIG__CREDENTIALS__API_KEY",
        exception=ConfigFieldMissingException,
        config={
            "client": {
                "base_url": "https://api.example.com",
                "auth": APIKeyAuth(),
            },
            "resources": ["posts"],
        },
    ),
    # expect missing api_key at the right config section coming from the dict notation
    # TODO: currently this test fails on validation, api_key is necessary. validation happens
    #   before secrets are bound, this must be changed
    ConfigTest(
        expected_message=(
            "For `ApiKeyAuthConfig`: Path `./client/auth`: missing required fields `{'api_key'}`"
        ),
        exception=DictValidationException,
        config={
            "client": {
                "base_url": "https://api.example.com",
                "auth": {"type": "api_key", "location": "header"},
            },
            "resources": ["posts"],
        },
    ),
    ConfigTest(
        expected_message="Path `./client`: received unexpected fields `{'invalid_key'}`",
        exception=DictValidationException,
        config={
            "client": {
                "base_url": "https://api.example.com",
                "invalid_key": "value",
            },
            "resources": ["posts"],
        },
    ),
    ConfigTest(
        expected_message="field `paginator` expects the following types: ",
        exception=DictValidationException,
        config={
            "client": {
                "base_url": "https://api.example.com",
                "paginator": "invalid_paginator",
            },
            "resources": ["posts"],
        },
    ),
    ConfigTest(
        expected_message="issuess",
        exception=ValueError,
        config={
            "client": {"base_url": "https://github.com/api/v2"},
            "resources": [
                "issues",
                {
                    "name": "comments",
                    "endpoint": {
                        "path": "issues/{id}/comments",
                        "params": {
                            "id": {
                                "type": "resolve",
                                "resource": "issuess",
                                "field": "id",
                            },
                        },
                    },
                },
            ],
        },
    ),
    ConfigTest(
        expected_message="{org}/{repo}/issues/",
        exception=ValueError,
        config={
            "client": {"base_url": "https://github.com/api/v2"},
            "resources": [
                {"name": "issues", "endpoint": {"path": "{org}/{repo}/issues/"}},
                {
                    "name": "comments",
                    "endpoint": {
                        "path": "{org}/{repo}/issues/{id}/comments",
                        "params": {
                            "id": {
                                "type": "resolve",
                                "resource": "issues",
                                "field": "id",
                            },
                        },
                    },
                },
            ],
        },
    ),
]


class CustomPaginator(HeaderLinkPaginator):
    def __init__(self) -> None:
        super().__init__(links_next_key="prev")


@configspec
class CustomOAuthAuth(OAuth2AuthBase):
    pass


@dlt.resource(name="repositories", selected=False)
def repositories():
    """A seed list of repositories to fetch"""
    yield [{"name": "dlt"}, {"name": "verified-sources"}, {"name": "dlthub-education"}]


VALID_CONFIGS: List[RESTAPIConfig] = [
    # Using the resolve field syntax
    {
        "client": {"base_url": "https://api.example.com"},
        "resources": [
            "posts",
            {
                "name": "post_comments",
                "endpoint": {
                    "path": "posts/{post_id}/comments",
                    "params": {
                        "post_id": {
                            "type": "resolve",
                            "resource": "posts",
                            "field": "id",
                        },
                    },
                },
            },
        ],
    },
    # Using the resource field reference syntax
    {
        "client": {"base_url": "https://api.example.com"},
        "resources": [
            "posts",
            {
                "name": "post_comments",
                "endpoint": {
                    "path": "posts/{resources.posts.id}/comments",
                },
            },
        ],
    },
    # Using short, endpoint-only resource definition
    {
        "client": {"base_url": "https://api.example.com"},
        "resources": [
            "posts",
            {
                "name": "post_comments",
                "endpoint": "posts/{resources.posts.id}/comments",
            },
        ],
    },
    {
        "client": {"base_url": "https://api.example.com"},
        "resources": [
            {
                "name": "posts",
                "endpoint": {
                    "path": "posts",
                    "params": {
                        "limit": 100,
                    },
                    "paginator": "json_link",
                },
            },
        ],
    },
    {
        "client": {"base_url": "https://api.example.com"},
        "resources": [
            {
                "name": "posts",
                "endpoint": {
                    "path": "posts",
                    "params": {
                        "limit": 1,
                    },
                    "paginator": SinglePagePaginator(),
                },
            },
        ],
    },
    {
        "client": {
            "base_url": "https://example.com",
            "auth": {"type": "bearer", "token": "X"},
        },
        "resources": ["users"],
    },
    {
        "client": {
            "base_url": "https://example.com",
            "auth": {"token": "X"},
        },
        "resources": ["users"],
    },
    {
        "client": {
            "base_url": "https://example.com",
            "paginator": CustomPaginator(),
            "auth": CustomOAuthAuth(access_token=cast(TSecretStrValue, "X")),
        },
        "resource_defaults": {
            "table_name": lambda event: event["type"],
            "endpoint": {
                "paginator": CustomPaginator(),
                "params": {"since": dlt.sources.incremental[str]("user_id")},
            },
        },
        "resources": [
            {
                "name": "users",
                "endpoint": {
                    "paginator": CustomPaginator(),
                    "params": {"since": dlt.sources.incremental[str]("user_id")},
                },
            }
        ],
    },
    {
        "client": {
            "base_url": "https://example.com",
            "paginator": "header_link",
            "auth": HttpBasicAuth("my-secret", cast(TSecretStrValue, "")),
        },
        "resources": ["users"],
    },
    {
        "client": {"base_url": "https://api.example.com"},
        "resources": [
            {
                "name": "posts",
                "endpoint": {
                    "path": "posts",
                    "params": {
                        "limit": 100,
                        "since": {
                            "type": "incremental",
                            "cursor_path": "updated_at",
                            "initial_value": "2024-01-25T11:21:28Z",
                        },
                    },
                    "paginator": "json_link",
                },
            },
        ],
    },
    {
        "client": {"base_url": "https://api.example.com"},
        "resources": [
            {
                "name": "posts",
                "endpoint": {
                    "path": "posts",
                    "params": {
                        "limit": 100,
                    },
                    "paginator": "json_link",
                    "incremental": {
                        "start_param": "since",
                        "end_param": "until",
                        "cursor_path": "updated_at",
                        "initial_value": "2024-01-25T11:21:28Z",
                    },
                },
            },
        ],
    },
    {
        "client": {
            "base_url": "https://api.example.com",
            "headers": {
                "X-Test-Header": "test42",
            },
        },
        "resources": [
            "users",
            {"name": "users_2"},
            {"name": "users_list", "endpoint": "users_list"},
        ],
    },
    {
        "client": {"base_url": "https://api.example.com"},
        "resources": [
            "posts",
            {
                "name": "post_comments",
                "table_name": lambda item: item["type"],
                "endpoint": {
                    "path": "posts/{post_id}/comments",
                    "params": {
                        "post_id": {
                            "type": "resolve",
                            "resource": "posts",
                            "field": "id",
                        },
                    },
                },
            },
        ],
    },
    {
        "client": {"base_url": "https://github.com/api/v2"},
        "resources": [
            {
                "name": "issues",
                "endpoint": {
                    "path": "{org}/{repo}/issues/",
                    "params": {"org": "dlt-hub", "repo": "dlt"},
                },
            },
            {
                "name": "comments",
                "endpoint": {
                    "path": "{org}/{repo}/issues/{id}/comments",
                    "params": {
                        "org": "dlt-hub",
                        "repo": "dlt",
                        "id": {
                            "type": "resolve",
                            "resource": "issues",
                            "field": "id",
                        },
                    },
                },
            },
        ],
    },
    # Using the resolve field syntax
    {
        "client": {"base_url": "https://github.com/api/v2"},
        "resources": [
            {
                "name": "issues",
                "endpoint": {
                    "path": "dlt-hub/{repository}/issues/",
                    "params": {
                        "repository": {
                            "type": "resolve",
                            "resource": "repositories",
                            "field": "name",
                        },
                    },
                },
            },
            repositories(),
        ],
    },
    # Using the resource field reference syntax
    {
        "client": {"base_url": "https://github.com/api/v2"},
        "resources": [
            {
                "name": "issues",
                "endpoint": {
                    "path": "dlt-hub/{resources.repositories.name}/issues/",
                },
            },
            repositories(),
        ],
    },
    {
        "client": {"base_url": "https://github.com/api/v2"},
        "resources": [
            {
                "name": "issues",
                "endpoint": {
                    "path": "user/repos",
                    "auth": HttpBasicAuth("", "BASIC_AUTH_TOKEN"),
                },
            }
        ],
    },
]


# NOTE: leaves some parameters as defaults to test if they are set correctly
PAGINATOR_TYPE_CONFIGS = [
    {"type": "auto"},
    {"type": "single_page"},
    {"type": "page_number", "page": 10, "base_page": 1, "total_path": "response.pages"},
    {"type": "offset", "limit": 100, "maximum_offset": 1000},
    {"type": "header_link", "links_next_key": "next_page"},
    {"type": "header_cursor", "cursor_key": "X-Next-Cursor", "cursor_param": "cursor"},
    {"type": "json_link", "next_url_path": "response.nex_page_link"},
    {"type": "cursor", "cursor_param": "cursor"},
]


# NOTE: leaves some required parameters to inject them from config
AUTH_TYPE_CONFIGS = [
    {"type": "bearer", "token": "token"},
    {"type": "api_key", "location": "cookie"},
    {"type": "http_basic", "username": "username"},
    {
        "type": "oauth2_client_credentials",
        "access_token_url": "https://example.com/oauth/token",
        "access_token_request_data": {"foo": "bar"},
        "default_token_expiration": 60,
    },
]
