import re
from typing import Any, List, Optional
from unittest import mock
from urllib.parse import parse_qs, urlsplit

import pytest
from requests import Request, Response, Session

import dlt
from dlt.common import pendulum
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.sources.helpers.rest_client.paginators import BaseReferencePaginator
from dlt.sources.rest_api import (
    ClientConfig,
    Endpoint,
    EndpointResource,
    RESTAPIConfig,
    rest_api_source,
)
from tests.sources.rest_api.conftest import DEFAULT_PAGE_SIZE, DEFAULT_TOTAL_PAGES
from tests.pipeline.utils import assert_load_info, load_table_counts, assert_query_column


def _parse_single_valued_qs(body) -> dict[str, str]:
    """Parse query string expecting single values per key."""
    return {k: v[0] for k, v in parse_qs(body).items()}


def _filter_by_path_pattern(request_history: list[Any], path_pattern: str) -> list[Any]:
    pattern = re.compile(path_pattern)
    return [request for request in request_history if pattern.match(request.path)]


@pytest.mark.parametrize(
    "config",
    [
        # Using resolve params in path
        pytest.param(
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
                                }
                            },
                        },
                    },
                    {
                        "name": "post_details",
                        "endpoint": {
                            "path": "posts/{post_id}",
                            "params": {
                                "post_id": {
                                    "type": "resolve",
                                    "resource": "posts",
                                    "field": "id",
                                }
                            },
                        },
                    },
                ],
            },
            id="resolve_params_in_path",
        ),
        # Using interpolated params in path
        pytest.param(
            {
                "client": {"base_url": "https://api.example.com"},
                "resources": [
                    "posts",
                    {
                        "name": "post_comments",
                        "endpoint": "posts/{resources.posts.id}/comments",
                    },
                    {
                        "name": "post_details",
                        "endpoint": "posts/{resources.posts.id}",
                    },
                ],
            },
            id="interpolated_params_in_path",
        ),
        # Using interpolated params in query string
        pytest.param(
            {
                "client": {"base_url": "https://api.example.com"},
                "resources": [
                    "posts",
                    {
                        "name": "post_comments",
                        "endpoint": {
                            "path": "post_comments",
                            "params": {
                                "post_id": "{resources.posts.id}",
                            },
                        },
                    },
                    {
                        "name": "post_details",
                        "endpoint": {
                            "path": "post_detail",
                            "params": {
                                "post_id": "{resources.posts.id}",
                            },
                        },
                    },
                ],
            },
            id="interpolated_params_in_query_string",
        ),
        # Using interpolated params in json object
        pytest.param(
            {
                "client": {"base_url": "https://api.example.com"},
                "resources": [
                    "posts",
                    {
                        "name": "post_comments",
                        "endpoint": {
                            "path": "post_comments",
                            "method": "POST",
                            "json": {
                                "post_id": "{resources.posts.id}",
                            },
                        },
                    },
                    {
                        "name": "post_details",
                        "endpoint": {
                            "path": "post_detail",
                            "method": "POST",
                            "json": {
                                "post_id": "{resources.posts.id}",
                            },
                        },
                    },
                ],
            },
            id="interpolated_params_in_json_string",
        ),
        # Using interpolated params in the headers
        pytest.param(
            {
                "client": {"base_url": "https://api.example.com"},
                "resources": [
                    "posts",
                    {
                        "name": "post_comments",
                        "endpoint": {
                            "path": "post_comments_via_headers",
                            "headers": {
                                "post_id": "{resources.posts.id}",
                            },
                        },
                    },
                    {
                        "name": "post_details",
                        "endpoint": {
                            "path": "post_detail_via_headers",
                            "headers": {
                                "post_id": "{resources.posts.id}",
                            },
                        },
                    },
                ],
            },
            id="interpolated_params_in_the_headers",
        ),
    ],
)
def test_load_mock_api(mock_api_server, config):
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_mock",
        destination="duckdb",
        dataset_name="rest_api_mock",
        dev_mode=True,
    )

    mock_source = rest_api_source(config)

    load_info = pipeline.run(mock_source)
    print(load_info)
    assert_load_info(load_info)
    table_counts = load_table_counts(pipeline)

    assert table_counts.keys() == {"posts", "post_comments", "post_details"}

    assert table_counts["posts"] == DEFAULT_PAGE_SIZE * DEFAULT_TOTAL_PAGES
    assert table_counts["post_details"] == DEFAULT_PAGE_SIZE * DEFAULT_TOTAL_PAGES
    assert table_counts["post_comments"] == 50 * DEFAULT_PAGE_SIZE * DEFAULT_TOTAL_PAGES

    with pipeline.sql_client() as client:
        posts_table = client.make_qualified_table_name("posts")
        posts_details_table = client.make_qualified_table_name("post_details")
        post_comments_table = client.make_qualified_table_name("post_comments")

    print(pipeline.default_schema.to_pretty_yaml())

    assert_query_column(
        pipeline,
        f"SELECT title FROM {posts_table} ORDER BY id limit 25",
        [f"Post {i}" for i in range(25)],
    )

    assert_query_column(
        pipeline,
        f"SELECT body FROM {posts_details_table} ORDER BY id limit 25",
        [f"Post body {i}" for i in range(25)],
    )

    assert_query_column(
        pipeline,
        f"SELECT body FROM {post_comments_table} ORDER BY post_id, id limit 5",
        [f"Comment {i} for post 0" for i in range(5)],
    )


@pytest.mark.parametrize(
    "endpoint_params,expected_static_params",
    [
        # Static params only
        pytest.param(
            {
                "path": "posts",
                "params": {"sort": "desc", "locale": "en"},
            },
            {"sort": ["desc"], "locale": ["en"]},
            id="static_params",
        ),
        # No static params
        pytest.param(
            {
                "path": "posts",
                "params": {},
            },
            {},
            id="empty_params",
        ),
        # One of the params is empty
        pytest.param(
            {
                "path": "posts",
                "params": {"sort": "desc", "locale": ""},
            },
            {"sort": ["desc"], "locale": [""]},
            id="one_empty_param",
        ),
        # Explicitly set page param gets ignored
        pytest.param(
            {
                "path": "posts",
                "params": {"sort": "desc", "locale": "en", "page": "100"},
            },
            {"sort": ["desc"], "locale": ["en"]},
            id="explicit_page_param",
        ),
        # Incremental defined in endpoint
        pytest.param(
            {
                "path": "posts",
                "incremental": {
                    "start_param": "since",
                    "end_param": "until",
                    "cursor_path": "id",
                    "initial_value": 1,
                    "end_value": 10,
                },
            },
            {"since": ["1"], "until": ["10"]},
            id="incremental_in_endpoint",
        ),
        # Incremental mixed with static params
        pytest.param(
            {
                "path": "posts",
                "params": {"sort": "desc", "locale": "en"},
                "incremental": {
                    "start_param": "since",
                    "end_param": "until",
                    "cursor_path": "id",
                    "initial_value": 1,
                    "end_value": 10,
                },
            },
            {"sort": ["desc"], "locale": ["en"], "since": ["1"], "until": ["10"]},
            id="incremental_in_endpoint_mixed_with_static",
        ),
        # Incremental defined in params
        pytest.param(
            {
                "path": "posts",
                "params": {
                    "sort": "desc",
                    "since": {
                        "type": "incremental",
                        "cursor_path": "id",
                        "initial_value": 1,
                    },
                },
            },
            {"since": ["1"], "sort": ["desc"]},
            id="incremental_in_params",
        ),
        pytest.param(
            {
                "path": "posts",
                "params": {
                    "sort": "desc",
                    "param_with_braces": "{{not_this}}",
                    "param_with_nested_braces": "{{not_this {{and}} {{not_that}}}}",
                },
            },
            {
                "sort": ["desc"],
                "param_with_braces": ["{not_this}"],
                "param_with_nested_braces": ["{not_this {and} {not_that}}"],
            },
            id="escaped_braces_in_params",
        ),
    ],
)
def test_single_resource_query_string_params(
    mock_api_server, endpoint_params, expected_static_params
):
    mock_source = rest_api_source(
        {
            "client": {
                "base_url": "https://api.example.com",
                "paginator": {
                    "type": "page_number",
                    "base_page": 1,
                    "total_path": "total_pages",
                },
            },
            "resources": [
                {
                    "name": "posts",
                    "endpoint": {
                        **endpoint_params,
                    },
                }
            ],
        }
    )

    list(mock_source.with_resources("posts"))

    history = mock_api_server.request_history
    assert len(history) == 5

    for i, request_call in enumerate(history, start=1):
        qs = parse_qs(urlsplit(request_call.url).query, keep_blank_values=True)
        expected = {**expected_static_params, "page": [str(i)]}
        assert qs == expected


@pytest.mark.parametrize(
    "endpoint_params,expected_static_params",
    [
        # Static params only
        pytest.param(
            {
                "path": "posts/{post_id}/comments",
                "params": {
                    "post_id": {"type": "resolve", "resource": "posts", "field": "id"},
                    "sort": "desc",
                    "locale": "en",
                },
            },
            {"sort": ["desc"], "locale": ["en"]},
            id="static_params",
        ),
        # No static params
        pytest.param(
            {
                "path": "posts/{post_id}/comments",
                "params": {
                    "post_id": {"type": "resolve", "resource": "posts", "field": "id"},
                },
            },
            {},
            id="empty_params",
        ),
        # One of the params is empty
        pytest.param(
            {
                "path": "posts/{post_id}/comments",
                "params": {
                    "post_id": {"type": "resolve", "resource": "posts", "field": "id"},
                    "sort": "desc",
                    "locale": "",
                },
            },
            {"sort": ["desc"], "locale": [""]},
            id="one_empty_param",
        ),
        # Explicitly set page param gets ignored
        pytest.param(
            {
                "path": "posts/{post_id}/comments",
                "params": {
                    "post_id": {"type": "resolve", "resource": "posts", "field": "id"},
                    "sort": "desc",
                    "locale": "en",
                    "page": "100",
                },
            },
            {"sort": ["desc"], "locale": ["en"]},
            id="explicit_page_param",
        ),
        # Incremental defined in endpoint
        pytest.param(
            {
                "path": "posts/{post_id}/comments",
                "params": {"post_id": {"type": "resolve", "resource": "posts", "field": "id"}},
                "incremental": {
                    "start_param": "since",
                    "end_param": "until",
                    "cursor_path": "id",
                    "initial_value": 1,
                    "end_value": 10,
                },
            },
            {"since": ["1"], "until": ["10"]},
            id="incremental_in_endpoint",
        ),
        # Incremental mixed with static params
        pytest.param(
            {
                "path": "posts/{post_id}/comments",
                "params": {
                    "post_id": {"type": "resolve", "resource": "posts", "field": "id"},
                    "sort": "desc",
                    "locale": "en",
                },
                "incremental": {
                    "start_param": "since",
                    "end_param": "until",
                    "cursor_path": "id",
                    "initial_value": 1,
                    "end_value": 10,
                },
            },
            {"sort": ["desc"], "locale": ["en"], "since": ["1"], "until": ["10"]},
            id="incremental_in_endpoint_mixed_with_static",
        ),
        # Incremental defined in params
        pytest.param(
            {
                "path": "posts/{post_id}/comments",
                "params": {
                    "post_id": {"type": "resolve", "resource": "posts", "field": "id"},
                    "since": {
                        "type": "incremental",
                        "cursor_path": "id",
                        "initial_value": 1,
                    },
                },
            },
            {"since": ["1"]},
            id="incremental_in_params",
        ),
        # Incremental defined in params with static params
        pytest.param(
            {
                "path": "posts/{post_id}/comments",
                "params": {
                    "post_id": {"type": "resolve", "resource": "posts", "field": "id"},
                    "since": {
                        "type": "incremental",
                        "cursor_path": "id",
                        "initial_value": 1,
                    },
                    "sort": "desc",
                    "locale": "en",
                },
            },
            {"since": ["1"], "sort": ["desc"], "locale": ["en"]},
            id="incremental_in_params_with_static",
        ),
    ],
)
def test_dependent_resource_query_string_params(
    mock_api_server, endpoint_params, expected_static_params
):
    mock_source = rest_api_source(
        {
            "client": {
                "base_url": "https://api.example.com",
                "paginator": {
                    "type": "page_number",
                    "base_page": 1,
                    "total_path": "total_pages",
                },
            },
            "resources": [
                "posts",
                {
                    "name": "post_comments",
                    "endpoint": {
                        **endpoint_params,
                    },
                },
            ],
        }
    )

    list(mock_source.with_resources("posts", "post_comments").add_limit(1))

    post_comments_calls = _filter_by_path_pattern(
        mock_api_server.request_history, r"/posts/\d+/comments"
    )
    assert len(post_comments_calls) == 50

    for call in post_comments_calls:
        qs = parse_qs(urlsplit(call.url).query, keep_blank_values=True)
        expected_keys = set(expected_static_params.keys()) | {"page"}
        assert set(qs.keys()) == expected_keys

        for param_key, param_values in expected_static_params.items():
            assert qs[param_key] == param_values

        assert 1 <= int(qs["page"][0]) <= 10


@pytest.mark.parametrize(
    "endpoint_params,expected_static_params",
    [
        # No static params
        pytest.param(
            {
                "path": "post_detail",
                "params": {"post_id": "{resources.posts.id}"},
            },
            {},
            id="interpolate_param_only",
        ),
        # With static params
        pytest.param(
            {
                "path": "post_detail",
                "params": {"post_id": "{resources.posts.id}", "sort": "desc"},
            },
            {"sort": ["desc"]},
            id="interpolate_param_with_static",
        ),
        # One static param is empty
        pytest.param(
            {
                "path": "post_detail",
                "params": {
                    "post_id": "{resources.posts.id}",
                    "sort": "desc",
                    "locale": "",
                },
            },
            {"sort": ["desc"], "locale": [""]},
            id="one_static_param_is_empty",
        ),
        # Escaped braces in params
        pytest.param(
            {
                "path": "post_detail",
                "params": {
                    "post_id": "{resources.posts.id}",
                    "sort": "desc",
                    "param_with_braces": "{{not_this}}",
                },
            },
            {"sort": ["desc"], "param_with_braces": ["{not_this}"]},
            id="escaped_braces_in_params",
        ),
    ],
)
def test_interpolate_params_in_query_string(
    mock_api_server, endpoint_params, expected_static_params
):
    mock_source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                "posts",
                {
                    "name": "post_details",
                    "endpoint": {
                        **endpoint_params,
                    },
                },
            ],
        }
    )
    list(mock_source.with_resources("posts", "post_details").add_limit(1))

    post_details_calls = _filter_by_path_pattern(mock_api_server.request_history, "/post_detail")
    assert len(post_details_calls) == 5

    for index, call in enumerate(post_details_calls):
        qs = parse_qs(urlsplit(call.url).query, keep_blank_values=True)
        assert set(qs.keys()) == set(expected_static_params.keys()) | {"post_id"}
        assert qs["post_id"] == [str(index)]

        for param_key, param_values in expected_static_params.items():
            assert qs[param_key] == param_values


@pytest.mark.parametrize(
    "endpoint_config,expected_body",
    [
        pytest.param(
            {
                "path": "posts/{resources.posts.id}/comments",
                # No json specified
            },
            None,
            id="no_json_get",
        ),
        pytest.param(
            {
                "path": "posts/{resources.posts.id}/comments",
                "method": "GET",
                "json": {},
            },
            b"{}",
            id="empty_json_get",
        ),
    ],
)
def test_request_json_body(mock_api_server, endpoint_config, expected_body) -> None:
    source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                "posts",
                {
                    "name": "posts_comments",
                    "endpoint": endpoint_config,
                },
            ],
        }
    )
    list(source.with_resources("posts", "posts_comments").add_limit(1))

    post_comments_calls = _filter_by_path_pattern(
        mock_api_server.request_history, r"/posts/\d+/comments"
    )
    assert len(post_comments_calls) == 50

    request = post_comments_calls[0]
    if expected_body is None:
        assert request.body is None
    else:
        assert request.body == expected_body


def test_raises_error_for_unused_resolve_params(mock_api_server):
    with pytest.raises(ValueError) as exc_info:
        rest_api_source(
            {
                "client": {"base_url": "https://api.example.com"},
                "resources": [
                    "posts",
                    {
                        "name": "post_details",
                        "endpoint": {
                            "path": "posts",
                            "params": {
                                "post_id": {
                                    "type": "resolve",
                                    "resource": "posts",
                                    "field": "id",
                                }
                            },
                        },
                    },
                ],
            }
        )

    assert (
        "Resource `post_details` defines resolve params `['post_id']` that are not bound in path"
        " `posts`. To reference parent resource in query params use syntax"
        " 'resources.<parent_resource>.<field>'"
        in str(exc_info.value)
    )


@pytest.mark.parametrize(
    "config,location",
    [
        pytest.param(
            {
                "client": {"base_url": "https://api.example.com"},
                "resources": [
                    "posts",
                    {
                        "name": "post_detail",
                        "endpoint": {
                            "path": "post_detail",
                            "params": {"post_id": "{unknown.posts.id}"},
                        },
                    },
                ],
            },
            "params",
            id="query_params",
        ),
        pytest.param(
            {
                "client": {"base_url": "https://api.example.com"},
                "resources": [
                    "posts",
                    {
                        "name": "search_by_id",
                        "endpoint": {
                            "path": "posts/search_by_id/{resources.posts.id}",
                            "method": "POST",
                            "json": {"post_id": "{unknown.posts.id}"},
                        },
                    },
                ],
            },
            "json",
            id="json_body",
        ),
    ],
)
def test_raises_error_for_incorrect_interpolation(mock_api_server, config, location):
    with pytest.raises(ValueError) as exc_info:
        rest_api_source(config)

    assert exc_info.match(
        f"Expression `unknown.posts.id` defined in `{location}` is not valid. Valid expressions"
        " must start with one of: `{'resources'}`"
    )


def test_source_with_post_request(mock_api_server):
    class JSONBodyPageCursorPaginator(BaseReferencePaginator):
        def update_state(self, response: Response, data: Optional[List[Any]] = None) -> None:
            self._next_reference = response.json().get("next_page")

        def update_request(self, request: Request) -> None:
            if request.json is None:
                request.json = {}

            request.json["page"] = self._next_reference

    mock_source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "search_posts",
                    "endpoint": {
                        "path": "/posts/search",
                        "method": "POST",
                        "json": {
                            "ids_greater_than": 50,
                            "page_size": 25,
                            "page_count": 4,
                        },
                        "paginator": JSONBodyPageCursorPaginator(),
                    },
                }
            ],
        }
    )

    res = list(mock_source.with_resources("search_posts"))

    for i in range(49):
        assert res[i] == {"id": 51 + i, "title": f"Post {51 + i}"}


@pytest.mark.parametrize(
    "json_body,expected_json",
    [
        pytest.param(
            {
                "initial_value": "{incremental.initial_value}",
                "escaped_braces": "{{not_this}}",
                "nested": {
                    "initial_value": "{incremental.initial_value}",
                    "escaped": "{{not_this_either}}",
                },
                "array_values": [
                    "{incremental.initial_value}",
                    "{{not_array_either}}",
                ],
            },
            {
                "initial_value": 1,
                "escaped_braces": "{not_this}",
                "nested": {
                    "initial_value": 1,
                    "escaped": "{not_this_either}",
                },
                "array_values": [
                    1,
                    "{not_array_either}",
                ],
            },
            id="complex_nested_json",
        ),
        pytest.param(
            {},
            {},
            id="empty_json",
        ),
        # TODO: None JSON breaks _merge_resource_endpoints
        # pytest.param(
        #     None,
        #     None,
        #     id="none_json",
        # ),
    ],
)
def test_json_body_in_top_level_resource(mock_api_server, json_body, expected_json):
    source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "posts",
                    "endpoint": {
                        "path": "posts/search",
                        "method": "POST",
                        "json": json_body,
                        "incremental": {
                            "cursor_path": "id",
                            "initial_value": 1,
                        },
                    },
                },
            ],
        }
    )

    list(source.with_resources("posts").add_limit(1))

    history = mock_api_server.request_history
    assert len(history) == 1
    request_call = history[0]
    if expected_json is None:
        assert request_call.json is None
    else:
        assert request_call.json() == expected_json


def test_interpolate_parent_values_in_path_and_json_body(mock_api_server):
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_mock",
        destination="duckdb",
        dataset_name="rest_api_mock",
        dev_mode=True,
    )
    mock_source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                "posts",
                {
                    "name": "post_details",
                    "endpoint": {
                        "path": "posts/search_by_id/{resources.posts.id}",
                        "method": "POST",
                        "json": {
                            "post_id": "{resources.posts.id}",
                            "limit": 5,
                            "more": {
                                "title": "{resources.posts.title}",
                            },
                            "more_array": [
                                "{resources.posts.id}",
                            ],
                            "escaped_braces": "{{ string in escaped braces }}",
                        },
                    },
                },
            ],
        }
    )
    load_info = pipeline.run(mock_source)
    print(load_info)
    assert_load_info(load_info)
    table_counts = load_table_counts(pipeline)
    assert table_counts.keys() == {"posts", "post_details"}
    assert table_counts["posts"] == DEFAULT_PAGE_SIZE * DEFAULT_TOTAL_PAGES
    assert table_counts["post_details"] == DEFAULT_PAGE_SIZE * DEFAULT_TOTAL_PAGES
    with pipeline.sql_client() as client:
        posts_table = client.make_qualified_table_name("posts")
        posts_details_table = client.make_qualified_table_name("post_details")
    print(pipeline.default_schema.to_pretty_yaml())
    assert_query_column(
        pipeline,
        f"SELECT title FROM {posts_table} ORDER BY id limit 25",
        [f"Post {i}" for i in range(25)],
    )
    assert_query_column(
        pipeline,
        f"SELECT body FROM {posts_details_table} ORDER BY id limit 25",
        [f"Post body {i}" for i in range(25)],
    )
    assert_query_column(
        pipeline,
        f"SELECT title FROM {posts_details_table} ORDER BY id limit 25",
        [f"Post {i}" for i in range(25)],
    )
    assert_query_column(
        pipeline,
        f"SELECT more FROM {posts_details_table} ORDER BY id limit 25",
        [f"More is equale to id: {i}" for i in range(25)],
    )


def test_unauthorized_access_to_protected_endpoint(mock_api_server):
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_mock",
        destination="duckdb",
        dataset_name="rest_api_mock",
        dev_mode=True,
    )

    mock_source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                "/protected/posts/bearer-token-plain-text-error",
            ],
        }
    )

    with pytest.raises(PipelineStepFailed) as e:
        pipeline.run(mock_source)
    assert e.match("401 Client Error")


def test_posts_under_results_key(mock_api_server):
    mock_source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "posts",
                    "endpoint": {
                        "path": "posts_under_a_different_key",
                        "data_selector": "many-results",
                        "paginator": "json_link",
                    },
                },
            ],
        }
    )

    res = list(mock_source.with_resources("posts").add_limit(1))

    assert res[:5] == [
        {"id": 0, "title": "Post 0"},
        {"id": 1, "title": "Post 1"},
        {"id": 2, "title": "Post 2"},
        {"id": 3, "title": "Post 3"},
        {"id": 4, "title": "Post 4"},
    ]


def test_posts_without_key(mock_api_server):
    mock_source = rest_api_source(
        {
            "client": {
                "base_url": "https://api.example.com",
                "paginator": "header_link",
            },
            "resources": [
                {
                    "name": "posts_no_key",
                    "endpoint": {
                        "path": "posts_no_key",
                    },
                },
            ],
        }
    )

    res = list(mock_source.with_resources("posts_no_key").add_limit(1))

    assert res[:5] == [
        {"id": 0, "title": "Post 0"},
        {"id": 1, "title": "Post 1"},
        {"id": 2, "title": "Post 2"},
        {"id": 3, "title": "Post 3"},
        {"id": 4, "title": "Post 4"},
    ]


@pytest.mark.parametrize(
    "config",
    [
        RESTAPIConfig(
            client=ClientConfig(base_url="https://api.example.com"),
            resources=[
                "posts",
                EndpointResource(
                    name="post_comments",
                    endpoint=Endpoint(
                        path="posts/{post_id}/comments",
                        params={
                            "post_id": {
                                "type": "resolve",
                                "resource": "posts",
                                "field": "id",
                            }
                        },
                    ),
                ),
            ],
        ),
        RESTAPIConfig(
            client=ClientConfig(base_url="https://api.example.com"),
            resources=[
                "posts",
                EndpointResource(
                    name="post_comments",
                    endpoint=Endpoint(
                        path="posts/{resources.posts.id}/comments",
                    ),
                ),
            ],
        ),
    ],
)
def test_load_mock_api_typeddict_config(mock_api_server, config):
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_mock",
        destination="duckdb",
        dataset_name="rest_api_mock",
        dev_mode=True,
    )

    mock_source = rest_api_source(config)

    load_info = pipeline.run(mock_source)
    assert_load_info(load_info)
    table_counts = load_table_counts(pipeline)

    assert table_counts.keys() == {"posts", "post_comments"}

    assert table_counts["posts"] == DEFAULT_PAGE_SIZE * DEFAULT_TOTAL_PAGES
    assert table_counts["post_comments"] == DEFAULT_PAGE_SIZE * DEFAULT_TOTAL_PAGES * 50


def test_posts_with_incremental_date_conversion(mock_api_server) -> None:
    start_time = pendulum.from_timestamp(1)
    one_day_later = start_time.add(days=1)
    config: RESTAPIConfig = {
        "client": {"base_url": "https://api.example.com"},
        "resources": [
            {
                "name": "posts",
                "endpoint": {
                    "path": "posts",
                    "incremental": {
                        "start_param": "since",
                        "end_param": "until",
                        "cursor_path": "updated_at",
                        "initial_value": str(start_time.int_timestamp),
                        "end_value": str(one_day_later.int_timestamp),
                        "convert": lambda epoch: pendulum.from_timestamp(
                            int(epoch)
                        ).to_date_string(),
                    },
                },
            },
        ],
    }
    RESTClient = dlt.sources.helpers.rest_client.RESTClient
    with mock.patch.object(RESTClient, "paginate") as mock_paginate:
        source = rest_api_source(config).add_limit(1)
        _ = list(source.with_resources("posts"))
        assert mock_paginate.call_count == 1
        _, called_kwargs = mock_paginate.call_args_list[0]
        assert called_kwargs["params"] == {"since": "1970-01-01", "until": "1970-01-02"}
        assert called_kwargs["path"] == "posts"


def test_incremental_values_in_param_template_with_conversion(mock_api_server) -> None:
    start_time = pendulum.from_timestamp(1)
    one_day_later = start_time.add(days=1)
    config: RESTAPIConfig = {
        "client": {"base_url": "https://api.example.com"},
        "resources": [
            {
                "name": "posts",
                "endpoint": {
                    "path": "posts",
                    "params": {
                        "since": "{incremental.start_value}",
                        "until": "{incremental.end_value}",
                    },
                    "incremental": {
                        "cursor_path": "updated_at",
                        "initial_value": str(start_time.int_timestamp),
                        "end_value": str(one_day_later.int_timestamp),
                        "convert": lambda epoch: pendulum.from_timestamp(
                            int(epoch)
                        ).to_date_string(),
                    },
                },
            },
        ],
    }
    RESTClient = dlt.sources.helpers.rest_client.RESTClient
    with mock.patch.object(RESTClient, "paginate") as mock_paginate:
        source = rest_api_source(config).add_limit(1)
        _ = list(source.with_resources("posts"))
        assert mock_paginate.call_count == 1
        _, called_kwargs = mock_paginate.call_args_list[0]
        assert called_kwargs["params"] == {"since": "1970-01-01", "until": "1970-01-02"}
        assert called_kwargs["path"] == "posts"


def test_incremental_object_interpolation(mock_api_server) -> None:
    initial_value = 1600000000
    end_value = 1700000000
    lag = 3600

    source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "posts",
                    "endpoint": {
                        "path": "posts",
                        "params": {
                            "initial_value": "{incremental.initial_value}",
                            "lag": "{incremental.lag}",
                            "since": "{incremental.start_value}",
                            "until": "{incremental.end_value}",
                        },
                        "incremental": {
                            "cursor_path": "id",
                            "initial_value": initial_value,
                            "end_value": end_value,
                            "lag": lag,
                        },
                    },
                },
            ],
        }
    )
    list(source.with_resources("posts").add_limit(1))

    history = mock_api_server.request_history
    # there will be 5 requests because pages are empty and incremental will remove those
    assert len(history) == 5
    request_call = history[0]
    qs = parse_qs(urlsplit(request_call.url).query, keep_blank_values=True)
    assert qs == {
        "initial_value": [str(initial_value)],
        "lag": [str(lag)],
        "since": [str(initial_value)],
        "until": [str(end_value)],
    }


@pytest.mark.parametrize(
    "config",
    [
        pytest.param(
            {
                "client": {"base_url": "https://api.example.com"},
                "resources": [
                    {
                        "name": "posts",
                        "endpoint": {
                            "path": "posts",
                            "params": {"since": "{incremental.start_value}"},
                            "incremental": {
                                "cursor_path": "id",
                                "initial_value": 1600000000,
                                "convert": lambda epoch: int(epoch),
                            },
                        },
                    },
                ],
            },
            id="incremental_interpolated_in_params",
        ),
        pytest.param(
            {
                "client": {"base_url": "https://api.example.com"},
                "resources": [
                    {
                        "name": "posts",
                        "endpoint": {
                            "path": "posts",
                            "incremental": {
                                "start_param": "since",
                                "cursor_path": "id",
                                "initial_value": 1600000000,
                                "convert": lambda epoch: int(epoch),
                            },
                        },
                    },
                ],
            },
            id="incremental_in_endpoint",
        ),
        pytest.param(
            {
                "client": {"base_url": "https://api.example.com"},
                "resources": [
                    {
                        "name": "posts",
                        "endpoint": {
                            "path": "posts",
                            "params": {
                                "since": {
                                    "type": "incremental",
                                    "cursor_path": "id",
                                    "initial_value": 1600000000,
                                    "convert": lambda epoch: int(epoch),
                                }
                            },
                        },
                    },
                ],
            },
            id="incremental_in_params",
        ),
    ],
)
def test_incremental_convert_without_end_value(mock_api_server, config) -> None:
    source = rest_api_source(config)

    list(source.with_resources("posts").add_limit(1))

    history = mock_api_server.request_history
    assert len(history) == 5
    request_call = history[0]
    qs = parse_qs(urlsplit(request_call.url).query, keep_blank_values=True)
    assert qs == {"since": ["1600000000"]}


def test_incremental_implicitly_filters_out_data(mock_api_server) -> None:
    source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "posts",
                    "endpoint": {
                        "path": "posts",
                        "incremental": {
                            "cursor_path": "id",
                            "initial_value": 1,
                        },
                    },
                }
            ],
        }
    )

    pipeline = dlt.pipeline(
        pipeline_name="test_incremental_filters_implicitly", destination="duckdb"
    )
    load_info = pipeline.run(source)
    assert len(load_info.loads_ids) == 1

    # On the second run, incremental should filter out everything implicitly
    load_info = pipeline.run(source)
    assert len(load_info.loads_ids) == 0


def test_custom_session_is_used(mock_api_server, mocker):
    class CustomSession(Session):
        pass

    my_session = CustomSession()
    mocked_send = mocker.spy(my_session, "send")

    source = rest_api_source(
        {
            "client": {
                "base_url": "https://api.example.com",
                "session": my_session,
            },
            "resources": [
                {
                    "name": "posts",
                },
            ],
        }
    )

    list(source.with_resources("posts").add_limit(1))

    mocked_send.assert_called_once()
    assert mocked_send.call_args[0][0].url == "https://api.example.com/posts"


@pytest.mark.parametrize(
    "posts_resource_config",
    [
        {
            "name": "posts",
            "endpoint": {
                "path": "posts/{post_id}/comments",
                "params": {
                    "post_id": {
                        "type": "resolve",
                        "resource": "post_list",
                        "field": "id",
                    },
                },
            },
        },
        {
            "name": "posts",
            "endpoint": "posts/{resources.post_list.id}/comments",
        },
    ],
)
def test_DltResource_gets_called(mock_api_server, mocker, posts_resource_config) -> None:
    @dlt.resource()
    def post_list():
        yield [{"id": "0"}, {"id": "1"}, {"id": "2"}]

    config: RESTAPIConfig = {
        "client": {"base_url": "http://api.example.com/"},
        "resource_defaults": {
            "write_disposition": "replace",
        },
        "resources": [
            posts_resource_config,
            post_list(),
        ],
    }

    RESTClient = dlt.sources.helpers.rest_client.RESTClient
    with mock.patch.object(RESTClient, "paginate") as mock_paginate:
        source = rest_api_source(config)
        _ = list(source)
        assert mock_paginate.call_count == 3
        for i in range(3):
            _, kwargs = mock_paginate.call_args_list[i]
            assert kwargs["path"] == f"posts/{i}/comments"


@pytest.mark.parametrize(
    "headers, expected_headers",
    [
        pytest.param(
            {
                "X-Custom-Header": "test-value",
                "X-Another-Header": "another-value",
            },
            {
                "X-Custom-Header": "test-value",
                "X-Another-Header": "another-value",
            },
            id="basic",
        ),
        pytest.param(
            {
                "X-Custom-Header": "test-value with {{escaped}} braces",
                "X-Another-Header": "",
            },
            {
                "X-Custom-Header": "test-value with {escaped} braces",
                "X-Another-Header": "",
            },
            id="empty-header-and-escaped-braces",
        ),
        pytest.param(
            {
                "X-Double-Braces-{{escaped}}": "test-value",
            },
            {
                "X-Double-Braces-{escaped}": "test-value",
            },
            id="braces-in-header-name",
        ),
    ],
)
def test_headers_in_top_level_resource(mock_api_server, headers, expected_headers):
    """Test that headers are properly passed in top-level resources"""
    source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "posts",
                    "endpoint": {
                        "path": "posts",
                        "headers": headers,
                    },
                },
            ],
        }
    )

    list(source.with_resources("posts").add_limit(1))

    history = mock_api_server.request_history
    assert len(history) == 1
    request_call = history[0]
    assert request_call.headers["User-Agent"].startswith("dlt/")
    for header_name, header_value in expected_headers.items():
        assert request_call.headers[header_name] == header_value


def test_headers_in_dependent_resource(mock_api_server):
    """Test that headers can use interpolation from parent resource"""
    source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                "posts",
                {
                    "name": "post_details",
                    "endpoint": {
                        "path": "post_detail",
                        "params": {
                            "post_id": "{resources.posts.id}",
                        },
                        "headers": {
                            "X-Static-Header": "test-value",
                            "X-Double-Braces-{{Escaped}}": "name-escaped",
                            "X-Post-ID": "{resources.posts.id}",
                            "X-Post-Title": "{resources.posts.title}",
                            "X-Escaped": "{{value_escaped}}",
                        },
                    },
                },
            ],
        }
    )

    list(source.with_resources("posts", "post_details").add_limit(1))
    post_details_calls = _filter_by_path_pattern(mock_api_server.request_history, "/post_detail")
    assert len(post_details_calls) == 5

    for index, call in enumerate(post_details_calls):
        assert call.headers["User-Agent"].startswith("dlt/")
        assert call.headers["X-Post-ID"] == str(index)
        assert call.headers["X-Post-Title"] == f"Post {index}"
        assert call.headers["X-Escaped"] == "{value_escaped}"


def test_headers_with_incremental_values(mock_api_server):
    """Test that headers can use incremental values"""
    source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "posts",
                    "endpoint": {
                        "path": "posts",
                        "headers": {
                            "X-Initial-Value": "{incremental.initial_value}",
                            "X-Start-Value": "{incremental.start_value}",
                            "X-End-Value": "{incremental.end_value}",
                            "X-Escaped": "{{not_this}}",
                        },
                        "incremental": {
                            "cursor_path": "id",
                            "initial_value": 1600000000,
                            "end_value": 1700000000,
                        },
                    },
                },
            ],
        }
    )

    list(source.with_resources("posts").add_limit(1))

    history = mock_api_server.request_history
    assert len(history) == 5
    request_call = history[0]
    assert request_call.headers["X-Initial-Value"] == "1600000000"
    assert request_call.headers["X-Start-Value"] == "1600000000"
    assert request_call.headers["X-End-Value"] == "1700000000"
    assert request_call.headers["X-Escaped"] == "{not_this}"


def test_client_headers_are_not_interpolated(mock_api_server):
    source = rest_api_source(
        {
            "client": {
                "base_url": "https://api.example.com",
                "headers": {
                    "X-Static-Header": "static-value",
                    "X-Not-Interpolated": (
                        "{resources.posts.id}"
                    ),  # Should remain as a literal string
                    "X-Escaped": "{{literal_braces}}",  # Should remain as a literal string
                },
            },
            "resources": [
                "posts",
            ],
        }
    )

    list(source.with_resources("posts").add_limit(1))

    history = mock_api_server.request_history
    assert len(history) == 1
    request_call = history[0]

    assert request_call.headers["X-Static-Header"] == "static-value"
    assert request_call.headers["X-Not-Interpolated"] == "{resources.posts.id}"
    assert request_call.headers["X-Escaped"] == "{{literal_braces}}"


def test_secret_redaction_in_logs_and_errors(mock_api_server, mocker):
    from dlt.sources.helpers.rest_client.client import RESTClient

    mock_api_server.get("https://api.example.com/data", json=[{"id": 1, "data": "test"}])

    # Mock logger
    log_messages = []
    original_log = RESTClient._send_request.__globals__["logger"].info

    def capture_log(msg, *args):
        log_messages.append(msg % args if args else msg)
        original_log(msg, *args)

    mocker.patch.object(
        RESTClient._send_request.__globals__["logger"], "info", side_effect=capture_log
    )

    # API keys in URL should be redacted in logs
    source = rest_api_source(
        {
            "client": {
                "base_url": "https://api.example.com",
            },
            "resources": [
                {
                    "name": "protected_resource",
                    "endpoint": {
                        "path": "data",
                        "params": {
                            "api_key": "super_secret_key_12345",
                            "token": "another_secret_token",
                            "public_param": "visible_value",
                        },
                    },
                }
            ],
        }
    )

    list(source.with_resources("protected_resource").add_limit(1))

    assert any("api_key=***" in msg for msg in log_messages)
    assert any("token=***" in msg for msg in log_messages)
    assert any("public_param=visible_value" in msg for msg in log_messages)

    assert not any("super_secret_key_12345" in msg for msg in log_messages)
    assert not any("another_secret_token" in msg for msg in log_messages)


def test_secret_redaction_in_http_errors(mock_api_server):
    mock_api_server.get(
        "https://api.example.com/missing", status_code=404, text="Resource not found"
    )

    source = rest_api_source(
        {
            "client": {
                "base_url": "https://api.example.com",
            },
            "resources": [
                {
                    "name": "error_resource",
                    "endpoint": {
                        "path": "missing",
                        "params": {
                            "api_key": "secret_api_key_xyz",
                            "access_token": "secret_token_abc",
                            "normal_param": "public_value",
                        },
                    },
                }
            ],
        }
    )

    with pytest.raises(PipelineStepFailed) as exc_info:
        pipeline = dlt.pipeline(
            pipeline_name="test_pipeline",
            destination="duckdb",
        )
        pipeline.run(source.with_resources("error_resource").add_limit(1))

    error_str = str(exc_info.value)
    assert "api_key=***" in error_str
    assert "access_token=***" in error_str
    assert "normal_param=public_value" in error_str

    # Check that actual secrets are not exposed
    assert "secret_api_key_xyz" not in error_str
    assert "secret_token_abc" not in error_str

    assert "404" in error_str
    assert "Resource not found" not in error_str


@pytest.mark.parametrize(
    "endpoint_config,expected_content_type,expected_body_contains",
    [
        pytest.param(
            {
                "path": "post_form_data",
                "method": "POST",
                "data": {},
            },
            None,
            None,
            id="empty_data_dict",
        ),
        pytest.param(
            {
                "path": "post_form_data",
                "method": "POST",
                "data": {"key1": "value1", "key2": "value2"},
            },
            "application/x-www-form-urlencoded",
            "key1=value1&key2=value2",
            id="data_as_dict",
        ),
        pytest.param(
            {
                "path": "post_form_data",
                "method": "POST",
                "data": [("key1", "value1"), ("key2", "value2"), ("key1", "another_value")],
            },
            "application/x-www-form-urlencoded",
            "key1=value1&key2=value2&key1=another_value",
            id="data_as_list_of_tuples",
        ),
        pytest.param(
            {
                "path": "post_raw_data",
                "method": "POST",
                "data": "raw string data",
            },
            None,
            "raw string data",
            id="data_as_string",
        ),
        pytest.param(
            {
                "path": "post_form_data",
                "method": "POST",
            },
            None,
            None,
            id="no_data_post",
        ),
    ],
)
def test_request_data_body(
    mock_api_server, endpoint_config, expected_content_type, expected_body_contains
) -> None:
    source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "test_data",
                    "endpoint": endpoint_config,
                },
            ],
        }
    )
    list(source.with_resources("test_data").add_limit(1))

    history = mock_api_server.request_history
    assert len(history) >= 1
    request = history[0]

    assert expected_content_type == request.headers.get("Content-Type")
    assert request.body == expected_body_contains


def test_data_and_json_mutual_exclusivity(mock_api_server):
    source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "posts",
                    "endpoint": {
                        "path": "posts/search",
                        "method": "POST",
                        "json": {"search_term": "test"},
                        "data": {"should_be_ignored": "yes"},
                    },
                },
            ],
        }
    )

    with pytest.raises(Exception) as exc_info:
        list(source.with_resources("posts").add_limit(1))

    assert "Cannot use both 'json' and 'data' parameters simultaneously" in str(exc_info.value)


def test_dependent_resource_post_data_param(mock_api_server):
    source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                "posts",
                {
                    "name": "post_comments",
                    "endpoint": {
                        "path": "post_comments_via_form_data",
                        "method": "POST",
                        "data": {
                            "post_id": "{resources.posts.id}",
                            "static_value": "constant",
                            "escaped_braces": "{{not_interpolated}}",
                        },
                        "paginator": "single_page",
                    },
                },
            ],
        }
    )

    list(source.with_resources("posts", "post_comments").add_limit(1))

    post_comments_calls = _filter_by_path_pattern(
        mock_api_server.request_history, "/post_comments_via_form_data"
    )
    assert len(post_comments_calls) == 5

    for index, call in enumerate(post_comments_calls):
        assert _parse_single_valued_qs(call.body) == {
            "post_id": str(index),
            "static_value": "constant",
            "escaped_braces": "{not_interpolated}",
        }


def test_post_data_param_with_incremental_values(mock_api_server):
    source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "posts_form_data_incremental",
                    "endpoint": {
                        "path": "/posts_form_data_incremental",
                        "method": "POST",
                        "data": {
                            "initial_value": "{incremental.initial_value}",
                            "start_value": "{incremental.start_value}",
                            "end_value": "{incremental.end_value}",
                            "escaped": "{{not_this}}",
                        },
                        "incremental": {
                            "cursor_path": "id",
                            "initial_value": 100,
                            "end_value": 200,
                        },
                        "paginator": "single_page",
                    },
                },
            ],
        }
    )

    list(source.with_resources("posts_form_data_incremental").add_limit(1))

    history = mock_api_server.request_history
    assert len(history) == 1
    request_call = history[0]
    actual_data = _parse_single_valued_qs(request_call.body)

    assert actual_data == {
        "initial_value": "100",
        "start_value": "100",
        "end_value": "200",
        "escaped": "{not_this}",
    }


def test_post_data_param_with_raw_data(mock_api_server):
    source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                "posts",
                {
                    "name": "post_comments",
                    "endpoint": {
                        "path": "post_comments_via_form_data",
                        "method": "POST",
                        "data": "{resources.posts.id}",
                        "paginator": "single_page",
                    },
                },
            ],
        }
    )

    list(source.with_resources("posts", "post_comments").add_limit(1))
    post_comments_calls = _filter_by_path_pattern(
        mock_api_server.request_history, "/post_comments_via_form_data"
    )

    assert len(post_comments_calls) == 5
    for index, call in enumerate(post_comments_calls):
        assert call.body == str(index)
        assert call.headers.get("Content-Type") is None, "Content-Type should be None for raw data"


def test_post_data_param_with_list_of_tuples(mock_api_server):
    """Test interpolation works with list of tuples data format"""
    source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                "posts",
                {
                    "name": "post_comments",
                    "endpoint": {
                        "path": "post_comments_via_form_data",
                        "method": "POST",
                        "data": [
                            ("post_id", "{resources.posts.id}"),
                            ("action", "comment"),
                            ("post_id", "{resources.posts.id}_duplicate"),  # Test duplicate keys
                            ("title", "{resources.posts.title}"),
                            ("escaped", "{{literal_braces}}"),
                        ],
                        "paginator": "single_page",
                    },
                },
            ],
        }
    )

    list(source.with_resources("posts", "post_comments").add_limit(1))

    post_comments_calls = _filter_by_path_pattern(
        mock_api_server.request_history, "/post_comments_via_form_data"
    )
    assert len(post_comments_calls) == 5

    for index, call in enumerate(post_comments_calls):
        actual_data = parse_qs(call.body)

        assert actual_data == {
            "post_id": [str(index), f"{index}_duplicate"],
            "action": ["comment"],
            "title": [f"Post {index}"],
            "escaped": ["{literal_braces}"],
        }
