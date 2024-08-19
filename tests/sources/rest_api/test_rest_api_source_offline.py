import json
import pytest
from typing import Any, cast, Dict
import pendulum
from unittest import mock

import dlt
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.sources.helpers.rest_client.paginators import BaseReferencePaginator
from dlt.sources.helpers.requests import Response

from tests.utils import assert_load_info, load_table_counts, assert_query_data

from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api import (
    RESTAPIConfig,
    ClientConfig,
    EndpointResource,
    Endpoint,
    create_response_hooks,
)
from tests.sources.rest_api.conftest import DEFAULT_PAGE_SIZE, DEFAULT_TOTAL_PAGES


def test_load_mock_api(mock_api_server):
    # import os
    # os.environ["EXTRACT__NEXT_ITEM_MODE"] = "fifo"
    # os.environ["EXTRACT__MAX_PARALLEL_ITEMS"] = "1"
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_mock",
        destination="duckdb",
        dataset_name="rest_api_mock",
        full_refresh=True,
    )

    mock_source = rest_api_source(
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
        }
    )

    load_info = pipeline.run(mock_source)
    print(load_info)
    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    assert table_counts.keys() == {"posts", "post_comments", "post_details"}

    assert table_counts["posts"] == DEFAULT_PAGE_SIZE * DEFAULT_TOTAL_PAGES
    assert table_counts["post_details"] == DEFAULT_PAGE_SIZE * DEFAULT_TOTAL_PAGES
    assert table_counts["post_comments"] == 50 * DEFAULT_PAGE_SIZE * DEFAULT_TOTAL_PAGES

    with pipeline.sql_client() as client:
        posts_table = client.make_qualified_table_name("posts")
        posts_details_table = client.make_qualified_table_name("post_details")
        post_comments_table = client.make_qualified_table_name("post_comments")

    print(pipeline.default_schema.to_pretty_yaml())

    assert_query_data(
        pipeline,
        f"SELECT title FROM {posts_table} ORDER BY id limit 25",
        [f"Post {i}" for i in range(25)],
    )

    assert_query_data(
        pipeline,
        f"SELECT body FROM {posts_details_table} ORDER BY id limit 25",
        [f"Post body {i}" for i in range(25)],
    )

    assert_query_data(
        pipeline,
        f"SELECT body FROM {post_comments_table} ORDER BY post_id, id limit 5",
        [f"Comment {i} for post 0" for i in range(5)],
    )


def test_ignoring_endpoint_returning_404(mock_api_server):
    mock_source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                "posts",
                {
                    "name": "post_details",
                    "endpoint": {
                        "path": "posts/{post_id}/some_details_404",
                        "params": {
                            "post_id": {
                                "type": "resolve",
                                "resource": "posts",
                                "field": "id",
                            }
                        },
                        "response_actions": [
                            {
                                "status_code": 404,
                                "action": "ignore",
                            },
                        ],
                    },
                },
            ],
        }
    )

    res = list(mock_source.with_resources("posts", "post_details").add_limit(1))

    assert res[:5] == [
        {"id": 0, "body": "Post body 0"},
        {"id": 0, "title": "Post 0"},
        {"id": 1, "title": "Post 1"},
        {"id": 2, "title": "Post 2"},
        {"id": 3, "title": "Post 3"},
    ]


def test_source_with_post_request(mock_api_server):
    class JSONBodyPageCursorPaginator(BaseReferencePaginator):
        def update_state(self, response):
            self._next_reference = response.json().get("next_page")

        def update_request(self, request):
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
                        "json": {"ids_greater_than": 50, "page_size": 25, "page_count": 4},
                        "paginator": JSONBodyPageCursorPaginator(),
                    },
                }
            ],
        }
    )

    res = list(mock_source.with_resources("search_posts"))

    for i in range(49):
        assert res[i] == {"id": 51 + i, "title": f"Post {51 + i}"}


def test_unauthorized_access_to_protected_endpoint(mock_api_server):
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_mock",
        destination="duckdb",
        dataset_name="rest_api_mock",
        full_refresh=True,
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


def test_load_mock_api_typeddict_config(mock_api_server):
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_mock",
        destination="duckdb",
        dataset_name="rest_api_mock",
        full_refresh=True,
    )

    mock_source = rest_api_source(
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
        )
    )

    load_info = pipeline.run(mock_source)
    print(load_info)
    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    assert table_counts.keys() == {"posts", "post_comments"}

    assert table_counts["posts"] == DEFAULT_PAGE_SIZE * DEFAULT_TOTAL_PAGES
    assert table_counts["post_comments"] == DEFAULT_PAGE_SIZE * DEFAULT_TOTAL_PAGES * 50


def test_response_action_on_status_code(mock_api_server, mocker):
    mock_response_hook = mocker.Mock()
    mock_source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "post_details",
                    "endpoint": {
                        "path": "posts/1/some_details_404",
                        "response_actions": [
                            {
                                "status_code": 404,
                                "action": mock_response_hook,
                            },
                        ],
                    },
                },
            ],
        }
    )

    list(mock_source.with_resources("post_details").add_limit(1))

    mock_response_hook.assert_called_once()


def test_response_action_on_every_response(mock_api_server, mocker):
    def custom_hook(request, *args, **kwargs):
        return request

    mock_response_hook = mocker.Mock(side_effect=custom_hook)
    mock_source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "posts",
                    "endpoint": {
                        "response_actions": [
                            mock_response_hook,
                        ],
                    },
                },
            ],
        }
    )

    list(mock_source.with_resources("posts").add_limit(1))

    mock_response_hook.assert_called_once()


def test_multiple_response_actions_on_every_response(mock_api_server, mocker):
    def custom_hook(response, *args, **kwargs):
        return response

    mock_response_hook_1 = mocker.Mock(side_effect=custom_hook)
    mock_response_hook_2 = mocker.Mock(side_effect=custom_hook)
    mock_source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "posts",
                    "endpoint": {
                        "response_actions": [
                            mock_response_hook_1,
                            mock_response_hook_2,
                        ],
                    },
                },
            ],
        }
    )

    list(mock_source.with_resources("posts").add_limit(1))

    mock_response_hook_1.assert_called_once()
    mock_response_hook_2.assert_called_once()


def test_response_actions_called_in_order(mock_api_server, mocker):
    def set_encoding(response: Response, *args, **kwargs) -> Response:
        assert response.encoding != "windows-1252"
        response.encoding = "windows-1252"
        return response

    def add_field(response: Response, *args, **kwargs) -> Response:
        assert response.encoding == "windows-1252"
        payload = response.json()
        for record in payload["data"]:
            record["custom_field"] = "foobar"
        modified_content: bytes = json.dumps(payload).encode("utf-8")
        response._content = modified_content
        return response

    mock_response_hook_1 = mocker.Mock(side_effect=set_encoding)
    mock_response_hook_2 = mocker.Mock(side_effect=add_field)

    response_actions = [
        mock_response_hook_1,
        {"status_code": 200, "action": mock_response_hook_2},
    ]
    hooks = cast(Dict[str, Any], create_response_hooks(response_actions))
    assert len(hooks.get("response")) == 2

    mock_source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "posts",
                    "endpoint": {
                        "response_actions": [
                            mock_response_hook_1,
                            {"status_code": 200, "action": mock_response_hook_2},
                        ],
                    },
                },
            ],
        }
    )

    data = list(mock_source.with_resources("posts").add_limit(1))

    mock_response_hook_1.assert_called_once()
    mock_response_hook_2.assert_called_once()

    assert all(record["custom_field"] == "foobar" for record in data)


def test_posts_with_inremental_date_conversion(mock_api_server) -> None:
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
