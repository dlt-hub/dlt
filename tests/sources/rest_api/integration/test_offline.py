from typing import Any, List, Optional
from unittest import mock

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
from tests.utils import assert_load_info, assert_query_data, load_table_counts


def test_load_mock_api(mock_api_server):
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


def test_DltResource_gets_called(mock_api_server, mocker) -> None:
    @dlt.resource()
    def post_list():
        yield [{"id": "0"}, {"id": "1"}, {"id": "2"}]

    config: RESTAPIConfig = {
        "client": {"base_url": "http://api.example.com/"},
        "resource_defaults": {
            "write_disposition": "replace",
        },
        "resources": [
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
