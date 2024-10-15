import pytest
from dlt.common import json
from dlt.sources.helpers.requests import Response
from dlt.sources.helpers.rest_client.exceptions import IgnoreResponseException
from dlt.sources.rest_api import create_response_hooks, rest_api_source
from dlt.extract.exceptions import ResourceExtractionError


def make_mock_source_for_response_actions(dependent_endpoint_path, response_actions):
    return rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                "posts",
                {
                    "name": "post_details",
                    "endpoint": {
                        "path": dependent_endpoint_path,
                        "params": {
                            "post_id": {
                                "type": "resolve",
                                "resource": "posts",
                                "field": "id",
                            }
                        },
                        "response_actions": response_actions,
                    },
                },
            ],
        }
    )


def test_ignoring_endpoint_returning_404(mock_api_server):
    mock_source = make_mock_source_for_response_actions(
        dependent_endpoint_path="posts/{post_id}/some_details_404",
        response_actions=[
            {
                "status_code": 404,
                "action": "ignore",
            },
        ],
    )

    res = list(mock_source.with_resources("posts", "post_details").add_limit(1))

    assert res[:5] == [
        {"id": 0, "body": "Post body 0"},
        {"id": 0, "title": "Post 0"},
        {"id": 1, "title": "Post 1"},
        {"id": 2, "title": "Post 2"},
        {"id": 3, "title": "Post 3"},
    ]


def test_ignoring_endpoint_returning_404_others_422(mock_api_server):
    mock_source = make_mock_source_for_response_actions(
        dependent_endpoint_path="posts/{post_id}/some_details_404_others_422",
        response_actions=[
            {
                "status_code": 422,
                "action": "ignore",
            },
            {
                "status_code": 404,
                "action": "ignore",
            },
        ],
    )

    expected_res = [
        {"id": 0, "body": "Post body 0"},
        {"id": 0, "title": "Post 0"},
        {"id": 1, "title": "Post 1"},
        {"id": 2, "title": "Post 2"},
        {"id": 3, "title": "Post 3"},
    ]

    res = list(mock_source.with_resources("posts", "post_details").add_limit(1))

    assert res[:5] == expected_res

    # Different order of response_actions should not affect the result

    mock_source = make_mock_source_for_response_actions(
        dependent_endpoint_path="posts/{post_id}/some_details_404_others_422",
        response_actions=[
            {
                "status_code": 404,
                "action": "ignore",
            },
            {
                "status_code": 422,
                "action": "ignore",
            },
        ],
    )

    res = list(mock_source.with_resources("posts", "post_details").add_limit(1))

    assert res[:5] == expected_res


def test_response_action_ignore_on_content(mock_api_server):
    mock_source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "post_details",
                    "endpoint": {
                        "path": "posts/0/some_details_404",
                        "response_actions": [
                            {"content": "Post body 0", "action": "ignore"},
                        ],
                    },
                },
            ],
        }
    )

    res = list(mock_source.with_resources("post_details"))
    assert res == []


def test_ignoring_endpoint_returning_204(mock_api_server):
    mock_source = make_mock_source_for_response_actions(
        dependent_endpoint_path="posts/{post_id}/some_details_204",
        response_actions=[
            {
                "status_code": 204,
                "action": "ignore",
            },
        ],
    )

    res = list(mock_source.with_resources("posts", "post_details").add_limit(1))

    assert res[:5] == [
        {"id": 0, "body": "Post body 0"},
        {"id": 0, "title": "Post 0"},
        {"id": 1, "title": "Post 1"},
        {"id": 2, "title": "Post 2"},
        {"id": 3, "title": "Post 3"},
    ]


def test_empty_response_actions_raise_on_404_by_default(mock_api_server):
    mock_source = rest_api_source(
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "post_details",
                    "endpoint": {
                        "path": "posts/1/some_details_404",
                    },
                },
            ],
        }
    )

    with pytest.raises(ResourceExtractionError) as exc_info:
        list(mock_source.with_resources("post_details"))

    assert exc_info.match("404 Client Error")


def test_non_empty_response_actions_raise_on_error_by_default(mock_api_server):
    mock_source = make_mock_source_for_response_actions(
        dependent_endpoint_path="posts/{post_id}/some_details_404_others_422",
        response_actions=[
            {
                "status_code": 404,
                "action": "ignore",
            },
        ],
    )

    with pytest.raises(ResourceExtractionError) as exc_info:
        list(list(mock_source.with_resources("posts", "post_details").add_limit(1)))

    assert exc_info.match("422 Client Error")


def test_response_action_on_status_code(mock_api_server, mocker):
    def custom_hook(response, *args, **kwargs):
        raise IgnoreResponseException

    mock_response_hook = mocker.Mock(side_effect=custom_hook)
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
    hooks = create_response_hooks(response_actions)
    assert len(hooks.get("response")) == 3  # 2 custom hooks + 1 fallback hook

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
