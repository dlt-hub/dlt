from dlt.common import json
from dlt.sources.helpers.requests import Response
from dlt.sources.helpers.rest_client.exceptions import IgnoreResponseException
from dlt.sources.rest_api import create_response_hooks, rest_api_source


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
