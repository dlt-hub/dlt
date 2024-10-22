import pytest
from typing import List

from dlt.sources.rest_api import (
    rest_api_source,
)

from dlt.sources.rest_api.config_setup import (
    create_response_hooks,
    _handle_response_action,
)
from dlt.sources.rest_api.typing import (
    RESTAPIConfig,
    ResponseAction,
)

try:
    from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
except ImportError:
    pass


def test_create_multiple_response_actions():
    def custom_hook(response, *args, **kwargs):
        return response

    response_actions: List[ResponseAction] = [
        custom_hook,
        {"status_code": 404, "action": "ignore"},
        {"content": "Not found", "action": "ignore"},
        {"status_code": 200, "content": "some text", "action": "ignore"},
    ]
    hooks = create_response_hooks(response_actions)
    assert len(hooks["response"]) == 5

    response_actions_2: List[ResponseAction] = [
        custom_hook,
        {"status_code": 200, "action": custom_hook},
    ]
    hooks_2 = create_response_hooks(response_actions_2)
    assert len(hooks_2["response"]) == 3


def test_response_action_raises_type_error(mocker):
    class C:
        pass

    response = mocker.Mock()
    response.status_code = 200

    with pytest.raises(ValueError) as e_1:
        _handle_response_action(response, {"status_code": 200, "action": C()})  # type: ignore[typeddict-item]
    assert e_1.match("does not conform to expected type")

    with pytest.raises(ValueError) as e_2:
        _handle_response_action(response, {"status_code": 200, "action": 123})  # type: ignore[typeddict-item]
    assert e_2.match("does not conform to expected type")

    assert ("ignore", None) == _handle_response_action(
        response, {"status_code": 200, "action": "ignore"}
    )
    assert ("foobar", None) == _handle_response_action(
        response, {"status_code": 200, "action": "foobar"}
    )


def test_parses_hooks_from_response_actions(mocker):
    response = mocker.Mock()
    response.status_code = 200

    hook_1 = mocker.Mock()
    hook_2 = mocker.Mock()

    assert (None, [hook_1]) == _handle_response_action(
        response, {"status_code": 200, "action": hook_1}
    )
    assert (None, [hook_1, hook_2]) == _handle_response_action(
        response, {"status_code": 200, "action": [hook_1, hook_2]}
    )


def test_config_validation_for_response_actions(mocker):
    mock_response_hook_1 = mocker.Mock()
    mock_response_hook_2 = mocker.Mock()
    config_1: RESTAPIConfig = {
        "client": {"base_url": "https://api.example.com"},
        "resources": [
            {
                "name": "posts",
                "endpoint": {
                    "response_actions": [
                        {
                            "status_code": 200,
                            "action": mock_response_hook_1,
                        },
                    ],
                },
            },
        ],
    }

    rest_api_source(config_1)

    config_2: RESTAPIConfig = {
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

    rest_api_source(config_2)

    config_3: RESTAPIConfig = {
        "client": {"base_url": "https://api.example.com"},
        "resources": [
            {
                "name": "posts",
                "endpoint": {
                    "response_actions": [
                        {
                            "status_code": 200,
                            "action": [mock_response_hook_1, mock_response_hook_2],
                        },
                    ],
                },
            },
        ],
    }

    rest_api_source(config_3)
