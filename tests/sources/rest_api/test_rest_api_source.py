import dlt
import pytest
from unittest.mock import patch, MagicMock
from requests import Response, Request

from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContainer

from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator
from dlt.sources.rest_api import rest_api_source, rest_api

from tests.common.configuration.utils import environment, toml_providers
from tests.utils import ALL_DESTINATIONS, assert_load_info, load_table_counts


def _make_pipeline(destination_name: str):
    return dlt.pipeline(
        pipeline_name="rest_api",
        destination=destination_name,
        dataset_name="rest_api_data",
        full_refresh=True,
    )


def test_rest_api_config_provider(toml_providers: ConfigProvidersContainer) -> None:
    # mock dicts in toml provider
    dlt.config["client"] = {
        "base_url": "https://pokeapi.co/api/v2/",
    }
    dlt.config["resources"] = [
        {
            "name": "pokemon_list",
            "endpoint": {
                "path": "pokemon",
                "paginator": SinglePagePaginator(),
                "data_selector": "results",
                "params": {
                    "limit": 10,
                },
            },
        }
    ]
    pipeline = _make_pipeline("duckdb")
    load_info = pipeline.run(rest_api())
    print(load_info)


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("invocation_type", ("deco", "factory"))
def test_rest_api_source(destination_name: str, invocation_type: str) -> None:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://pokeapi.co/api/v2/",
        },
        "resource_defaults": {
            "endpoint": {
                "params": {
                    "limit": 1000,
                },
            }
        },
        "resources": [
            {
                "name": "pokemon_list",
                "endpoint": "pokemon",
            },
            "berry",
            "location",
        ],
    }
    if invocation_type == "deco":
        data = rest_api(**config)
    else:
        data = rest_api_source(config)
    pipeline = _make_pipeline(destination_name)
    load_info = pipeline.run(data)
    print(load_info)
    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    assert table_counts.keys() == {"pokemon_list", "berry", "location"}

    assert table_counts["pokemon_list"] == 1302
    assert table_counts["berry"] == 64
    assert table_counts["location"] == 1036


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("invocation_type", ("deco", "factory"))
def test_dependent_resource(destination_name: str, invocation_type: str) -> None:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://pokeapi.co/api/v2/",
        },
        "resource_defaults": {
            "endpoint": {
                "params": {
                    "limit": 1000,
                },
            }
        },
        "resources": [
            {
                "name": "pokemon_list",
                "endpoint": {
                    "path": "pokemon",
                    "paginator": SinglePagePaginator(),
                    "data_selector": "results",
                    "params": {
                        "limit": 2,
                    },
                },
                "selected": False,
            },
            {
                "name": "pokemon",
                "endpoint": {
                    "path": "pokemon/{name}",
                    "params": {
                        "name": {
                            "type": "resolve",
                            "resource": "pokemon_list",
                            "field": "name",
                        },
                    },
                },
            },
        ],
    }

    if invocation_type == "deco":
        data = rest_api(**config)
    else:
        data = rest_api_source(config)
    pipeline = _make_pipeline(destination_name)
    load_info = pipeline.run(data)
    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    assert set(table_counts.keys()) == {
        "pokemon",
        "pokemon__types",
        "pokemon__stats",
        "pokemon__moves__version_group_details",
        "pokemon__moves",
        "pokemon__game_indices",
        "pokemon__forms",
        "pokemon__abilities",
    }

    assert table_counts["pokemon"] == 2


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("invocation_type", ("deco", "factory"))
@patch("dlt.sources.helpers.rest_client.client.RESTClient._send_request")
def test_request_headers(mock: MagicMock, destination_name: str, invocation_type: str) -> None:
    mock_resp = Response()
    mock_resp.status_code = 200
    mock_resp.json = lambda: {"success": "ok"}  # type: ignore
    mock.return_value = mock_resp

    @dlt.resource
    def authenticate():
        yield [{"token": 1}]

    base_url = "https://api.example.com"
    config: RESTAPIConfig = {
        "client": {"base_url": base_url, "headers": {"foo": "bar"}},
        "resources": [
            {
                "name": "chicken",
                "endpoint": {
                    "path": "chicken",
                    "headers": {"token": "{token}", "num": "2"},
                    "params": {
                        "token": {
                            "type": "resolve",
                            "field": "token",
                            "resource": "authenticate",
                        },
                    },
                },
            },
            authenticate(),
        ],
    }

    if invocation_type == "deco":
        data = rest_api(**config)
    else:
        data = rest_api_source(config)
    pipeline = _make_pipeline(destination_name)
    pipeline.run(data)

    mock.assert_called()
    args, kwargs = mock.call_args
    request_param: Request = args[0]

    assert request_param.url == f"{base_url}/chicken"
    assert request_param.headers == {"foo": "bar", "token": "1", "num": "2"}


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("invocation_type", ("deco", "factory"))
@patch("dlt.sources.helpers.rest_client.client.RESTClient._send_request")
def test_request_headers_dynamic_key(
    mock: MagicMock, destination_name: str, invocation_type: str
) -> None:
    mock_resp = Response()
    mock_resp.status_code = 200
    mock_resp.json = lambda: {"success": "ok"}  # type: ignore
    mock.return_value = mock_resp

    @dlt.resource
    def authenticate():
        yield [{"token": 1}]

    base_url = "https://api.example.com"
    config: RESTAPIConfig = {
        "client": {"base_url": base_url, "headers": {"foo": "bar"}},
        "resources": [
            {
                "name": "chicken",
                "endpoint": {
                    "path": "chicken",
                    "headers": {"{token}": "{token}", "num": "2"},
                    "params": {
                        "token": {
                            "type": "resolve",
                            "field": "token",
                            "resource": "authenticate",
                        },
                    },
                },
            },
            authenticate(),
        ],
    }

    if invocation_type == "deco":
        data = rest_api(**config)
    else:
        data = rest_api_source(config)
    pipeline = _make_pipeline(destination_name)
    pipeline.run(data)

    mock.assert_called()
    args, kwargs = mock.call_args
    request_param: Request = args[0]

    assert request_param.url == f"{base_url}/chicken"
    assert request_param.headers == {"foo": "bar", "1": "1", "num": "2"}


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
@pytest.mark.parametrize("invocation_type", ("deco", "factory"))
@patch("dlt.sources.helpers.rest_client.client.RESTClient._send_request")
def test_request_headers_nested(
    mock: MagicMock, destination_name: str, invocation_type: str
) -> None:
    mock_resp = Response()
    mock_resp.status_code = 200
    mock_resp.json = lambda: {"success": "ok"}  # type: ignore
    mock.return_value = mock_resp

    @dlt.resource
    def authenticate():
        yield [{"token": 1}]

    base_url = "https://api.example.com"
    config: RESTAPIConfig = {
        "client": {"base_url": base_url, "headers": {"foo": "bar"}},
        "resources": [
            {
                "name": "chicken",
                "endpoint": {
                    "path": "chicken",
                    "headers": {
                        "{token}": "{token}",
                        "num": "2",
                        "nested": {"nested": "{token}", "{token}": "other"},
                    },
                    "params": {
                        "token": {
                            "type": "resolve",
                            "field": "token",
                            "resource": "authenticate",
                        },
                    },
                },
            },
            authenticate(),
        ],
    }

    if invocation_type == "deco":
        data = rest_api(**config)
    else:
        data = rest_api_source(config)
    pipeline = _make_pipeline(destination_name)
    pipeline.run(data)

    mock.assert_called()
    args, kwargs = mock.call_args
    request_param: Request = args[0]

    assert request_param.url == f"{base_url}/chicken"
    assert request_param.headers == {
        "foo": "bar",
        "1": "1",
        "num": "2",
        "nested": {"nested": "1", "1": "other"},
    }
