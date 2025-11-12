import dlt
import pytest

from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContainer

from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator
from dlt.sources.rest_api import rest_api_source, rest_api

from tests.common.configuration.utils import environment, toml_providers
from tests.sources.rest_api.utils import POKEMON_EXPECTED_TABLE_COUNTS
from tests.utils import ALL_DESTINATIONS
from tests.pipeline.utils import assert_load_info, load_table_counts, load_tables_to_dicts


def _make_pipeline(destination_name: str):
    return dlt.pipeline(
        pipeline_name="rest_api",
        destination=destination_name,
        dataset_name="rest_api_data",
        dev_mode=True,
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
    table_counts = load_table_counts(pipeline)

    assert table_counts.keys() == {"pokemon_list", "berry", "location"}
    assert table_counts.items() >= POKEMON_EXPECTED_TABLE_COUNTS.items()


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
    table_counts = load_table_counts(pipeline)

    assert set(table_counts.keys()) == {
        "pokemon",
        "pokemon__types",
        "pokemon__stats",
        "pokemon__moves__version_group_details",
        "pokemon__past_abilities",
        "pokemon__past_abilities__abilities",
        "pokemon__moves",
        "pokemon__game_indices",
        "pokemon__forms",
        "pokemon__abilities",
    }

    assert table_counts["pokemon"] == 2


@pytest.mark.parametrize("destination_name", ALL_DESTINATIONS)
def test_rest_api_source_with_data_parameter(destination_name: str) -> None:
    """Test REST API source with data parameter for form-encoded requests"""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://httpbingo.org",
        },
        "resources": [
            {
                "name": "post_form_test",
                "endpoint": {
                    "path": "post",
                    "method": "POST",
                    "data": {"field1": "value1", "field2": "value2", "field3": "test data"},
                    "paginator": SinglePagePaginator(),
                    "data_selector": "$",
                },
            },
            {
                "name": "post_raw_test",
                "endpoint": {
                    "path": "post",
                    "method": "POST",
                    "data": "raw string data",
                    "paginator": SinglePagePaginator(),
                    "data_selector": "$",
                },
            },
        ],
    }

    pipeline = _make_pipeline(destination_name)
    load_info = pipeline.run(rest_api_source(config))
    assert_load_info(load_info)
    table_counts = load_table_counts(pipeline, "post_form_test", "post_raw_test")
    assert table_counts["post_form_test"] == 1
    assert table_counts["post_raw_test"] == 1

    tables = load_tables_to_dicts(pipeline, exclude_system_cols=True)
    assert tables["post_form_test"][0]["data"] == "field1=value1&field2=value2&field3=test+data"
    assert tables["post_form_test__form__field1"] == [{"value": "value1"}]
    assert tables["post_form_test__form__field2"] == [{"value": "value2"}]
    assert tables["post_form_test__form__field3"] == [{"value": "test data"}]
    # Requests does not decode raw data from httpbingo.org so the raw data is returned
    assert (
        tables["post_raw_test"][0]["data"]
        == "data:application/octet-stream;base64,cmF3IHN0cmluZyBkYXRh"
    )


def test_rest_api_data_json_mutual_exclusivity() -> None:
    """Test that data and json parameters are mutually exclusive in REST API config"""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://httpbin.org",
        },
        "resources": [
            {
                "name": "invalid_test",
                "endpoint": {
                    "path": "post",
                    "method": "POST",
                    "json": {"key": "value"},
                    "data": {"other": "data"},
                    "paginator": SinglePagePaginator(),
                },
            },
        ],
    }

    pipeline = _make_pipeline("duckdb")
    from dlt.pipeline.exceptions import PipelineStepFailed

    with pytest.raises(PipelineStepFailed) as exc_info:
        pipeline.run(rest_api_source(config))

    # The actual error is wrapped in the pipeline exception
    assert "Cannot use both 'json' and 'data' parameters simultaneously" in str(exc_info.value)
