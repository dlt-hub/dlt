from typing import Any
import dlt
import pytest
from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.sources.helpers.rest_client.paginators import SinglePagePaginator

from dlt.sources.rest_api import rest_api_source
from tests.pipeline.utils import assert_load_info, load_table_counts
from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
)


def _make_pipeline(destination_name: str):
    return dlt.pipeline(
        pipeline_name="rest_api",
        destination=destination_name,
        dataset_name="rest_api_data",
        full_refresh=True,
    )


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, local_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_rest_api_source(destination_config: DestinationTestConfiguration, request: Any) -> None:
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
    data = rest_api_source(config)
    pipeline = destination_config.setup_pipeline(request.node.name, dev_mode=True)
    load_info = pipeline.run(data)
    assert_load_info(load_info)
    table_names = [t["name"] for t in pipeline.default_schema.data_tables()]
    table_counts = load_table_counts(pipeline, *table_names)

    assert table_counts.keys() == {"pokemon_list", "berry", "location"}

    assert table_counts["pokemon_list"] == 1302
    assert table_counts["berry"] == 64
    assert table_counts["location"] == 1036


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, local_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_dependent_resource(destination_config: DestinationTestConfiguration, request: Any) -> None:
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

    data = rest_api_source(config)
    pipeline = destination_config.setup_pipeline(request.node.name, dev_mode=True)
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
