from copy import copy
from typing import cast
from unittest.mock import patch

import pytest

import dlt
import dlt.common
import dlt.common.exceptions
import dlt.extract
from dlt.common.utils import update_dict_nested
from dlt.sources.helpers.rest_client.paginators import (
    HeaderLinkPaginator,
    SinglePagePaginator,
)
from dlt.sources.rest_api import (
    rest_api_resources,
    rest_api_source,
)
from dlt.sources.rest_api.config_setup import (
    _make_endpoint_resource,
    _merge_resource_endpoints,
    _setup_single_entity_endpoint,
)
from dlt.sources.rest_api.typing import (
    Endpoint,
    EndpointResource,
    EndpointResourceBase,
    RESTAPIConfig,
)

try:
    from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
except ImportError:
    pass


from .source_configs import (
    INVALID_CONFIGS,
    VALID_CONFIGS,
)


@pytest.mark.parametrize("expected_message, exception, invalid_config", INVALID_CONFIGS)
def test_invalid_configurations(expected_message, exception, invalid_config):
    with pytest.raises(exception, match=expected_message):
        rest_api_source(invalid_config, name="invalid_config")


@pytest.mark.parametrize("valid_config", VALID_CONFIGS)
def test_valid_configurations(valid_config):
    rest_api_source(valid_config)


@pytest.mark.parametrize("config", VALID_CONFIGS)
def test_configurations_dict_is_not_modified_in_place(config):
    # deep clone dicts but do not touch instances of classes so ids still compare
    config_copy = update_dict_nested({}, config)
    rest_api_source(config)
    assert config_copy == config


def test_resource_expand() -> None:
    # convert str into name / path
    assert _make_endpoint_resource("path", {}) == {
        "name": "path",
        "endpoint": {"path": "path"},
    }
    # expand endpoint str into path
    assert _make_endpoint_resource({"name": "resource", "endpoint": "path"}, {}) == {
        "name": "resource",
        "endpoint": {"path": "path"},
    }
    # expand name into path with optional endpoint
    assert _make_endpoint_resource({"name": "resource"}, {}) == {
        "name": "resource",
        "endpoint": {"path": "resource"},
    }
    # endpoint path is optional
    assert _make_endpoint_resource({"name": "resource", "endpoint": {}}, {}) == {
        "name": "resource",
        "endpoint": {"path": "resource"},
    }


def test_resource_endpoint_deep_merge() -> None:
    # columns deep merged
    resource = _make_endpoint_resource(
        {
            "name": "resources",
            "columns": [
                {"name": "col_a", "data_type": "bigint"},
                {"name": "col_b"},
            ],
        },
        {
            "columns": [
                {"name": "col_a", "data_type": "text", "primary_key": True},
                {"name": "col_c", "data_type": "timestamp", "partition": True},
            ]
        },
    )
    assert resource["columns"] == {
        # data_type and primary_key merged
        "col_a": {"name": "col_a", "data_type": "bigint", "primary_key": True},
        # from defaults
        "col_c": {"name": "col_c", "data_type": "timestamp", "partition": True},
        # from resource (partial column moved to the end)
        "col_b": {"name": "col_b"},
    }
    # json and params deep merged
    resource = _make_endpoint_resource(
        {
            "name": "resources",
            "endpoint": {
                "json": {"param1": "A", "param2": "B"},
                "params": {"param1": "A", "param2": "B"},
            },
        },
        {
            "endpoint": {
                "json": {"param1": "X", "param3": "Y"},
                "params": {"param1": "X", "param3": "Y"},
            }
        },
    )
    assert resource["endpoint"] == {
        "json": {"param1": "A", "param3": "Y", "param2": "B"},
        "params": {"param1": "A", "param3": "Y", "param2": "B"},
        "path": "resources",
    }


def test_resource_endpoint_shallow_merge() -> None:
    # merge paginators and other typed dicts as whole
    resource_config: EndpointResource = {
        "name": "resources",
        "max_table_nesting": 5,
        "write_disposition": {"disposition": "merge", "strategy": "scd2"},
        "schema_contract": {"tables": "freeze"},
        "endpoint": {
            "paginator": {"type": "cursor", "cursor_param": "cursor"},
            "incremental": {"cursor_path": "$", "start_param": "since"},
        },
    }

    resource = _make_endpoint_resource(
        resource_config,
        {
            "max_table_nesting": 1,
            "parallelized": True,
            "write_disposition": {
                "disposition": "replace",
            },
            "schema_contract": {"columns": "freeze"},
            "endpoint": {
                "paginator": {
                    "type": "header_link",
                },
                "incremental": {
                    "cursor_path": "response.id",
                    "start_param": "since",
                    "end_param": "before",
                },
            },
        },
    )
    # resource should keep all values, just parallel is added
    expected_resource = copy(resource_config)
    expected_resource["parallelized"] = True
    assert resource == expected_resource


def test_resource_merge_with_objects() -> None:
    paginator = SinglePagePaginator()
    incremental = dlt.sources.incremental[int]("id", row_order="asc")
    resource = _make_endpoint_resource(
        {
            "name": "resource",
            "endpoint": {
                "path": "path/to",
                "paginator": paginator,
                "params": {"since": incremental},
            },
        },
        {
            "table_name": lambda item: item["type"],
            "endpoint": {
                "paginator": HeaderLinkPaginator(),
                "params": {"since": dlt.sources.incremental[int]("id", row_order="desc")},
            },
        },
    )
    # objects are as is, not cloned
    assert resource["endpoint"]["paginator"] is paginator  # type: ignore[index]
    assert resource["endpoint"]["params"]["since"] is incremental  # type: ignore[index]
    # callable coming from default
    assert callable(resource["table_name"])


def test_resource_merge_with_none() -> None:
    endpoint_config: EndpointResource = {
        "name": "resource",
        "endpoint": {"path": "user/{id}", "paginator": None, "data_selector": None},
    }
    # None should be able to reset the default
    resource = _make_endpoint_resource(
        endpoint_config,
        {"endpoint": {"paginator": SinglePagePaginator(), "data_selector": "data"}},
    )
    # nones will overwrite defaults
    assert resource == endpoint_config


def test_setup_for_single_item_endpoint() -> None:
    # single item should revert to single page validator
    endpoint = _setup_single_entity_endpoint({"path": "user/{id}"})
    assert endpoint["data_selector"] == "$"
    assert isinstance(endpoint["paginator"], SinglePagePaginator)

    # this is not single page
    endpoint = _setup_single_entity_endpoint({"path": "user/{id}/messages"})
    assert "data_selector" not in endpoint

    # simulate using None to remove defaults
    endpoint_config: EndpointResource = {
        "name": "resource",
        "endpoint": {"path": "user/{id}", "paginator": None, "data_selector": None},
    }
    # None should be able to reset the default
    resource = _make_endpoint_resource(
        endpoint_config,
        {"endpoint": {"paginator": HeaderLinkPaginator(), "data_selector": "data"}},
    )

    endpoint = _setup_single_entity_endpoint(cast(Endpoint, resource["endpoint"]))
    assert endpoint["data_selector"] == "$"
    assert isinstance(endpoint["paginator"], SinglePagePaginator)


def test_resource_schema() -> None:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            "users",
            {
                "name": "user",
                "endpoint": {
                    "path": "user/{id}",
                    "paginator": None,
                    "data_selector": None,
                    "params": {
                        "id": {
                            "type": "resolve",
                            "field": "id",
                            "resource": "users",
                        },
                    },
                },
            },
        ],
    }
    resources = rest_api_resources(config)
    assert len(resources) == 2
    resource = resources[0]
    assert resource.name == "users"
    assert resources[1].name == "user"


def test_resource_hints_are_passed_to_resource_constructor() -> None:
    config: RESTAPIConfig = {
        "client": {"base_url": "https://api.example.com"},
        "resources": [
            {
                "name": "posts",
                "endpoint": {
                    "params": {
                        "limit": 100,
                    },
                },
                "table_name": "a_table",
                "max_table_nesting": 2,
                "write_disposition": "merge",
                "columns": {"a_text": {"name": "a_text", "data_type": "text"}},
                "primary_key": "a_pk",
                "merge_key": "a_merge_key",
                "schema_contract": {"tables": "evolve"},
                "table_format": "iceberg",
                "selected": False,
            },
        ],
    }

    with patch.object(dlt, "resource", wraps=dlt.resource) as mock_resource_constructor:
        rest_api_resources(config)
        mock_resource_constructor.assert_called_once()
        expected_kwargs = {
            "table_name": "a_table",
            "max_table_nesting": 2,
            "write_disposition": "merge",
            "columns": {"a_text": {"name": "a_text", "data_type": "text"}},
            "primary_key": "a_pk",
            "merge_key": "a_merge_key",
            "schema_contract": {"tables": "evolve"},
            "table_format": "iceberg",
            "selected": False,
        }
        for arg in expected_kwargs.items():
            _, kwargs = mock_resource_constructor.call_args_list[0]
            assert arg in kwargs.items()


def test_resource_defaults_params_get_merged() -> None:
    resource_defaults: EndpointResourceBase = {
        "primary_key": "id",
        "write_disposition": "merge",
        "endpoint": {
            "params": {
                "per_page": 30,
            },
        },
    }

    resource: EndpointResource = {
        "endpoint": {
            "path": "issues",
            "params": {
                "sort": "updated",
                "direction": "desc",
                "state": "open",
            },
        },
    }
    merged_resource = _merge_resource_endpoints(resource_defaults, resource)
    assert merged_resource["endpoint"]["params"]["per_page"] == 30  # type: ignore[index]


def test_resource_defaults_params_get_overwritten() -> None:
    resource_defaults: EndpointResourceBase = {
        "primary_key": "id",
        "write_disposition": "merge",
        "endpoint": {
            "params": {
                "per_page": 30,
            },
        },
    }

    resource: EndpointResource = {
        "endpoint": {
            "path": "issues",
            "params": {
                "per_page": 50,
                "sort": "updated",
            },
        },
    }
    merged_resource = _merge_resource_endpoints(resource_defaults, resource)
    assert merged_resource["endpoint"]["params"]["per_page"] == 50  # type: ignore[index]


def test_resource_defaults_params_no_resource_params() -> None:
    resource_defaults: EndpointResourceBase = {
        "primary_key": "id",
        "write_disposition": "merge",
        "endpoint": {
            "params": {
                "per_page": 30,
            },
        },
    }

    resource: EndpointResource = {
        "endpoint": {
            "path": "issues",
        },
    }
    merged_resource = _merge_resource_endpoints(resource_defaults, resource)
    assert merged_resource["endpoint"]["params"]["per_page"] == 30  # type: ignore[index]


def test_resource_defaults_no_params() -> None:
    resource_defaults: EndpointResourceBase = {
        "primary_key": "id",
        "write_disposition": "merge",
    }

    resource: EndpointResource = {
        "endpoint": {
            "path": "issues",
            "params": {
                "per_page": 50,
                "sort": "updated",
            },
        },
    }
    merged_resource = _merge_resource_endpoints(resource_defaults, resource)
    assert merged_resource["endpoint"]["params"] == {  # type: ignore[index]
        "per_page": 50,
        "sort": "updated",
    }


def test_accepts_DltResource_in_resources() -> None:
    @dlt.resource(selected=False)
    def repositories():
        """A seed list of repositories to fetch"""
        yield [{"name": "dlt"}, {"name": "verified-sources"}, {"name": "dlthub-education"}]

    config: RESTAPIConfig = {
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
    }

    source = rest_api_source(config)
    assert list(source.resources.keys()) == ["repositories", "issues"]
    assert list(source.selected_resources.keys()) == ["issues"]


def test_resource_defaults_dont_apply_to_DltResource() -> None:
    @dlt.resource()
    def repositories():
        """A seed list of repositories to fetch"""
        yield [{"name": "dlt"}, {"name": "verified-sources"}, {"name": "dlthub-education"}]

    config: RESTAPIConfig = {
        "client": {"base_url": "https://github.com/api/v2"},
        "resource_defaults": {
            "write_disposition": "replace",
        },
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
    }

    source = rest_api_source(config)
    assert source.resources["issues"].write_disposition == "replace"
    assert source.resources["repositories"].write_disposition != "replace", (
        "DltResource defined outside of the RESTAPIConfig object is influenced by the content of"
        " the RESTAPIConfig"
    )
