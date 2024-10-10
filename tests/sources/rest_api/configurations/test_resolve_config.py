import re
from copy import deepcopy

import pytest
from graphlib import CycleError  # type: ignore

from dlt.sources.rest_api import (
    rest_api_resources,
    rest_api_source,
)
from dlt.sources.rest_api.config_setup import (
    _bind_path_params,
    process_parent_data_item,
)
from dlt.sources.rest_api.typing import (
    EndpointResource,
    ResolvedParam,
    RESTAPIConfig,
)

try:
    from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
except ImportError:
    from dlt.sources.helpers.rest_client.paginators import (
        JSONResponsePaginator as JSONLinkPaginator,
    )


try:
    from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator
except ImportError:
    pass


def test_bind_path_param() -> None:
    three_params: EndpointResource = {
        "name": "comments",
        "endpoint": {
            "path": "{org}/{repo}/issues/{id}/comments",
            "params": {
                "org": "dlt-hub",
                "repo": "dlt",
                "id": {
                    "type": "resolve",
                    "field": "id",
                    "resource": "issues",
                },
            },
        },
    }
    tp_1 = deepcopy(three_params)
    _bind_path_params(tp_1)

    # do not replace resolved params
    assert tp_1["endpoint"]["path"] == "dlt-hub/dlt/issues/{id}/comments"  # type: ignore[index]
    # bound params popped
    assert len(tp_1["endpoint"]["params"]) == 1  # type: ignore[index]
    assert "id" in tp_1["endpoint"]["params"]  # type: ignore[index]

    tp_2 = deepcopy(three_params)
    tp_2["endpoint"]["params"]["id"] = 12345  # type: ignore[index]
    _bind_path_params(tp_2)
    assert tp_2["endpoint"]["path"] == "dlt-hub/dlt/issues/12345/comments"  # type: ignore[index]
    assert len(tp_2["endpoint"]["params"]) == 0  # type: ignore[index]

    # param missing
    tp_3 = deepcopy(three_params)
    with pytest.raises(ValueError) as val_ex:
        del tp_3["endpoint"]["params"]["id"]  # type: ignore[index, union-attr]
        _bind_path_params(tp_3)
    # path is a part of an exception
    assert tp_3["endpoint"]["path"] in str(val_ex.value)  # type: ignore[index]

    # path without params
    tp_4 = deepcopy(three_params)
    tp_4["endpoint"]["path"] = "comments"  # type: ignore[index]
    # no unbound params
    del tp_4["endpoint"]["params"]["id"]  # type: ignore[index, union-attr]
    tp_5 = deepcopy(tp_4)
    _bind_path_params(tp_4)
    assert tp_4 == tp_5

    # resolved param will remain unbounded and
    tp_6 = deepcopy(three_params)
    tp_6["endpoint"]["path"] = "{org}/{repo}/issues/1234/comments"  # type: ignore[index]
    with pytest.raises(NotImplementedError):
        _bind_path_params(tp_6)


def test_process_parent_data_item() -> None:
    resolve_params = [
        ResolvedParam("id", {"field": "obj_id", "resource": "issues", "type": "resolve"})
    ]
    bound_path, parent_record = process_parent_data_item(
        "dlt-hub/dlt/issues/{id}/comments", {"obj_id": 12345}, resolve_params, None
    )
    assert bound_path == "dlt-hub/dlt/issues/12345/comments"
    assert parent_record == {}

    bound_path, parent_record = process_parent_data_item(
        "dlt-hub/dlt/issues/{id}/comments", {"obj_id": 12345}, resolve_params, ["obj_id"]
    )
    assert parent_record == {"_issues_obj_id": 12345}

    bound_path, parent_record = process_parent_data_item(
        "dlt-hub/dlt/issues/{id}/comments",
        {"obj_id": 12345, "obj_node": "node_1"},
        resolve_params,
        ["obj_id", "obj_node"],
    )
    assert parent_record == {"_issues_obj_id": 12345, "_issues_obj_node": "node_1"}

    # test nested data
    resolve_param_nested = [
        ResolvedParam(
            "id", {"field": "some_results.obj_id", "resource": "issues", "type": "resolve"}
        )
    ]
    item = {"some_results": {"obj_id": 12345}}
    bound_path, parent_record = process_parent_data_item(
        "dlt-hub/dlt/issues/{id}/comments", item, resolve_param_nested, None
    )
    assert bound_path == "dlt-hub/dlt/issues/12345/comments"

    # param path not found
    with pytest.raises(ValueError) as val_ex:
        bound_path, parent_record = process_parent_data_item(
            "dlt-hub/dlt/issues/{id}/comments", {"_id": 12345}, resolve_params, None
        )
    assert "Transformer expects a field 'obj_id'" in str(val_ex.value)

    # included path not found
    with pytest.raises(ValueError) as val_ex:
        bound_path, parent_record = process_parent_data_item(
            "dlt-hub/dlt/issues/{id}/comments",
            {"obj_id": 12345, "obj_node": "node_1"},
            resolve_params,
            ["obj_id", "node"],
        )
    assert "in order to include it in child records under _issues_node" in str(val_ex.value)

    # Resolve multiple parameters from a single record
    multi_resolve_params = [
        ResolvedParam("issue_id", {"field": "issue", "resource": "comments", "type": "resolve"}),
        ResolvedParam("id", {"field": "id", "resource": "comments", "type": "resolve"}),
    ]

    bound_path, parent_record = process_parent_data_item(
        "dlt-hub/dlt/issues/{issue_id}/comments/{id}",
        {"issue": 12345, "id": 56789},
        multi_resolve_params,
        None,
    )
    assert bound_path == "dlt-hub/dlt/issues/12345/comments/56789"
    assert parent_record == {}

    # param path not found with multiple parameters
    with pytest.raises(ValueError) as val_ex:
        bound_path, parent_record = process_parent_data_item(
            "dlt-hub/dlt/issues/{issue_id}/comments/{id}",
            {"_issue": 12345, "id": 56789},
            multi_resolve_params,
            None,
        )
    assert "Transformer expects a field 'issue'" in str(val_ex.value)


def test_two_resources_can_depend_on_one_parent_resource() -> None:
    user_id = {
        "user_id": {
            "type": "resolve",
            "field": "id",
            "resource": "users",
        }
    }
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            "users",
            {
                "name": "user_details",
                "endpoint": {
                    "path": "user/{user_id}/",
                    "params": user_id,  # type: ignore[typeddict-item]
                },
            },
            {
                "name": "meetings",
                "endpoint": {
                    "path": "meetings/{user_id}/",
                    "params": user_id,  # type: ignore[typeddict-item]
                },
            },
        ],
    }
    resources = rest_api_source(config).resources
    assert resources["meetings"]._pipe.parent.name == "users"
    assert resources["user_details"]._pipe.parent.name == "users"


def test_dependent_resource_can_bind_multiple_parameters() -> None:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            "users",
            {
                "name": "user_details",
                "endpoint": {
                    "path": "user/{user_id}/{group_id}",
                    "params": {
                        "user_id": {
                            "type": "resolve",
                            "field": "id",
                            "resource": "users",
                        },
                        "group_id": {
                            "type": "resolve",
                            "field": "group",
                            "resource": "users",
                        },
                    },
                },
            },
        ],
    }

    resources = rest_api_source(config).resources
    assert resources["user_details"]._pipe.parent.name == "users"


def test_one_resource_cannot_bind_two_parents() -> None:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            "users",
            "groups",
            {
                "name": "user_details",
                "endpoint": {
                    "path": "user/{user_id}/{group_id}",
                    "params": {
                        "user_id": {
                            "type": "resolve",
                            "field": "id",
                            "resource": "users",
                        },
                        "group_id": {
                            "type": "resolve",
                            "field": "id",
                            "resource": "groups",
                        },
                    },
                },
            },
        ],
    }

    with pytest.raises(ValueError) as e:
        rest_api_resources(config)

    error_part_1 = re.escape(
        "Multiple parent resources for user_details: [ResolvedParam(param_name='user_id'"
    )
    error_part_2 = re.escape("ResolvedParam(param_name='group_id'")
    assert e.match(error_part_1)
    assert e.match(error_part_2)


def test_resource_dependent_dependent() -> None:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            "locations",
            {
                "name": "location_details",
                "endpoint": {
                    "path": "location/{location_id}",
                    "params": {
                        "location_id": {
                            "type": "resolve",
                            "field": "id",
                            "resource": "locations",
                        },
                    },
                },
            },
            {
                "name": "meetings",
                "endpoint": {
                    "path": "/meetings/{room_id}",
                    "params": {
                        "room_id": {
                            "type": "resolve",
                            "field": "room_id",
                            "resource": "location_details",
                        },
                    },
                },
            },
        ],
    }

    resources = rest_api_source(config).resources
    assert resources["meetings"]._pipe.parent.name == "location_details"
    assert resources["location_details"]._pipe.parent.name == "locations"


def test_circular_resource_bindingis_invalid() -> None:
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            {
                "name": "chicken",
                "endpoint": {
                    "path": "chicken/{egg_id}/",
                    "params": {
                        "egg_id": {
                            "type": "resolve",
                            "field": "id",
                            "resource": "egg",
                        },
                    },
                },
            },
            {
                "name": "egg",
                "endpoint": {
                    "path": "egg/{chicken_id}/",
                    "params": {
                        "chicken_id": {
                            "type": "resolve",
                            "field": "id",
                            "resource": "chicken",
                        },
                    },
                },
            },
        ],
    }

    with pytest.raises(CycleError) as e:
        rest_api_resources(config)
    assert e.match(re.escape("'nodes are in a cycle', ['chicken', 'egg', 'chicken']"))
