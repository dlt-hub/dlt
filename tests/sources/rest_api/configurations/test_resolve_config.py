import re
from copy import deepcopy

import pytest
from graphlib import CycleError

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
    with pytest.raises(ValueError):
        _bind_path_params(tp_6)


def test_process_parent_data_item() -> None:
    resolved_params = [
        ResolvedParam("id", {"field": "obj_id", "resource": "issues", "type": "resolve"})
    ]

    processed_data = process_parent_data_item(
        path="dlt-hub/dlt/issues/{id}/comments",
        item={"obj_id": 12345},
        resolved_params=resolved_params,
        include_from_parent=None,
    )
    assert processed_data.path == "dlt-hub/dlt/issues/12345/comments"
    assert processed_data.params == {}  # defaults to empty dict
    assert processed_data.json is None  # defaults to None
    assert processed_data.headers is None  # defaults to None
    assert processed_data.parent_record == {}

    # same but with empty headers, params and json
    processed_data = process_parent_data_item(
        path="dlt-hub/dlt/issues/{id}/comments",
        item={"obj_id": 12345},
        params={},
        request_json={},
        headers={},
        resolved_params=resolved_params,
    )
    # those got propagated
    assert processed_data.params == {}
    assert processed_data.json == {}  # generates empty body!
    assert processed_data.headers == {}

    # also test params and json
    processed_data = process_parent_data_item(
        path="dlt-hub/dlt/issues/comments",
        item={"obj_id": 12345},
        params={"orig_id": "{id}"},
        request_json={"orig_id": "{id}"},
        headers={"X-Id": "{id}"},
        resolved_params=resolved_params,
    )
    assert processed_data.params == {"orig_id": "12345"}
    assert processed_data.json == {"orig_id": 12345}
    assert processed_data.headers == {"X-Id": "12345"}

    processed_data = process_parent_data_item(
        path="dlt-hub/dlt/issues/{id}/comments",
        item={"obj_id": 12345},
        resolved_params=resolved_params,
        include_from_parent=["obj_id"],
    )
    assert processed_data.parent_record == {"_issues_obj_id": 12345}

    processed_data = process_parent_data_item(
        path="dlt-hub/dlt/issues/{id}/comments",
        item={"obj_id": 12345, "obj_node": "node_1"},
        resolved_params=resolved_params,
        include_from_parent=["obj_id", "obj_node"],
    )
    assert processed_data.parent_record == {"_issues_obj_id": 12345, "_issues_obj_node": "node_1"}

    # Test resource field reference in path
    resolved_params_reference = [
        ResolvedParam(
            "resources.issues.obj_id",
            {"field": "obj_id", "resource": "issues", "type": "resolve"},
        )
    ]
    processed_data = process_parent_data_item(
        path="dlt-hub/dlt/issues/{resources.issues.obj_id}/comments",
        item={"obj_id": 12345, "obj_node": "node_1"},
        resolved_params=resolved_params_reference,
        include_from_parent=["obj_id", "obj_node"],
    )
    assert processed_data.path == "dlt-hub/dlt/issues/12345/comments"

    # Test resource field reference in params and headers
    processed_data = process_parent_data_item(
        path="dlt-hub/dlt/issues/comments",
        item={"obj_id": 12345, "obj_node": "node_1"},
        params={"id": "{resources.issues.obj_id}"},
        request_json={"id": "{resources.issues.obj_id}"},
        headers={"X-Id": "{resources.issues.obj_id}"},
        resolved_params=resolved_params_reference,
        include_from_parent=["obj_id", "obj_node"],
    )
    assert processed_data.path == "dlt-hub/dlt/issues/comments"
    assert processed_data.params == {"id": "12345"}
    assert processed_data.json == {"id": 12345}
    assert processed_data.headers == {"X-Id": "12345"}

    # Test nested data
    resolved_param_nested = [
        ResolvedParam(
            "id",
            {"field": "some_results.obj_id", "resource": "issues", "type": "resolve"},
        )
    ]
    item = {"some_results": {"obj_id": 12345}}
    processed_data = process_parent_data_item(
        path="dlt-hub/dlt/issues/{id}/comments",
        item=item,
        params={},
        headers={"X-Id": "{id}"},
        resolved_params=resolved_param_nested,
        include_from_parent=None,
    )
    assert processed_data.path == "dlt-hub/dlt/issues/12345/comments"
    assert processed_data.headers == {"X-Id": "12345"}

    # Test incremental values in headers
    from dlt.extract import Incremental

    incremental = Incremental(initial_value="2025-01-01", end_value="2025-01-02")
    processed_data = process_parent_data_item(
        path="dlt-hub/dlt/issues/comments",
        item={"obj_id": 12345},
        params={},
        headers={
            "X-Initial": "{incremental.initial_value}",
            "X-End": "{incremental.end_value}",
        },
        resolved_params=resolved_params,
        incremental=incremental,
    )
    assert processed_data.headers == {"X-Initial": "2025-01-01", "X-End": "2025-01-02"}

    # Param path not found
    with pytest.raises(ValueError) as val_ex:
        process_parent_data_item(
            path="dlt-hub/dlt/issues/{id}/comments",
            item={"_id": 12345},
            params={},
            resolved_params=resolved_params,
            include_from_parent=None,
        )
    assert "Resource expects a field `obj_id`" in str(val_ex.value)

    # Included path not found
    with pytest.raises(ValueError) as val_ex:
        process_parent_data_item(
            path="dlt-hub/dlt/issues/{id}/comments",
            item={"_id": 12345, "obj_node": "node_1"},
            params={},
            resolved_params=resolved_params,
            include_from_parent=["obj_id", "node"],
        )
    assert (
        "Resource expects a field `obj_id` to be present in the incoming data from resource"
        " `issues` in order to bind it to"
        in str(val_ex.value)
    )

    # Resolve multiple parameters from a single record
    multi_resolve_params = [
        ResolvedParam("issue_id", {"field": "issue", "resource": "comments", "type": "resolve"}),
        ResolvedParam("id", {"field": "id", "resource": "comments", "type": "resolve"}),
    ]

    processed_data = process_parent_data_item(
        path="dlt-hub/dlt/issues/{issue_id}/comments/{id}",
        item={"issue": 12345, "id": 56789},
        params={},
        headers={"X-Issue": "{issue_id}", "X-Id": "{id}"},
        resolved_params=multi_resolve_params,
        include_from_parent=None,
    )
    assert processed_data.path == "dlt-hub/dlt/issues/12345/comments/56789"
    assert processed_data.headers == {"X-Issue": "12345", "X-Id": "56789"}
    assert processed_data.parent_record == {}

    # Param path not found with multiple parameters
    with pytest.raises(ValueError) as val_ex:
        process_parent_data_item(
            path="dlt-hub/dlt/issues/{issue_id}/comments/{id}",
            item={"_issue": 12345, "id": 56789},
            params={},
            resolved_params=multi_resolve_params,
            include_from_parent=None,
        )
    assert "Resource expects a field `issue`" in str(val_ex.value)


def test_two_resources_can_depend_on_one_parent_resource() -> None:
    # Using resolve syntax
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

    # Using resource field reference syntax
    config_with_ref: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.example.com",
        },
        "resources": [
            "users",
            {
                "name": "user_details",
                "endpoint": {
                    "path": "user/{resources.users.id}/",
                },
            },
            {
                "name": "meetings",
                "endpoint": {
                    "path": "meetings/{resources.users.id}/",
                },
            },
        ],
    }
    resources = rest_api_source(config_with_ref).resources
    assert resources["meetings"]._pipe.parent.name == "users"
    assert resources["user_details"]._pipe.parent.name == "users"


@pytest.mark.parametrize(
    "config",
    [
        {
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
        },
        {
            "client": {
                "base_url": "https://api.example.com",
            },
            "resources": [
                "users",
                {
                    "name": "user_details",
                    "endpoint": {
                        "path": "user/{resources.users.id}/{resources.users.group}",
                    },
                },
            ],
        },
    ],
)
def test_dependent_resource_can_bind_multiple_parameters(config: RESTAPIConfig) -> None:
    resources = rest_api_source(config).resources
    assert resources["user_details"]._pipe.parent.name == "users"


@pytest.mark.parametrize(
    "config,resolved_param1,resolved_param2",
    [
        (
            {
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
            },
            "ResolvedParam(param_name='user_id'",
            "ResolvedParam(param_name='group_id'",
        ),
        (
            {
                "client": {
                    "base_url": "https://api.example.com",
                },
                "resources": [
                    "users",
                    "groups",
                    {
                        "name": "user_details",
                        "endpoint": {
                            "path": "user/{resources.users.id}/{resources.groups.id}",
                        },
                    },
                ],
            },
            "ResolvedParam(param_name='resources.users.id'",
            "ResolvedParam(param_name='resources.groups.id'",
        ),
    ],
)
def test_one_resource_cannot_bind_two_parents(
    config: RESTAPIConfig, resolved_param1: str, resolved_param2: str
) -> None:
    with pytest.raises(ValueError) as exc_info:
        rest_api_resources(config)

    error_msg = str(exc_info.value)
    assert "Multiple parent resources for resource `user_details`" in error_msg
    assert resolved_param1 in error_msg, f"{resolved_param1} not found in {error_msg}"
    assert resolved_param2 in error_msg, f"{resolved_param2} not found in {error_msg}"


@pytest.mark.parametrize(
    "config",
    [
        # Using resolve syntax
        {
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
        },
        # Using resource field reference syntax
        {
            "client": {
                "base_url": "https://api.example.com",
            },
            "resources": [
                "locations",
                {
                    "name": "location_details",
                    "endpoint": {
                        "path": "location/{resources.locations.id}",
                    },
                },
                {
                    "name": "meetings",
                    "endpoint": {
                        "path": "/meetings/{resources.location_details.room_id}",
                    },
                },
            ],
        },
        # Using shorter syntax with string endpoints
        {
            "client": {
                "base_url": "https://api.example.com",
            },
            "resources": [
                "locations",
                {
                    "name": "location_details",
                    "endpoint": "location/{resources.locations.id}",
                },
                {
                    "name": "meetings",
                    "endpoint": "/meetings/{resources.location_details.room_id}",
                },
            ],
        },
    ],
)
def test_resource_dependent_dependent(config: RESTAPIConfig) -> None:
    resources = rest_api_source(config).resources
    assert resources["meetings"]._pipe.parent.name == "location_details"
    assert resources["location_details"]._pipe.parent.name == "locations"


@pytest.mark.parametrize(
    "config",
    [
        # Using resolve syntax
        {
            "client": {"base_url": "https://api.example.com"},
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
        },
        # Using resource field reference syntax
        {
            "client": {"base_url": "https://api.example.com"},
            "resources": [
                {
                    "name": "chicken",
                    "endpoint": {
                        "path": "chicken/{resources.egg.id}/",
                    },
                },
                {
                    "name": "egg",
                    "endpoint": {
                        "path": "egg/{resources.chicken.id}/",
                    },
                },
            ],
        },
    ],
)
def test_circular_resource_bindingis_invalid(config: RESTAPIConfig) -> None:
    with pytest.raises(CycleError) as e:
        rest_api_resources(config)
    assert e.match(re.escape("'nodes are in a cycle', ['chicken', 'egg', 'chicken']"))
