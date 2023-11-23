import re
from typing import Dict, Any
from copy import deepcopy
from unittest import mock

from dlt.common import pipeline as ps


def test_delete_source_state_keys() -> None:
    _fake_source_state = {
        "a": {"b": {"c": 1}},
        "x": {"y": {"c": 2}},
        "y": {"x": {"a": 3}},
        "resources": {"some_data": {"incremental": {"last_value": 123}}},
    }

    state = deepcopy(_fake_source_state)
    # Delete single json path with source_state context
    with mock.patch.object(ps, "source_state", autospec=True, return_value=state):
        ps._delete_source_state_keys("a.b.c")

    expected: Dict[str, Any] = deepcopy(_fake_source_state)
    del expected["a"]["b"]["c"]
    assert state == expected

    state = deepcopy(_fake_source_state)
    # Delete multiple paths with source state context
    with mock.patch.object(ps, "source_state", autospec=True, return_value=state):
        ps._delete_source_state_keys(["a.b.c", "x.y.c"])

    del expected["x"]["y"]["c"]
    assert state == expected

    state = deepcopy(_fake_source_state)
    # Delete paths from passed in state
    ps._delete_source_state_keys(["a.b.c", "x.y.c"], state)
    assert state == expected

    state = deepcopy(_fake_source_state)
    # Path not found is ignored
    ps._delete_source_state_keys(["a.b.c", "x.y.c", "foo.bar"], state)
    assert state == expected


def test_get_matching_resources() -> None:
    _fake_source_state = {
        "resources": {
            "events_a": {"a": 1, "b": 2},
            "events_b": {"m": [1, 2, 3]},
            "reactions": {"f": 44, "g": [4242]},
        }
    }
    pattern = re.compile("^events_[a-z]$")

    # with state argument
    results = ps._get_matching_resources(pattern, _fake_source_state)
    assert sorted(results) == ["events_a", "events_b"]

    # with state context
    with mock.patch.object(ps, "source_state", autospec=True, return_value=_fake_source_state):
        results = ps._get_matching_resources(pattern, _fake_source_state)
        assert sorted(results) == ["events_a", "events_b"]

    # no resources key
    results = ps._get_matching_resources(pattern, {})
    assert results == []
