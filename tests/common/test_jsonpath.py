from typing import Any

import pytest

from dlt.common import jsonpath as jp
from dlt.common.jsonpath import set_value_at_path


@pytest.mark.parametrize("compiled", [True, False])
@pytest.mark.parametrize(
    "path, expected",
    [
        ("col_a", "col_a"),
        ("'col.a'", "col.a"),
        ("'$col_a'", "$col_a"),
        ("'col|a'", "col|a"),
    ],
)
def test_extract_simple_field_name_positive(path, expected, compiled):
    if compiled:
        path = jp.compile_path(path)

    result = jp.extract_simple_field_name(path)
    assert result == expected


@pytest.mark.parametrize("compiled", [True, False])
@pytest.mark.parametrize(
    "path",
    [
        "$.col_a",
        "$.col_a.items",
        "$.col_a.items[0]",
        "$.col_a.items[*]",
        "col_a|col_b",
    ],
)
def test_extract_simple_field_name_negative(path, compiled):
    if compiled:
        path = jp.compile_path(path)

    result = jp.extract_simple_field_name(path)
    assert result is None


def test_set_value_at_path():
    # Test setting value in empty dict
    test_obj: dict[str, Any] = {}
    jp.set_value_at_path(test_obj, "key", "value")
    assert test_obj == {"key": "value"}

    # Test setting value in nested path
    test_obj = {}
    jp.set_value_at_path(test_obj, "parent.child", "value")
    assert test_obj == {"parent": {"child": "value"}}

    # Test setting value in deeply nested path
    test_obj = {}
    jp.set_value_at_path(test_obj, "level1.level2.level3", "value")
    assert test_obj == {"level1": {"level2": {"level3": "value"}}}

    # Test setting value with existing structure
    test_obj = {"existing": "data", "parent": {"existing_child": "data"}}
    jp.set_value_at_path(test_obj, "parent.new_child", "value")
    assert test_obj == {
        "existing": "data",
        "parent": {"existing_child": "data", "new_child": "value"},
    }

    # Test overwriting existing value
    test_obj = {"key": "old_value"}
    jp.set_value_at_path(test_obj, "key", "new_value")
    assert test_obj == {"key": "new_value"}


@pytest.mark.parametrize(
    "data,selector,matched_paths,matched_data",
    [
        pytest.param(
            {"data_from_d": {"foo1": {"bar": 1}, "foo2": {"bar": 2}}},
            "data_from_d.*.bar",
            ["data_from_d.foo1.bar", "data_from_d.foo2.bar"],
            [[1], [2]],
            id="star-selector-mid-path",
        ),
        pytest.param(
            {"data_from_d": {"foo1": {"bar": 1}, "foo2": {"bar": 2}}},
            "@.data_from_d",
            ["data_from_d"],
            [[{"foo1": {"bar": 1}, "foo2": {"bar": 2}}]],
            id="current-datum-selector",
        ),
        pytest.param(
            {"a": {"items": [{"b": 2}, {"b": 3}]}},
            "$.a.items[*].b",
            ["a.items.[0].b", "a.items.[1].b"],
            [[2], [3]],
            id="array-wildcard-selector",
        ),
        pytest.param(
            {"a.b": {"c": 1}},
            "$['a.b'].c",
            ["'a.b'.c"],
            [[1]],
            id="quoted-field-preserves-dot",
        ),
        pytest.param(
            {"@odata.nextLink": "https://example.com/next"},
            "$['@odata.nextLink']",
            ["'@odata.nextLink'"],
            [["https://example.com/next"]],
            id="quoted-odata-field",
        ),
    ],
)
def test_resolve_paths_roundtrips_to_matching_paths(
    data: dict[str, Any], selector: str, matched_paths: list[str], matched_data: list[Any]
) -> None:
    resolved_paths = jp.resolve_paths(selector, data)
    assert resolved_paths == matched_paths
    assert [jp.find_values(path, data) for path in resolved_paths] == matched_data
