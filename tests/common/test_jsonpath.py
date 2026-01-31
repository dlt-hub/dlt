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


# TODO: Test all jsonpath utils
