import pytest

from dlt.common import jsonpath as jp


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


# TODO: Test all jsonpath utils
