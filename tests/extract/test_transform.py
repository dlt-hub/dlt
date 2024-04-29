import pytest

import dlt
from dlt.sources.helpers.transform import pivot
from dlt.extract.exceptions import ResourceExtractionError


@dlt.resource
def pivot_test_wrong_resource():
    yield [{"a": 1}]


@dlt.resource
def pivot_test_wrong_resource2():
    yield [{"a": [1]}]


def test_transform_pivot_wrong_data() -> None:
    res = pivot_test_wrong_resource()
    res.add_map(pivot("$.a"))

    with pytest.raises(ResourceExtractionError):
        list(res)

    res2 = pivot_test_wrong_resource2()
    res2.add_map(pivot("$.a"))

    with pytest.raises(ResourceExtractionError):
        list(res)


@dlt.resource
def pivot_test_resource():
    for row in (
        [
            {
                "a": {
                    "inner_1": [[1, 1, 1], [2, 2, 2]],
                    "inner_2": [[3, 3, 3], [4, 4, 4]],
                    "inner_3": [[5, 5, 5], [6, 6, 6]],
                },
                "b": [7, 7, 7],
            },
            {
                "a": {
                    "inner_1": [[8, 8, 8], [9, 9, 9]],
                    "inner_2": [[10, 10, 10], [11, 11, 11]],
                    "inner_3": [[12, 12, 12], [13, 13, 13]],
                },
                "b": [[14, 14, 14], [15, 15, 15]],
            },
        ],
    ):
        yield row


def test_transform_pivot_single_path() -> None:
    res = pivot_test_resource()
    res.add_map(pivot("$.a.inner_1|inner_2"))

    result = list(res)
    assert result == [
        {
            "a": {
                "inner_1": [{"col0": 1, "col1": 1, "col2": 1}, {"col0": 2, "col1": 2, "col2": 2}],
                "inner_2": [{"col0": 3, "col1": 3, "col2": 3}, {"col0": 4, "col1": 4, "col2": 4}],
                "inner_3": [[5, 5, 5], [6, 6, 6]],
            },
            "b": [7, 7, 7],
        },
        {
            "a": {
                "inner_1": [{"col0": 8, "col1": 8, "col2": 8}, {"col0": 9, "col1": 9, "col2": 9}],
                "inner_2": [
                    {"col0": 10, "col1": 10, "col2": 10},
                    {"col0": 11, "col1": 11, "col2": 11},
                ],
                "inner_3": [[12, 12, 12], [13, 13, 13]],
            },
            "b": [[14, 14, 14], [15, 15, 15]],
        },
    ]


def test_transform_pivot_multiple_paths() -> None:
    res = pivot_test_resource()
    res.add_map(pivot(["$.a.inner_1", "$.a.inner_2"]))
    result = list(res)
    assert result == [
        {
            "a": {
                "inner_1": [{"col0": 1, "col1": 1, "col2": 1}, {"col0": 2, "col1": 2, "col2": 2}],
                "inner_2": [{"col0": 3, "col1": 3, "col2": 3}, {"col0": 4, "col1": 4, "col2": 4}],
                "inner_3": [[5, 5, 5], [6, 6, 6]],
            },
            "b": [7, 7, 7],
        },
        {
            "a": {
                "inner_1": [{"col0": 8, "col1": 8, "col2": 8}, {"col0": 9, "col1": 9, "col2": 9}],
                "inner_2": [
                    {"col0": 10, "col1": 10, "col2": 10},
                    {"col0": 11, "col1": 11, "col2": 11},
                ],
                "inner_3": [[12, 12, 12], [13, 13, 13]],
            },
            "b": [[14, 14, 14], [15, 15, 15]],
        },
    ]


def test_transform_pivot_no_root() -> None:
    res = pivot_test_resource()
    res.add_map(pivot("a.inner_1"))
    result = list(res)
    assert result == [
        {
            "a": {
                "inner_1": [{"col0": 1, "col1": 1, "col2": 1}, {"col0": 2, "col1": 2, "col2": 2}],
                "inner_2": [[3, 3, 3], [4, 4, 4]],
                "inner_3": [[5, 5, 5], [6, 6, 6]],
            },
            "b": [7, 7, 7],
        },
        {
            "a": {
                "inner_1": [{"col0": 8, "col1": 8, "col2": 8}, {"col0": 9, "col1": 9, "col2": 9}],
                "inner_2": [[10, 10, 10], [11, 11, 11]],
                "inner_3": [[12, 12, 12], [13, 13, 13]],
            },
            "b": [[14, 14, 14], [15, 15, 15]],
        },
    ]


def test_transform_pivot_prefix() -> None:
    res = pivot_test_resource()
    res.add_map(pivot("$.a.inner_1", "prefix_"))

    result = list(res)
    assert result == [
        {
            "a": {
                "inner_1": [
                    {"prefix_0": 1, "prefix_1": 1, "prefix_2": 1},
                    {"prefix_0": 2, "prefix_1": 2, "prefix_2": 2},
                ],
                "inner_2": [[3, 3, 3], [4, 4, 4]],
                "inner_3": [[5, 5, 5], [6, 6, 6]],
            },
            "b": [7, 7, 7],
        },
        {
            "a": {
                "inner_1": [
                    {"prefix_0": 8, "prefix_1": 8, "prefix_2": 8},
                    {"prefix_0": 9, "prefix_1": 9, "prefix_2": 9},
                ],
                "inner_2": [[10, 10, 10], [11, 11, 11]],
                "inner_3": [[12, 12, 12], [13, 13, 13]],
            },
            "b": [[14, 14, 14], [15, 15, 15]],
        },
    ]


@dlt.resource
def pivot_test_pandas_resource():
    import pandas

    df = pandas.DataFrame({"A": [1, 2], "B": [3, 4], "C": [5, 6]})

    yield df.values.tolist()


def test_pandas_dataframe_pivot() -> None:
    res = pivot_test_pandas_resource()
    res.add_map(pivot())

    result = list(res)
    assert result == [[{"col0": 1, "col1": 3, "col2": 5}], [{"col0": 2, "col1": 4, "col2": 6}]]
