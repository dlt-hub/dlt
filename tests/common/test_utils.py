import pytest

from dlt.common.utils import flatten_list_of_str_or_dicts, digest128


def test_flatten_list_of_str_or_dicts() -> None:
    l_d = [{"a": "b"}, "c", 1, [2]]
    d_d = flatten_list_of_str_or_dicts(l_d)
    assert d_d == {"a": "b", "c": None, "1": None, "[2]": None}
    # key clash
    l_d = [{"a": "b"}, "a"]
    with pytest.raises(KeyError):
        d_d = flatten_list_of_str_or_dicts(l_d)


def test_digest128_length() -> None:
    assert len(digest128("hash it")) == 120/6
