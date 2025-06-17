# test utils :)
import pytest

from tests.pipeline.utils import assert_records_as_set


def test_assert_records_as_set():
    assert_records_as_set([{"a": 1}, {"a": 2}], [{"a": 2}, {"a": 1}])
    assert_records_as_set([{"a": 1}, {"a": 1}], [{"a": 1}, {"a": 1}])

    # test that a different number of the same recoreds actually fails
    with pytest.raises(AssertionError):
        assert_records_as_set([{"a": 1}, {"a": 1}], [{"a": 1}])
