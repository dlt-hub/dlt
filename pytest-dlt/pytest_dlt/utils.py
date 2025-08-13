from typing import Any, Dict, List
from collections import Counter

def assert_records_as_set(actual: List[Dict[str, Any]], expected: List[Dict[str, Any]]) -> None:
    """Assert that *actual* and *expected* contain the same records irrespective of order.

    Args:
        actual (List[Dict[str, Any]]): Records retrieved from the destination.
        expected (List[Dict[str, Any]]): Reference records defined in the test.

    Raises:
        AssertionError: When the two sets of records differ.
    """

    def dict_to_tuple(d):
        # Sort items to ensure consistent ordering
        return tuple(sorted(d.items()))

    counter1 = Counter(dict_to_tuple(d) for d in actual)
    counter2 = Counter(dict_to_tuple(d) for d in expected)

    assert counter1 == counter2