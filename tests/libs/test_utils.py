import sys
from typing import Any

import pytest
import pyarrow

from dlt.common.libs.utils import is_instance_lib


# TODO to thoroughly test `is_instance_lib()`, we need to reset
# the imports in `sys.modules` before running each check.
@pytest.mark.parametrize(
    ("obj", "class_ref", "expected_result"),
    (
        ("a_string", "str", False),  # built-in
        (object(), "pyarrow.Array", False),
        (pyarrow.array([0, 1]), "pyarrow.Array", True),
    ),
)
def test_is_instance_lib(obj: Any, class_ref: str, expected_result: bool):
    result = is_instance_lib(obj, class_ref=class_ref)

    assert result == expected_result
