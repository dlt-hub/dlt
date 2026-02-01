import sys
from base64 import b64decode
from typing import Any

import pytest
import pyarrow

from dlt.common.libs.utils import is_instance_lib


def decode_b64b64(value: str) -> str:
    try:
        return b64decode(b64decode(value)).decode("utf-8")
    except Exception as e:
        raise ValueError("Invalid double-base64 encoded secret") from e


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
