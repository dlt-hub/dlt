from dlt.common.libs.utils import is_instance_lib


def test_is_instance_lib_builtin() -> None:
    """is_instance_lib returns False for built-in types."""
    assert is_instance_lib("a_string", class_ref="str") is False


def test_is_instance_lib_not_matching() -> None:
    """is_instance_lib returns False when object is not an instance of the class."""
    assert is_instance_lib(object(), class_ref="pyarrow.Array") is False


def test_is_instance_lib_pyarrow() -> None:
    """is_instance_lib returns True for a matching pyarrow type."""
    # lazy import — pyarrow is optional
    from dlt.common.libs.pyarrow import pyarrow

    arr = pyarrow.array([0, 1])
    assert is_instance_lib(arr, class_ref="pyarrow.Array") is True
