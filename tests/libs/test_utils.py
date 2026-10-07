from dlt.common.libs import is_instance_lib


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


def test_is_instance_lib_imports_submodules() -> None:
    """Submodules of an imported package are imported on demand — packages do not
    necessarily re-export them (e.g. newest starlette drops `starlette.applications`)."""
    import sys
    import xml  # noqa: F401

    sys.modules.pop("xml.sax", None)
    assert is_instance_lib(object(), class_ref="xml.sax.xmlreader.XMLReader") is False

    import xml.sax.xmlreader

    reader = xml.sax.xmlreader.XMLReader()
    assert is_instance_lib(reader, class_ref="xml.sax.xmlreader.XMLReader") is True
    # missing submodule of an imported package
    assert is_instance_lib(object(), class_ref="xml.nonexistent.Klass") is False
