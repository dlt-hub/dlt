from pathlib import Path

import pytest
from lance.namespace import DirectoryNamespace
from lance_namespace import NamespaceExistsRequest, TableExistsRequest

from dlt.destinations.impl.lance.exceptions import (
    LANCE_DOES_NOT_EXIST,
    LANCE_MANIFEST_MODE,
    LANCE_NOT_FOUND,
    is_lance_undefined_entity_exception,
)


pytestmark = pytest.mark.essential


def test_lance_exists_methods_raise_runtime_error(tmp_path: Path) -> None:
    """Asserts namespace_exists() / table_exists() raise RuntimeError with expected message if namespace / table does not exist.

    This is a known bug: https://github.com/lance-format/lance/issues/6240.

    When this test starts failing, the bug has likely been fixed upstream and we should remove our
    workaround in is_lance_undefined_entity_exception() (or get rid of the function entirely).
    """
    ns = DirectoryNamespace(root=str(tmp_path))

    # missing namespace
    with pytest.raises(RuntimeError, match=LANCE_NOT_FOUND) as exc_info:
        ns.namespace_exists(NamespaceExistsRequest(id=["nonexistent_namespace"]))
    assert is_lance_undefined_entity_exception(exc_info.value)

    # missing table: single-level table id
    with pytest.raises(RuntimeError, match=LANCE_DOES_NOT_EXIST) as exc_info:
        ns.table_exists(TableExistsRequest(id=["nonexistent_table"]))
    assert is_lance_undefined_entity_exception(exc_info.value)

    # missing table: multi-level table id
    with pytest.raises(RuntimeError, match=LANCE_MANIFEST_MODE) as exc_info:
        ns.table_exists(TableExistsRequest(id=["nonexistent_namespace", "nonexistent_table"]))
    assert is_lance_undefined_entity_exception(exc_info.value)
