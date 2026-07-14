from pathlib import Path
from types import SimpleNamespace
from typing import Optional, Type

import pytest
from lance.namespace import DirectoryNamespace
from lance_namespace import NamespaceExistsRequest, TableExistsRequest
from lance_namespace.errors import (
    NamespaceNotFoundError,
    PermissionDeniedError,
    TableNotFoundError,
)

from dlt.common.destination.exceptions import (
    DestinationTransientException,
    DestinationUndefinedEntity,
)
from dlt.destinations.impl.lance.exceptions import (
    LanceManifestMisconfiguration,
    is_lance_undefined_entity_exception,
    raise_destination_error,
)


pytestmark = pytest.mark.essential

MANIFEST_MODE_MSG = "Child namespaces are only supported when manifest mode is enabled"


def test_lance_exists_methods_raise(tmp_path: Path) -> None:
    """Asserts namespace_exists() / table_exists() raise NotFound errors if namespace / table does not exist.

    This is a known bug: https://github.com/lance-format/lance/issues/6240.

    When this test starts failing, the bug has likely been fixed upstream and we should remove our
    workaround in is_lance_undefined_entity_exception() (or get rid of the function entirely).
    """
    ns = DirectoryNamespace(root=str(tmp_path))

    # missing namespace
    with pytest.raises(NamespaceNotFoundError) as exc_info:
        ns.namespace_exists(NamespaceExistsRequest(id=["nonexistent_namespace"]))
    assert is_lance_undefined_entity_exception(exc_info.value)

    # missing table: single-level table id
    with pytest.raises(TableNotFoundError) as table_exc_info:
        ns.table_exists(TableExistsRequest(id=["nonexistent_table"]))
    assert is_lance_undefined_entity_exception(table_exc_info.value)

    # missing table: multi-level table id
    with pytest.raises(TableNotFoundError) as table_exc_info:
        ns.table_exists(TableExistsRequest(id=["nonexistent_namespace", "nonexistent_table"]))
    assert is_lance_undefined_entity_exception(table_exc_info.value)


@pytest.mark.parametrize(
    ("exc", "manifest_enabled", "expected"),
    [
        pytest.param(
            NamespaceNotFoundError("Namespace not found: x"), None, True, id="typed-namespace"
        ),
        pytest.param(TableNotFoundError("Table not found: x"), None, True, id="typed-table"),
        pytest.param(PermissionDeniedError("denied"), None, False, id="typed-not-a-not-found"),
        pytest.param(
            RuntimeError("Namespace error: Table does not exist: x"), None, True, id="untyped"
        ),
        pytest.param(
            ValueError("Not found: t.lance/tree/b/_versions"), None, True, id="missing-branch"
        ),
        pytest.param(
            OSError("Dataset at path t.lance/_versions/9.manifest was not found"),
            None,
            True,
            id="missing-version",
        ),
        pytest.param(RuntimeError("boom"), None, False, id="unrelated"),
        # configured manifest mode reported as disabled means broken storage, not a missing entity
        pytest.param(
            NamespaceNotFoundError(MANIFEST_MODE_MSG), True, False, id="contradiction-typed"
        ),
        pytest.param(RuntimeError(MANIFEST_MODE_MSG), True, False, id="contradiction-untyped"),
        pytest.param(RuntimeError(MANIFEST_MODE_MSG), False, True, id="manifest-off"),
        pytest.param(RuntimeError(MANIFEST_MODE_MSG), None, True, id="manifest-unknown"),
    ],
)
def test_is_lance_undefined_entity_exception(
    exc: Exception, manifest_enabled: Optional[bool], expected: bool
) -> None:
    assert is_lance_undefined_entity_exception(exc, manifest_enabled) is expected


@pytest.mark.parametrize(
    ("manifest_enabled", "raised", "expected"),
    [
        pytest.param(
            True,
            RuntimeError(MANIFEST_MODE_MSG),
            LanceManifestMisconfiguration,
            id="misconfiguration",
        ),
        pytest.param(
            False,
            RuntimeError(MANIFEST_MODE_MSG),
            DestinationUndefinedEntity,
            id="undefined-entity",
        ),
        pytest.param(True, RuntimeError("boom"), DestinationTransientException, id="transient"),
    ],
)
def test_raise_destination_error_classification(
    manifest_enabled: bool, raised: Exception, expected: Type[Exception]
) -> None:
    class _Client:
        config = SimpleNamespace(manifest_enabled=manifest_enabled)

        @raise_destination_error
        def probe(self) -> None:
            raise raised

    with pytest.raises(expected):
        _Client().probe()
