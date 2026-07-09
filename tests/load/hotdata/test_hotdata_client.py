from __future__ import annotations

import inspect
import os
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from dlt.common.configuration.exceptions import ConfigFieldMissingException
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.destination.client import JobClientBase
from dlt.common.libs.pyarrow import get_py_arrow_numeric, pyarrow as pa

from dlt.destinations.impl.hotdata.configuration import (
    HotdataClientConfiguration,
    HotdataCredentials,
)
from dlt.destinations.impl.hotdata.contracts import TableContract, normalize_identifier
from dlt.destinations.impl.hotdata.errors import (
    HotdataTerminalError,
    HotdataTransientError,
    classify_sdk_error,
)
from dlt.destinations.impl.hotdata.hotdata import HotdataClient
from dlt.destinations.impl.hotdata.merge import (
    combine_tables,
    merge_rows,
    resolve_primary_key,
    resolve_write_disposition,
    row_key,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------


def test_hotdata_credentials_fields() -> None:
    cred = HotdataCredentials()
    cred.api_key = "key_abc"
    cred.workspace_id = "ws_xyz"
    assert str(cred) == "hotdata://ws_xyz"
    assert cred.is_partial() is False


def test_hotdata_credentials_optional_fields() -> None:
    """Both credential fields are optional — resolving with no env vars succeeds with None values."""
    cred = resolve_configuration(HotdataCredentials())
    assert cred.api_key is None
    assert cred.workspace_id is None


def test_hotdata_credentials_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CREDENTIALS__API_KEY", "env_key")
    monkeypatch.setenv("CREDENTIALS__WORKSPACE_ID", "env_ws")
    cred = resolve_configuration(HotdataCredentials())
    assert cred.api_key == "env_key"
    assert cred.workspace_id == "env_ws"


def test_hotdata_configuration_defaults() -> None:
    config = HotdataClientConfiguration()
    assert config.api_base_url == "https://api.hotdata.dev"
    assert config.database_name == "dlt"
    assert config.schema == "public"
    assert config.write_disposition == "append"
    assert config.create_database_if_missing is True
    assert config.max_retries == 5
    assert config.retry_backoff_seconds == 1.0
    assert config.declared_tables is None


def test_hotdata_capabilities_defaults() -> None:
    from dlt.destinations import hotdata as hotdata_factory

    caps = hotdata_factory()._raw_capabilities()
    assert caps.preferred_loader_file_format == "parquet"
    assert caps.loader_parallelism_strategy == "table-sequential"
    assert caps.max_table_nesting == 1000
    assert "insert-only" in caps.supported_merge_strategies
    assert "upsert" in caps.supported_merge_strategies
    assert "truncate-and-insert" in caps.supported_replace_strategies
    # numeric precision must be set or parquet normalization of decimal/wei columns crashes
    assert caps.decimal_precision == (38, 9)
    assert caps.wei_precision == (78, 0)


def test_hotdata_capabilities_numeric_maps_to_arrow() -> None:
    """The numeric precision caps must map to arrow decimal types.

    A bare `DestinationCapabilitiesContext` leaves `decimal_precision`/`wei_precision` as `None`,
    which crashed parquet normalization of decimal/wei columns with `'NoneType' object is not
    subscriptable`.
    """
    from dlt.destinations import hotdata as hotdata_factory

    caps = hotdata_factory()._raw_capabilities()
    assert pa.types.is_decimal(get_py_arrow_numeric(caps.decimal_precision))
    assert pa.types.is_decimal(get_py_arrow_numeric(caps.wei_precision))


def test_hotdata_capabilities_config_override() -> None:
    from dlt.destinations import hotdata as hotdata_factory

    dest = hotdata_factory(max_table_nesting=2, loader_parallelism_strategy="row-parallel")
    caps = dest.capabilities()
    assert caps.max_table_nesting == 2
    assert caps.loader_parallelism_strategy == "row-parallel"


def test_hotdata_update_stored_schema_signature() -> None:
    """The load path calls `update_stored_schema(..., force=...)` for replace/refresh loads, so
    the override must accept every parameter the base client declares."""
    base = set(inspect.signature(JobClientBase.update_stored_schema).parameters)
    override = set(inspect.signature(HotdataClient.update_stored_schema).parameters)
    assert "force" in override
    assert base <= override


def test_hotdata_configuration_str() -> None:
    config = HotdataClientConfiguration()
    config.credentials = HotdataCredentials()
    config.credentials.api_key = "k"
    config.credentials.workspace_id = "w"
    assert str(config) == "hotdata://w"


# ---------------------------------------------------------------------------
# error classification
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "error,expected_type",
    [
        (TimeoutError("timed out"), HotdataTransientError),
        (ConnectionError("connection refused"), HotdataTransientError),
        (ValueError("something else"), HotdataTerminalError),
        (RuntimeError("unrelated"), HotdataTerminalError),
    ],
    ids=["timeout", "connection-error", "value-error", "runtime-error"],
)
def test_classify_sdk_error(error: Exception, expected_type: type) -> None:
    result = classify_sdk_error(error)
    assert isinstance(result, expected_type)


def test_classify_sdk_error_api_exception_4xx() -> None:
    """4xx API errors (except retryable codes) are terminal."""
    pytest.importorskip("hotdata")
    from hotdata.rest import ApiException

    err = ApiException(status=400, reason="Bad Request")
    result = classify_sdk_error(err)
    assert isinstance(result, HotdataTerminalError)


def test_classify_sdk_error_api_exception_5xx() -> None:
    """5xx API errors are transient."""
    pytest.importorskip("hotdata")
    from hotdata.rest import ApiException

    err = ApiException(status=503, reason="Service Unavailable")
    result = classify_sdk_error(err)
    assert isinstance(result, HotdataTransientError)


@pytest.mark.parametrize("status", [408, 409, 425, 429], ids=["408", "409", "425", "429"])
def test_classify_sdk_error_retryable_4xx(status: int) -> None:
    """Specific 4xx status codes are transient (rate-limit, conflict, etc.)."""
    pytest.importorskip("hotdata")
    from hotdata.rest import ApiException

    result = classify_sdk_error(ApiException(status=status, reason="retryable"))
    assert isinstance(result, HotdataTransientError)


# ---------------------------------------------------------------------------
# merge logic
# ---------------------------------------------------------------------------


def _make_table(rows: List[Dict[str, Any]]) -> pa.Table:
    return pa.Table.from_pylist(rows)


def test_merge_rows_upsert() -> None:
    existing = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]
    incoming = [{"id": 1, "v": "updated"}, {"id": 3, "v": "new"}]
    result = merge_rows(existing, incoming, primary_key=["id"])
    assert len(result) == 3
    by_id = {r["id"]: r for r in result}
    assert by_id[1]["v"] == "updated"
    assert by_id[2]["v"] == "b"
    assert by_id[3]["v"] == "new"


def test_merge_rows_missing_pk_raises() -> None:
    with pytest.raises(ValueError, match="Primary key field"):
        merge_rows([{"id": 1}], [{"id": None}], primary_key=["id"])


def test_row_key_stability() -> None:
    assert row_key({"id": 1, "x": "y"}, ["id"]) == row_key({"id": 1, "x": "z"}, ["id"])
    assert row_key({"id": 1}, ["id"]) != row_key({"id": 2}, ["id"])


@pytest.mark.parametrize(
    "disposition,has_existing,expected_len",
    [
        ("replace", True, 1),
        ("append", True, 3),
        ("merge", True, 2),
        ("replace", False, 1),
        ("append", False, 1),
        ("merge", False, 1),
    ],
    ids=[
        "replace-with-existing",
        "append-with-existing",
        "merge-with-existing",
        "replace-no-existing",
        "append-no-existing",
        "merge-no-existing",
    ],
)
def test_combine_tables(disposition: str, has_existing: bool, expected_len: int) -> None:
    existing = _make_table([{"id": 1, "v": "old"}, {"id": 2, "v": "old"}]) if has_existing else None
    incoming = _make_table([{"id": 2, "v": "new"}])
    result = combine_tables(
        disposition=disposition,
        existing=existing,
        incoming=incoming,
        primary_key=["id"],
    )
    assert len(result) == expected_len


def test_combine_tables_insert_only_skips_existing() -> None:
    existing = _make_table([{"_dlt_id": "a", "v": "old"}, {"_dlt_id": "b", "v": "old"}])
    incoming = _make_table([{"_dlt_id": "b", "v": "new"}, {"_dlt_id": "c", "v": "new"}])
    result = combine_tables(
        disposition="insert-only", existing=existing, incoming=incoming, primary_key=None
    )
    by_id = {r["_dlt_id"]: r for r in result.to_pylist()}
    assert by_id["a"]["v"] == "old"
    assert by_id["b"]["v"] == "old"  # not updated — insert-only
    assert by_id["c"]["v"] == "new"
    assert len(result) == 3


def test_combine_tables_insert_only_no_existing() -> None:
    incoming = _make_table([{"_dlt_id": "a"}, {"_dlt_id": "b"}])
    result = combine_tables(
        disposition="insert-only", existing=None, incoming=incoming, primary_key=None
    )
    assert len(result) == 2


def test_combine_tables_insert_only_all_duplicate() -> None:
    existing = _make_table([{"_dlt_id": "a"}])
    incoming = _make_table([{"_dlt_id": "a"}])
    result = combine_tables(
        disposition="insert-only", existing=existing, incoming=incoming, primary_key=None
    )
    assert len(result) == 1


def test_combine_tables_append_schema_drift() -> None:
    existing = _make_table([{"id": 1}])
    incoming = _make_table([{"id": 2, "extra_col": "x"}])
    result = combine_tables(
        disposition="append", existing=existing, incoming=incoming, primary_key=None
    )
    assert len(result) == 2
    assert "extra_col" in result.schema.names


def test_combine_tables_unsupported_disposition() -> None:
    with pytest.raises(ValueError, match="Unsupported write_disposition"):
        combine_tables(
            disposition="overwrite",
            existing=_make_table([{"id": 1}]),
            incoming=_make_table([{"id": 2}]),
            primary_key=None,
        )


def test_resolve_write_disposition_fallback() -> None:
    assert resolve_write_disposition({}, "append") == "append"
    assert resolve_write_disposition({"write_disposition": "replace"}, "append") == "replace"
    assert resolve_write_disposition({"write_disposition": "MERGE"}, "append") == "merge"


def test_resolve_primary_key() -> None:
    assert resolve_primary_key({}) is None
    assert resolve_primary_key({"primary_key": ["id"]}) == ["id"]
    assert resolve_primary_key({"primary_key": ["id", "ts"]}) == ["id", "ts"]


# ---------------------------------------------------------------------------
# contracts / identifier normalization
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("my_table", "my_table"),
        ("My Table", "my_table"),
        ("table-name", "table_name"),
        ("123table", "123table"),
        ("__leading", "leading"),
        ("", ""),
    ],
    ids=["snake", "spaces", "hyphens", "leading-digits", "leading-underscores", "empty"],
)
def test_normalize_identifier(raw: str, expected: str) -> None:
    assert normalize_identifier(raw) == expected


def test_table_contract_from_schema() -> None:
    schema = {"name": "My Orders"}
    contract = TableContract.from_table_schema(schema, database_name="MyDB", schema="Public")
    assert contract.table_name == "my_orders"
    assert contract.database_name == "mydb"
    assert contract.schema == "public"
    assert contract.qualified_target == "mydb.public.my_orders"


def test_table_contract_nested_table() -> None:
    """dlt schema names for nested tables already contain the full path — no parent re-prefixing."""
    schema = {"name": "orders__items", "parent": "orders"}
    contract = TableContract.from_table_schema(schema, database_name="db", schema="s")
    assert contract.table_name == "orders__items"


def test_declared_table_names_deduplicates() -> None:
    names = TableContract.declared_table_names(
        database_name="db",
        schema="public",
        table_names=["orders", "orders", "items"],
    )
    assert sorted(names) == ["items", "orders"]


# ---------------------------------------------------------------------------
# hotdata.py table classification
# ---------------------------------------------------------------------------


def test_nested_table_name_normalized() -> None:
    """TableContract normalizes nested table names to parent__child, not parent▶child."""
    schema = {"name": "orders▶items", "parent": "orders"}
    contract = TableContract.from_table_schema(schema, database_name="db", schema="s")
    assert contract.table_name == "orders_items"
    assert "▶" not in contract.table_name


def test_is_internal_table() -> None:
    from dlt.destinations.impl.hotdata.hotdata import _is_internal_table

    assert _is_internal_table("_dlt_loads") is True
    assert _is_internal_table("_dlt_version") is True
    assert _is_internal_table("_dlt_pipeline_state") is True
    assert _is_internal_table("_dlt_custom") is True
    assert _is_internal_table("orders") is False
    assert _is_internal_table("users") is False
