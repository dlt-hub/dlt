"""Tests for the mssql native Arrow bulk copy parquet load job.

These drive the job against a mocked mssql-python cursor and need no SQL Server. End-to-end
coverage lives in `tests/load/mssql/test_mssql_bulk_copy.py` and in the parametrized `tests/load`
suite, which runs mssql with `file_format="parquet"`.
"""

from typing import Any, Dict, List, Tuple, cast
from unittest.mock import MagicMock

import pytest

mssql_python = pytest.importorskip("mssql_python")

from dlt.common.configuration import resolve_configuration
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.libs.pyarrow import pyarrow
from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableSchema
from dlt.destinations import mssql
from dlt.destinations.impl.mssql.bulk_copy import (
    has_native_arrow_bulk_copy,
    make_native_parquet_file_format_selector,
)
from dlt.destinations.impl.mssql.configuration import MsSqlClientConfiguration, MsSqlCredentials
from dlt.destinations.impl.mssql.mssql import MsSqlBulkCopyArrowJob
from dlt.destinations.impl.mssql.sql_client import MsSqlClient
from dlt.destinations.exceptions import DatabaseTerminalException, DatabaseTransientException

DOCS_URL = "https://dlthub.com/docs/dlt-ecosystem/destinations/mssql#data-loading"


def _write_parquet(path: str, columns: List[str], num_row_groups: int, rows_per_group: int) -> None:
    schema = pyarrow.schema([(name, pyarrow.int64()) for name in columns])
    with pyarrow.parquet.ParquetWriter(path, schema) as writer:
        for group in range(num_row_groups):
            values = list(range(group * rows_per_group, (group + 1) * rows_per_group))
            writer.write_table(
                pyarrow.table({name: values for name in columns}, schema=schema),
                row_group_size=rows_per_group or None,
            )


def _make_job(
    file_path: str, rows_copied: int = 12, table_name: str = "items", **credential_kwargs: Any
) -> Tuple[MsSqlBulkCopyArrowJob, MagicMock]:
    """Builds a job wired to a mocked cursor, returning the job and that cursor."""
    credentials = MsSqlCredentials()
    credentials.host = "sql.example.com"
    credentials.database = "test_db"
    credentials.username = "loader"
    credentials.password = "pass"
    for key, value in credential_kwargs.items():
        setattr(credentials, key, value)

    config = MsSqlClientConfiguration(
        credentials=resolve_configuration(credentials)
    )._bind_dataset_name("my_dataset")
    job_client = mssql().client(Schema("schema"), config)

    cursor = MagicMock()
    cursor.bulkcopy_arrow = MagicMock(return_value={"rows_copied": rows_copied, "batch_count": 1})
    connection = MagicMock()
    connection.cursor = MagicMock(return_value=cursor)
    cast(MsSqlClient, job_client.sql_client)._conn = connection

    job = MsSqlBulkCopyArrowJob(file_path)
    job._job_client = job_client
    job._load_table = {"name": table_name}
    return job, cursor


def _bulkcopy_kwargs(cursor: MagicMock) -> Dict[str, Any]:
    return cursor.bulkcopy_arrow.call_args.kwargs


def test_bulk_copy_streams_a_reader_over_all_row_groups(tmp_path) -> None:
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id", "value"], num_row_groups=3, rows_per_group=4)

    job, cursor = _make_job(file_path)
    job.run()

    assert cursor.bulkcopy_arrow.call_count == 1
    source = cursor.bulkcopy_arrow.call_args.args[1]
    # a reader, not a materialized table, so the driver pulls row groups instead of dlt buffering
    assert isinstance(source, pyarrow.RecordBatchReader)
    assert source.read_all().num_rows == 12
    assert cursor.close.call_count == 1


def test_bulk_copy_reader_is_not_consumed_before_the_driver_pulls_it(tmp_path) -> None:
    """Nothing may be read from the parquet file until bulk copy asks for a batch."""
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id"], num_row_groups=4, rows_per_group=5)

    job, cursor = _make_job(file_path, rows_copied=20)
    job.run()

    source = cursor.bulkcopy_arrow.call_args.args[1]
    # the full stream is still pending after the call returned
    assert [batch.num_rows for batch in source] == [5, 5, 5, 5]


def test_bulk_copy_passes_qualified_table_and_explicit_column_mappings(tmp_path) -> None:
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id", "name", "created_at"], num_row_groups=1, rows_per_group=2)

    job, cursor = _make_job(file_path, rows_copied=2, table_name="items")
    job.run()

    assert cursor.bulkcopy_arrow.call_args.args[0] == '"my_dataset"."items"'
    # positional source index -> destination column name, in the load file's own order
    assert _bulkcopy_kwargs(cursor)["column_mappings"] == [
        (0, "id"),
        (1, "name"),
        (2, "created_at"),
    ]


def test_bulk_copy_sends_one_transactional_batch(tmp_path) -> None:
    """The whole file must be a single transactional batch, so a failure commits nothing."""
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id"], num_row_groups=3, rows_per_group=2)

    job, cursor = _make_job(file_path, rows_copied=6)
    job._job_client.config.bulk_copy_timeout = 900
    job.run()

    kwargs = _bulkcopy_kwargs(cursor)
    assert kwargs["batch_size"] == 0
    assert kwargs["use_internal_transaction"] is True
    # column defaults must never replace a NULL that dlt wrote
    assert kwargs["keep_nulls"] is True
    assert kwargs["timeout"] == 900


def test_bulk_copy_skips_an_empty_file(tmp_path) -> None:
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id"], num_row_groups=0, rows_per_group=0)

    job, cursor = _make_job(file_path)
    job.run()

    assert cursor.bulkcopy_arrow.call_count == 0


def test_bulk_copy_fails_when_fewer_rows_land_than_the_file_holds(tmp_path) -> None:
    """A silent driver-side row drop must not finish green with a short table."""
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id"], num_row_groups=2, rows_per_group=5)

    job, cursor = _make_job(file_path, rows_copied=7)

    with pytest.raises(DestinationTerminalException, match="7 rows copied but the file holds 10"):
        job.run()


@pytest.mark.parametrize(
    "credential_kwargs,expected",
    [
        ({"access_token": "a-token"}, "access_token"),
        ({"authentication": "ActiveDirectoryPassword"}, "ActiveDirectoryPassword"),
        ({"authentication": "ActiveDirectoryIntegrated"}, "ActiveDirectoryIntegrated"),
    ],
    ids=["access_token", "ad_password", "ad_integrated"],
)
def test_bulk_copy_rejects_credentials_it_cannot_sign_in_with(
    tmp_path, credential_kwargs: Dict[str, str], expected: str
) -> None:
    """mssql-py-core reconnects on its own and implements neither of these, so fail before any row."""
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id"], num_row_groups=1, rows_per_group=1)

    job, cursor = _make_job(file_path, rows_copied=1, **credential_kwargs)

    with pytest.raises(DestinationTerminalException, match=expected):
        job.run()
    assert cursor.bulkcopy_arrow.call_count == 0


def test_bulk_copy_classifies_driver_errors_and_keeps_their_message(tmp_path) -> None:
    """A transient login failure stays retryable; the driver's own text must survive."""
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id"], num_row_groups=1, rows_per_group=3)

    job, cursor = _make_job(file_path, rows_copied=3)
    cursor.bulkcopy_arrow.side_effect = mssql_python.OperationalError(
        driver_error="Database is not currently available", ddbc_error="40613"
    )

    with pytest.raises(DatabaseTransientException) as exc_info:
        job.run()

    assert "40613" in str(exc_info.value)
    # the cursor is released even when the copy blows up
    assert cursor.close.call_count == 1


def test_bulk_copy_classifies_a_data_error_as_terminal(tmp_path) -> None:
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id"], num_row_groups=1, rows_per_group=3)

    job, cursor = _make_job(file_path, rows_copied=3)
    cursor.bulkcopy_arrow.side_effect = mssql_python.ProgrammingError(
        driver_error="String or binary data would be truncated", ddbc_error="22001"
    )

    with pytest.raises(DatabaseTerminalException) as exc_info:
        job.run()

    assert "String or binary data would be truncated" in str(exc_info.value)


def test_bulk_copy_propagates_non_driver_errors_unchanged(tmp_path) -> None:
    """A non-DBAPI failure keeps its own type so dlt's default retry classification applies."""
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id"], num_row_groups=1, rows_per_group=3)

    job, cursor = _make_job(file_path, rows_copied=3)
    cursor.bulkcopy_arrow.side_effect = RuntimeError("token acquisition failed")

    with pytest.raises(RuntimeError, match="token acquisition failed"):
        job.run()
    assert cursor.close.call_count == 1


def test_native_arrow_bulk_copy_is_detected() -> None:
    found, err_str = has_native_arrow_bulk_copy()

    assert found is True
    assert err_str is None


@pytest.mark.parametrize("requests_parquet", [False, True], ids=["no_request", "parquet_requested"])
def test_file_format_selector_drops_parquet_when_unavailable(
    monkeypatch, requests_parquet: bool
) -> None:
    monkeypatch.setattr(
        "dlt.destinations.impl.mssql.bulk_copy.has_native_arrow_bulk_copy",
        lambda: (False, "no pyarrow"),
    )
    selector = make_native_parquet_file_format_selector(DOCS_URL, prefer_parquet=False)
    table_schema: TTableSchema = {"name": "items"}
    if requests_parquet:
        table_schema["file_format"] = "parquet"

    preferred, supported = selector(
        "insert_values", ["insert_values", "parquet", "model"], table_schema=table_schema
    )

    assert preferred == "insert_values"
    assert supported == ["insert_values", "model"]


def test_file_format_selector_keeps_insert_values_preferred() -> None:
    selector = make_native_parquet_file_format_selector(DOCS_URL, prefer_parquet=False)

    preferred, supported = selector(
        "insert_values", ["insert_values", "parquet", "model"], table_schema={"name": "items"}
    )

    assert preferred == "insert_values"
    assert "parquet" in supported


def test_file_format_selector_can_prefer_parquet() -> None:
    selector = make_native_parquet_file_format_selector(DOCS_URL, prefer_parquet=True)

    preferred, _ = selector(
        "insert_values", ["insert_values", "parquet", "model"], table_schema={"name": "items"}
    )

    assert preferred == "parquet"


def test_mssql_destination_does_not_prefer_parquet() -> None:
    caps = mssql().capabilities()

    assert caps.preferred_loader_file_format == "insert_values"
    assert "parquet" in caps.supported_loader_file_formats
