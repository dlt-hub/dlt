"""Unit tests for the native Arrow bulk copy parquet load job.

These drive the job against a mocked mssql-python cursor, so they need no SQL Server. The
end-to-end behaviour is covered by the `mssql`-marked tests further down and by the parametrized
`tests/load` suite.
"""

from typing import Any, Dict, List, Tuple, cast
from unittest.mock import MagicMock

import pytest

import dlt
from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.libs.pyarrow import pyarrow
from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableSchema, TWriteDisposition
from dlt.common.utils import uniq_id
from dlt.destinations import mssql
from dlt.destinations.impl.mssql.bulk_copy import (
    has_native_arrow_bulk_copy,
    make_native_parquet_file_format_selector,
)
from dlt.destinations.impl.mssql.configuration import MsSqlClientConfiguration, MsSqlCredentials
from dlt.destinations.impl.mssql.mssql import MsSqlBulkCopyArrowJob
from dlt.destinations.impl.mssql.sql_client import MsSqlClient

from tests.pipeline.utils import assert_load_info, load_table_counts

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential

DOCS_URL = "https://dlthub.com/docs/dlt-ecosystem/destinations/mssql#data-loading"


@pytest.fixture
def mssql_server() -> None:
    """Skips when no mssql destination credentials are configured."""
    try:
        resolve_configuration(MsSqlCredentials(), sections=("destination", "mssql"))
    except ConfigurationException as conf_ex:
        pytest.skip(f"no mssql destination configured: {conf_ex}")


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
    file_path: str, table_name: str = "items", **credential_kwargs: Any
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
    cursor.bulkcopy_arrow = MagicMock(return_value={"rows_copied": 12, "batch_count": 3})
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

    job, cursor = _make_job(file_path)
    job.run()

    source = cursor.bulkcopy_arrow.call_args.args[1]
    # the full stream is still pending after the call returned
    assert [batch.num_rows for batch in source] == [5, 5, 5, 5]


def test_bulk_copy_passes_qualified_table_and_explicit_column_mappings(tmp_path) -> None:
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id", "name", "created_at"], num_row_groups=1, rows_per_group=2)

    job, cursor = _make_job(file_path, table_name="items")
    job.run()

    assert cursor.bulkcopy_arrow.call_args.args[0] == '"my_dataset"."items"'
    # positional source index -> destination column name, in the load file's own order
    assert _bulkcopy_kwargs(cursor)["column_mappings"] == [
        (0, "id"),
        (1, "name"),
        (2, "created_at"),
    ]


def test_bulk_copy_keeps_nulls_and_uses_the_configured_timeout(tmp_path) -> None:
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id"], num_row_groups=1, rows_per_group=1)

    job, cursor = _make_job(file_path)
    job._job_client.config.bulk_copy_timeout = 900
    job.run()

    kwargs = _bulkcopy_kwargs(cursor)
    # column defaults must never replace a NULL that dlt wrote
    assert kwargs["keep_nulls"] is True
    assert kwargs["timeout"] == 900


def test_bulk_copy_skips_an_empty_file(tmp_path) -> None:
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id"], num_row_groups=0, rows_per_group=0)

    job, cursor = _make_job(file_path)
    job.run()

    assert cursor.bulkcopy_arrow.call_count == 0


def test_bulk_copy_rejects_a_pre_acquired_access_token(tmp_path) -> None:
    """`bulkcopy_arrow` reconnects natively and never sees `attrs_before`, so the token is lost."""
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id"], num_row_groups=1, rows_per_group=1)

    job, cursor = _make_job(file_path, access_token="a-token")

    with pytest.raises(DestinationTerminalException, match="access_token"):
        job.run()
    assert cursor.bulkcopy_arrow.call_count == 0


def test_bulk_copy_failure_is_terminal_so_the_job_is_not_retried(tmp_path) -> None:
    """A partial commit cannot be undone, so a retry would duplicate the committed prefix."""
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id"], num_row_groups=2, rows_per_group=3)

    job, cursor = _make_job(file_path)
    cursor.bulkcopy_arrow.side_effect = RuntimeError("TDS stream aborted")

    with pytest.raises(DestinationTerminalException) as exc_info:
        job.run()

    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert "may have committed part of the file" in str(exc_info.value)
    # the cursor is released even when the copy blows up
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

    # parquet stays available but opt-in: bulk copy is not rollback-safe like insert_values
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


@pytest.mark.mssql
def test_bulk_copy_loads_parquet_end_to_end(mssql_server: None) -> None:
    """Round-trips a parquet load through a real SQL Server, if one is configured."""
    pipeline = dlt.pipeline(
        pipeline_name="mssql_bulk_copy_" + uniq_id(),
        destination=mssql(),
        dataset_name="bulk_copy_data",
        dev_mode=True,
    )

    info = pipeline.run(
        [{"id": i, "name": f"n{i}", "maybe": None if i % 2 else i} for i in range(1000)],
        table_name="items",
        loader_file_format="parquet",
    )
    assert_load_info(info)
    assert load_table_counts(pipeline)["items"] == 1000

    # append a second load file into the same table
    info = pipeline.run(
        [{"id": i, "name": f"n{i}", "maybe": None} for i in range(1000, 1500)],
        table_name="items",
        loader_file_format="parquet",
    )
    assert_load_info(info)
    assert load_table_counts(pipeline)["items"] == 1500


@pytest.mark.mssql
@pytest.mark.parametrize(
    "write_disposition", ["replace", "merge"], ids=["replace", "merge_staging"]
)
def test_bulk_copy_staged_write_dispositions(
    mssql_server: None, write_disposition: TWriteDisposition
) -> None:
    """Both flows land the parquet job in the staging dataset before the followup job runs."""
    pipeline = dlt.pipeline(
        pipeline_name="mssql_bulk_copy_staged_" + uniq_id(),
        destination=mssql(),
        dataset_name="bulk_copy_data",
        dev_mode=True,
    )

    @dlt.resource(name="items", primary_key="id", write_disposition=write_disposition)
    def items(offset: int) -> Any:
        yield from ({"id": i, "name": f"n{i}"} for i in range(offset, offset + 100))

    assert_load_info(pipeline.run(items(0), loader_file_format="parquet"))
    assert load_table_counts(pipeline)["items"] == 100

    assert_load_info(pipeline.run(items(50), loader_file_format="parquet"))
    expected = 100 if write_disposition == "replace" else 150
    assert load_table_counts(pipeline)["items"] == expected
