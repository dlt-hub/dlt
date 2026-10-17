from datetime import date, datetime, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

import dlt
import pytest

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.timezone_context import TimezoneContext
from dlt.common.data_writers.escape import escape_snowflake_literal
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.destination import utils as destination_utils
from dlt.common.destination.utils import (
    expand_merge_condition,
    get_scd2_timestamps,
    prepare_load_table,
)
from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableFormat
from dlt.destinations.utils import (
    get_resource_for_adapter,
    verify_schema_merge_disposition,
    verify_unsupported_merge_options,
)
from dlt.extract import DltResource

from tests.utils import capture_dlt_logger


def test_get_resource_for_adapter() -> None:
    # test on pure data
    data = [1, 2, 3]
    adapted_resource = get_resource_for_adapter(data)
    assert isinstance(adapted_resource, DltResource)
    assert list(adapted_resource) == [1, 2, 3]
    assert adapted_resource.name == "content"

    # test on resource
    @dlt.resource(table_name="my_table")
    def some_resource():
        yield [1, 2, 3]

    adapted_resource = get_resource_for_adapter(some_resource)
    assert adapted_resource == some_resource
    assert adapted_resource.name == "some_resource"

    # test on source with one resource
    @dlt.source
    def source():
        return [some_resource]

    adapted_resource = get_resource_for_adapter(source())
    assert adapted_resource.table_name == "my_table"

    # test on source with multiple resources
    @dlt.resource(table_name="my_table")
    def other_resource():
        yield [1, 2, 3]

    @dlt.source
    def other_source():
        return [some_resource, other_resource]

    with pytest.raises(ValueError):
        get_resource_for_adapter(other_source())


@pytest.mark.parametrize(
    "destination,table_format,ignored_options",
    [
        ("duckdb", None, ["skip_unchanged_rows", "row_version_column_name"]),
        ("filesystem", "delta", []),
    ],
    ids=["delete_insert_default", "upsert_default"],
)
def test_verify_merge_options_with_default_strategy(
    destination: str, table_format: Optional[TTableFormat], ignored_options: Any, caplog: Any
) -> None:
    """Without a strategy, the destination keeps the merge options that its default strategy
    supports and warns about the others."""

    disposition: Any = {
        "disposition": "merge",
        "skip_unchanged_rows": True,
        "row_version_column_name": "version",
    }

    @dlt.resource(
        primary_key="id",
        table_format=table_format,
        columns={"id": {"data_type": "bigint"}, "version": {"data_type": "bigint"}},
        write_disposition=disposition,
    )
    def items():
        yield [{"id": 1, "version": 1}]

    schema = Schema("test")
    table = schema.update_table(items().compute_table_schema())
    caps = Destination.from_reference(destination).capabilities()
    load_table = prepare_load_table(schema.tables, table, caps)
    with capture_dlt_logger(caplog):
        assert verify_schema_merge_disposition(schema, [load_table], caps) == []
    warnings = "\n".join(
        r.message for r in caplog.records if "dlt ignores this option" in r.message
    )
    for option in ("skip_unchanged_rows", "row_version_column_name"):
        assert (f"`{option}`" in warnings) is (option in ignored_options)


@pytest.mark.parametrize("write_disposition", ["append", "merge"])
def test_verify_unsupported_merge_options(write_disposition: str) -> None:
    """A destination that cannot apply a merge option rejects it, only on merge tables."""
    disposition: Any = write_disposition
    if write_disposition == "merge":
        disposition = {
            "disposition": "merge",
            "strategy": "upsert",
            "skip_unchanged_rows": True,
            "source_filter": "id > 0",
        }

    @dlt.resource(
        primary_key="id", columns={"id": {"data_type": "bigint"}}, write_disposition=disposition
    )
    def items():
        yield [{"id": 1}]

    schema = Schema("test")
    table = schema.update_table(items().compute_table_schema())
    caps = Destination.from_reference("lancedb").capabilities()
    load_table = prepare_load_table(schema.tables, table, caps)
    exceptions = verify_unsupported_merge_options(
        schema, [load_table], caps, "lancedb", ("skip_unchanged_rows", "source_filter")
    )
    if write_disposition == "append":
        assert exceptions == []
    else:
        assert [str(e) for e in exceptions] == [
            (
                "In schema `test`: dlt does not support `skip_unchanged_rows` with the `upsert`"
                " merge strategy on `lancedb` table `items`."
            ),
            (
                "In schema `test`: dlt does not support `source_filter` with the `upsert` merge"
                " strategy on `lancedb` table `items`. Filter the yielded items with"
                " `resource.add_filter()` instead."
            ),
        ]


@pytest.mark.parametrize(
    "condition,expected",
    [
        ("{table}.id = {tenant}", "\"t\".id = 'O''B'"),
        ("day = {day} AND n > {n}", "day = '2024-01-01' AND n > 3"),
        ("ts >= {since}", "ts >= '2024-01-01T12:00:00+00:00'"),
        ("name LIKE '{{x}}%'", "name LIKE '{x}%'"),
        ("day IN {days}", "day IN ('2024-01-01', '2024-01-02')"),
        ("tenant IN {tenants}", "tenant IN ('O''B', 'A')"),
        ("ts IN {stamps}", "ts IN ('2024-01-01T11:00:00+00:00')"),
        ("day IN {no_days}", "day IN (NULL)"),
    ],
    ids=[
        "identifier_and_string",
        "date_and_int",
        "naive_datetime",
        "literal_brace",
        "list_of_dates",
        "tuple_of_strings",
        "list_of_aware_datetimes",
        "empty_list",
    ],
)
def test_expand_merge_condition(condition: str, expected: str) -> None:
    values = {
        "tenant": "O'B",
        "day": date(2024, 1, 1),
        "n": 3,
        "since": datetime(2024, 1, 1, 12),
        "days": [date(2024, 1, 1), date(2024, 1, 2)],
        "tenants": ("O'B", "A"),
        "stamps": [datetime(2024, 1, 1, 12, tzinfo=ZoneInfo("Europe/Berlin"))],
        "no_days": [],
    }
    expanded = expand_merge_condition(
        "destination_scope", condition, {"table": '"t"'}, values, escape_snowflake_literal
    )
    assert expanded == expected


def test_expand_merge_condition_missing_value() -> None:
    with pytest.raises(ValueError) as py_ex:
        expand_merge_condition("source_filter", "day = {day}", {}, {}, escape_snowflake_literal)
    assert "unknown placeholders `{day}`" in str(py_ex.value)


def test_scd2_timestamps(monkeypatch: pytest.MonkeyPatch) -> None:
    caps = DestinationCapabilitiesContext.generic_capabilities()
    caps.timestamp_precision = 3
    created_at = datetime(2024, 3, 1, 23, 30, 0, 123456, tzinfo=ZoneInfo("Asia/Kolkata"))
    state: Any = {"created_at": created_at}
    package = {"load_id": "1.2", "state": state}
    monkeypatch.setattr(destination_utils, "load_package_state", lambda: package)
    valid_to: Any = {
        "name": "_dlt_valid_to",
        "data_type": "timestamp",
        "x-active-record-timestamp": None,
    }
    # a hint left by an older dlt version is a stale value
    table: Any = {
        "name": "items",
        "x-boundary-timestamp": datetime(2020, 1, 1),
        "columns": {"_dlt_valid_to": valid_to},
    }

    # the boundary defaults to the package creation time, in UTC at the precision of the destination
    boundary, active_record = get_scd2_timestamps(table, caps)
    assert boundary == created_at.replace(microsecond=123000)
    assert str(boundary.tzinfo) == "UTC"
    assert active_record is None

    # the package holds the declared boundary. like loaded data, a naive value is read in the
    # context timezone and an aware value is converted to it
    state["placeholders"] = {"items": {"boundary_timestamp": datetime(2024, 1, 1, 12)}}
    valid_to["x-active-record-timestamp"] = "9999-12-31T00:00:00+01:00"
    for context_tz in ("UTC", "Europe/Berlin"):
        with Container().injectable_context(TimezoneContext(context_tz)):
            boundary, active_record = get_scd2_timestamps(table, caps)
        assert boundary == datetime(2024, 1, 1, 12, tzinfo=ZoneInfo(context_tz))
        assert active_record == datetime(9999, 12, 30, 23, tzinfo=timezone.utc)
        assert str(active_record.tzinfo) == context_tz

    # a destination without tz support stores the wall clock of the context timezone, UTC here
    caps.supports_tz_aware_datetime = False
    state["placeholders"]["items"]["boundary_timestamp"] = "2024-01-01T12:00:00+01:00"
    valid_to["x-active-record-timestamp"] = "9999-12-31"
    assert get_scd2_timestamps(table, caps) == (datetime(2024, 1, 1, 11), datetime(9999, 12, 31))


def test_merge_placeholder_values_of_load_date(monkeypatch: pytest.MonkeyPatch) -> None:
    # 23:30 in Kolkata is 18:00 UTC of the same day. the values follow UTC
    created_at = datetime(2024, 3, 1, 23, 30, tzinfo=ZoneInfo("Asia/Kolkata"))
    package: Any = {"load_id": "1.2", "state": {"created_at": created_at, "placeholders": {}}}
    monkeypatch.setattr(destination_utils, "load_package_state", lambda: package)

    values = destination_utils.get_merge_placeholder_values("items")
    assert values["curr_date"] == date(2024, 3, 1)
    assert (values["YYYY"], values["MM"], values["DD"], values["HH"]) == (2024, 3, 1, 18)
    assert (values["MMMM"], values["dddd"]) == ("march", "friday")
    assert values["load_package_timestamp"] == int(created_at.timestamp())
    assert values["boundary_timestamp"] == created_at
    assert values["boundary_timestamp"].tzinfo is timezone.utc

    # a declared boundary is stored in the load package
    package["state"]["placeholders"] = {"items": {"boundary_timestamp": date(2024, 1, 1)}}
    values = destination_utils.get_merge_placeholder_values("items")
    assert values["boundary_timestamp"] == date(2024, 1, 1)
