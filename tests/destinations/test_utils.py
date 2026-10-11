from datetime import date, datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

import dlt
import pytest

from dlt.common.data_writers.escape import escape_snowflake_literal
from dlt.common.destination import Destination
from dlt.common.destination import utils as destination_utils
from dlt.common.destination.utils import expand_merge_condition, prepare_load_table
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
        ("name LIKE '{{x}}%'", "name LIKE '{x}%'"),
    ],
    ids=["identifier_and_string", "date_and_int", "literal_brace"],
)
def test_expand_merge_condition(condition: str, expected: str) -> None:
    values = {"tenant": "O'B", "day": date(2024, 1, 1), "n": 3}
    expanded = expand_merge_condition(
        "destination_scope", condition, {"table": '"t"'}, values, escape_snowflake_literal
    )
    assert expanded == expected


def test_expand_merge_condition_missing_value() -> None:
    with pytest.raises(ValueError) as py_ex:
        expand_merge_condition("source_filter", "day = {day}", {}, {}, escape_snowflake_literal)
    assert "unknown placeholders `{day}`" in str(py_ex.value)


def test_merge_placeholder_values_of_load_date(monkeypatch: pytest.MonkeyPatch) -> None:
    # 23:30 in Kolkata is 18:00 UTC of the same day. the values follow UTC
    created_at = datetime(2024, 3, 1, 23, 30, tzinfo=ZoneInfo("Asia/Kolkata"))
    package = {"load_id": "1.2", "state": {"created_at": created_at, "placeholders": {}}}
    monkeypatch.setattr(destination_utils, "load_package_state", lambda: package)

    values = destination_utils.get_merge_placeholder_values("items")
    assert values["curr_date"] == date(2024, 3, 1)
    assert (values["YYYY"], values["MM"], values["DD"], values["HH"]) == (2024, 3, 1, 18)
    assert (values["MMMM"], values["dddd"]) == ("march", "friday")
    assert values["load_package_timestamp"] == int(created_at.timestamp())
