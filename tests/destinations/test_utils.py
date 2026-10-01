from typing import Any, Optional

import dlt
import pytest

from dlt.common.destination import Destination
from dlt.common.destination.utils import prepare_load_table
from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableFormat
from dlt.destinations.utils import get_resource_for_adapter, verify_schema_merge_disposition
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
