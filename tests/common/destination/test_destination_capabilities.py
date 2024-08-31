import pytest

from dlt.common.destination.exceptions import DestinationCapabilitiesException
from dlt.common.destination.utils import resolve_merge_strategy
from dlt.common.schema.schema import Schema
from dlt.common.schema.utils import new_table


def test_resolve_merge_strategy() -> None:
    schema = Schema("schema")

    table = new_table("table", write_disposition="merge")
    delta_table = new_table("delta_table", table_format="delta", write_disposition="merge")
    iceberg_table = new_table("delta_table", table_format="iceberg", write_disposition="merge")

    schema.update_table(table)
    schema.update_table(delta_table)
    schema.update_table(iceberg_table)

    assert resolve_merge_strategy(schema.tables, table) is None
    assert resolve_merge_strategy(schema.tables, delta_table) is None
    assert resolve_merge_strategy(schema.tables, iceberg_table) is None

    # try default merge dispositions
    from dlt.destinations import athena, filesystem, duckdb

    assert resolve_merge_strategy(schema.tables, table, filesystem().capabilities()) is None
    assert (
        resolve_merge_strategy(schema.tables, delta_table, filesystem().capabilities()) == "upsert"
    )
    assert (
        resolve_merge_strategy(schema.tables, iceberg_table, athena().capabilities())
        == "delete-insert"
    )

    # unknown table formats
    assert resolve_merge_strategy(schema.tables, iceberg_table, filesystem().capabilities()) is None
    assert resolve_merge_strategy(schema.tables, delta_table, athena().capabilities()) is None

    # not supported strategy
    schema.tables["delta_table"]["x-merge-strategy"] = "delete-insert"  # type: ignore[typeddict-unknown-key]
    with pytest.raises(DestinationCapabilitiesException):
        resolve_merge_strategy(schema.tables, delta_table, filesystem().capabilities())

    # non-default strategy
    schema.tables["table"]["x-merge-strategy"] = "scd2"  # type: ignore[typeddict-unknown-key]
    assert resolve_merge_strategy(schema.tables, table, filesystem().capabilities()) is None
    assert resolve_merge_strategy(schema.tables, table, duckdb().capabilities()) == "scd2"
