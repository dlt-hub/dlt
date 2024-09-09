import pytest

from dlt.common.destination.exceptions import DestinationCapabilitiesException, UnsupportedDataType
from dlt.common.destination.utils import (
    resolve_merge_strategy,
    verify_schema_capabilities,
    verify_supported_data_types,
)
from dlt.common.exceptions import TerminalValueError
from dlt.common.schema.exceptions import SchemaIdentifierNormalizationCollision
from dlt.common.schema.schema import Schema
from dlt.common.schema.utils import new_table
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.destinations.impl.bigquery.bigquery_adapter import AUTODETECT_SCHEMA_HINT


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


def test_verify_capabilities_ident_collisions() -> None:
    schema = Schema("schema")
    table = new_table(
        "table",
        write_disposition="merge",
        columns=[{"name": "col1", "data_type": "bigint"}, {"name": "COL1", "data_type": "bigint"}],
    )
    schema.update_table(table, normalize_identifiers=False)
    from dlt.destinations import athena, filesystem

    # case sensitive - no name collision
    exceptions = verify_schema_capabilities(schema, filesystem().capabilities(), "filesystem")
    assert len(exceptions) == 0
    # case insensitive - collision on column name
    exceptions = verify_schema_capabilities(schema, athena().capabilities(), "filesystem")
    assert len(exceptions) == 1
    assert isinstance(exceptions[0], SchemaIdentifierNormalizationCollision)
    assert exceptions[0].identifier_type == "column"

    table = new_table(
        "TABLE", write_disposition="merge", columns=[{"name": "col1", "data_type": "bigint"}]
    )
    schema.update_table(table, normalize_identifiers=False)
    exceptions = verify_schema_capabilities(schema, filesystem().capabilities(), "filesystem")
    assert len(exceptions) == 0
    # case insensitive - collision on table name
    exceptions = verify_schema_capabilities(schema, athena().capabilities(), "filesystem")
    assert len(exceptions) == 2
    assert isinstance(exceptions[1], SchemaIdentifierNormalizationCollision)
    assert exceptions[1].identifier_type == "table"


def test_verify_capabilities_data_types() -> None:
    schema = Schema("schema")
    table = new_table(
        "table",
        write_disposition="merge",
        columns=[{"name": "col1", "data_type": "time"}, {"name": "col2", "data_type": "date"}],
    )
    schema.update_table(table, normalize_identifiers=False)

    schema.update_table(table, normalize_identifiers=False)
    from dlt.destinations import athena, filesystem, databricks, redshift

    new_jobs_parquet = [ParsedLoadJobFileName.parse("table.12345.1.parquet")]
    new_jobs_jsonl = [ParsedLoadJobFileName.parse("table.12345.1.jsonl")]

    # all data types supported (no mapper)
    exceptions = verify_supported_data_types(
        schema.tables.values(), new_jobs_parquet, filesystem().capabilities(), "filesystem"  # type: ignore[arg-type]
    )
    assert len(exceptions) == 0
    # time not supported via list
    exceptions = verify_supported_data_types(
        schema.tables.values(), new_jobs_parquet, athena().capabilities(), "athena"  # type: ignore[arg-type]
    )
    assert len(exceptions) == 1
    assert isinstance(exceptions[0], UnsupportedDataType)
    assert exceptions[0].destination_type == "athena"
    assert exceptions[0].table_name == "table"
    assert exceptions[0].column == "col1"
    assert exceptions[0].file_format == "parquet"
    assert exceptions[0].available_in_formats == []

    # all supported on parquet
    exceptions = verify_supported_data_types(
        schema.tables.values(), new_jobs_parquet, databricks().capabilities(), "databricks"  # type: ignore[arg-type]
    )
    assert len(exceptions) == 0
    # date not supported on jsonl
    exceptions = verify_supported_data_types(
        schema.tables.values(), new_jobs_jsonl, databricks().capabilities(), "databricks"  # type: ignore[arg-type]
    )
    assert len(exceptions) == 1
    assert isinstance(exceptions[0], UnsupportedDataType)
    assert exceptions[0].column == "col2"
    assert exceptions[0].available_in_formats == ["parquet"]

    # exclude binary type if precision is set on column
    schema_bin = Schema("schema_bin")
    table = new_table(
        "table",
        write_disposition="merge",
        columns=[
            {"name": "binary_1", "data_type": "binary"},
            {"name": "binary_2", "data_type": "binary", "precision": 128},
        ],
    )
    schema_bin.update_table(table, normalize_identifiers=False)
    exceptions = verify_supported_data_types(
        schema_bin.tables.values(),  # type: ignore[arg-type]
        new_jobs_jsonl,
        redshift().capabilities(),
        "redshift",
    )
    # binary not supported on jsonl
    assert len(exceptions) == 2
    exceptions = verify_supported_data_types(
        schema_bin.tables.values(), new_jobs_parquet, redshift().capabilities(), "redshift"  # type: ignore[arg-type]
    )
    # fixed length not supported on parquet
    assert len(exceptions) == 1
    assert isinstance(exceptions[0], UnsupportedDataType)
    assert exceptions[0].data_type == "binary(128)"
    assert exceptions[0].column == "binary_2"
    assert exceptions[0].available_in_formats == ["insert_values"]

    # check nested type on bigquery
    from dlt.destinations import bigquery

    schema_nested = Schema("nested")
    table = new_table(
        "table",
        write_disposition="merge",
        columns=[
            {"name": "nested_1", "data_type": "json"},
        ],
    )
    schema_nested.update_table(table, normalize_identifiers=False)
    exceptions = verify_supported_data_types(
        schema_nested.tables.values(), new_jobs_parquet, bigquery().capabilities(), "bigquery"  # type: ignore[arg-type]
    )
    assert len(exceptions) == 1
    assert isinstance(exceptions[0], UnsupportedDataType)
    assert exceptions[0].data_type == "json"

    # enable schema autodetect
    table[AUTODETECT_SCHEMA_HINT] = True  # type: ignore[typeddict-unknown-key]
    exceptions = verify_supported_data_types(
        schema_nested.tables.values(), new_jobs_parquet, bigquery().capabilities(), "bigquery"  # type: ignore[arg-type]
    )
    assert len(exceptions) == 0

    # lancedb uses arrow types in type mapper
    from dlt.destinations import lancedb

    exceptions = verify_supported_data_types(
        schema_bin.tables.values(),  # type: ignore[arg-type]
        new_jobs_jsonl,
        lancedb().capabilities(),
        "lancedb",
    )
    try:
        import pyarrow

        assert len(exceptions) == 0
    except ImportError:
        assert len(exceptions) > 0

    # provoke mapping error, precision not supported on NTZ timestamp
    schema_timezone = Schema("tx")
    table = new_table(
        "table",
        write_disposition="merge",
        columns=[
            {"name": "ts_1", "data_type": "timestamp", "precision": 12, "timezone": False},
        ],
    )
    schema_timezone.update_table(table, normalize_identifiers=False)
    from dlt.destinations import motherduck

    exceptions = verify_supported_data_types(
        schema_timezone.tables.values(), new_jobs_parquet, motherduck().capabilities(), "motherduck"  # type: ignore[arg-type]
    )
    assert len(exceptions) == 1
    assert isinstance(exceptions[0], TerminalValueError)
