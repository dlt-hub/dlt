import pytest
import pyarrow as pa
import pandas as pd
from typing import List, Dict, Any, Generator
from datetime import datetime, date, time
from decimal import Decimal

import dlt
from dlt.common.typing import DictStrAny
from dlt.common.utils import uniq_id
from tests.load.lancedb.utils import assert_table
from tests.pipeline.utils import assert_load_info
from tests.cases import arrow_table_all_data_types

# Mark all tests as essential, don't remove.
pytestmark = pytest.mark.essential

ALL_DATA_TYPES_BLOB = [
    {
        "id": 1,
        "string": "Hello World",
        "int8_field": 127,
        "int16_field": 32767,
        "int32_field": 2147483647,
        "int64_field": 9223372036854775807,
        "float32_field": 3.14159,
        "float64_field": 3.141592653589793,
        "bool_field": True,
        "date_field": date(2023, 1, 1),
        # "timestamp_field": datetime(2023, 1, 1, 12, 0, 0),
        # "time_field": time(12, 0, 0),
        "binary_field": b"binary data 1",
        "decimal_field": Decimal("123.45"),
    },
]

ANOTHER_ALL_DATA_TYPES_BLOB = [
    {
        "id": 2,
        "text_field": "Goodbye World",
        "int8_field": 100,
        "int16_field": 10000,
        "int32_field": 1000000,
        "int64_field": 1000000000000,
        "float32_field": 1.23456,
        "float64_field": 1.234567890123456,
        "bool_field": False,
        "date_field": date(2023, 6, 15),
        # "timestamp_field": datetime(2023, 6, 15, 15, 30, 45),
        # "time_field": time(15, 30, 45),
        "binary_field": b"more binary data",
        "decimal_field": Decimal("999.99"),
    },
]

all_data_schema = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("text_field", pa.string()),
        pa.field("int8_field", pa.int8()),
        pa.field("int16_field", pa.int16()),
        pa.field("int32_field", pa.int32()),
        pa.field("int64_field", pa.int64()),
        pa.field("float32_field", pa.float32()),
        pa.field("float64_field", pa.float64()),
        pa.field("bool_field", pa.bool_()),
        pa.field("date_field", pa.date32()),
        # pa.field("timestamp_field", pa.timestamp("ms")),
        # pa.field("time_field", pa.time64("ms")),
        pa.field("binary_field", pa.binary()),
        pa.field("decimal_field", pa.decimal128(10, 2)),
    ]
)


def test_identical_schemas() -> None:
    """Test that identical schemas return the original table."""
    pipeline = dlt.pipeline(
        pipeline_name="test_identical_schemas",
        destination="lancedb",
        dataset_name=f"test_identical_schemas_{uniq_id()}",
        dev_mode=True,
    )

    @dlt.resource(
        table_name="parent",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        primary_key="id",
        merge_key="id",
    )
    def identity_resource(
        data: List[DictStrAny],
    ) -> Generator[List[DictStrAny], None, None]:
        yield data

    data = ALL_DATA_TYPES_BLOB

    info = pipeline.run(identity_resource(data))
    assert_load_info(info)

    schema_after_first_load = pipeline.default_schema

    # Second load: Same schema, different data
    data2 = ANOTHER_ALL_DATA_TYPES_BLOB
    info = pipeline.run(identity_resource(data2))
    assert_load_info(info)

    # Verify schema should not have changed
    assert schema_after_first_load == pipeline.default_schema


def test_identical_schemas_arrow_table_all_types() -> None:
    """Test that identical schemas return the original table."""
    pipeline = dlt.pipeline(
        pipeline_name="test_identical_schemas_arrow_table_all_types",
        destination="lancedb",
        dataset_name=f"test_identical_schemas_arrow_table_all_types_{uniq_id()}",
        dev_mode=True,
    )

    arrow_table: pa.Table = None
    arrow_table, _, _ = arrow_table_all_data_types(object_format="arrow-table")

    @dlt.resource(
        table_name="all_types_table",
        primary_key="string",
    )
    def identity_resource(data: pa.Table) -> Generator[pa.Table, None, None]:
        yield data

    info = pipeline.run(identity_resource(arrow_table))
    assert_load_info(info)
    schema_after_first_load = pipeline.default_schema

    # Second load: Same schema, different data (more)
    arrow_table_more_data, _, _ = arrow_table_all_data_types(
        object_format="arrow-table", num_rows=1
    )
    info = pipeline.run(identity_resource(arrow_table_more_data))
    assert_load_info(info)

    # Verify schema should not have changed
    assert schema_after_first_load == pipeline.default_schema


def test_add_columns_of_new_types_one_by_one() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_slow_schema_evolution",
        destination="lancedb",
        dataset_name=f"test_slow_schema_evolution_{uniq_id()}",
        dev_mode=True,
    )

    _, _, object_data = arrow_table_all_data_types(
        object_format="arrow-table",
        include_null=False,
        include_not_normalized_name=False,
        include_decimal_arrow_max_precision=True, #-> breaks normalizer
        include_json=False,
        num_rows=1,
    )

    initial_data = {"id": 1} 

    @dlt.resource(
        table_name="all_types_table",
        primary_key="id",
    )
    def identity_resource(data: pa.Table) -> Generator[pa.Table, None, None]:
        yield data

    pipeline.run(identity_resource(initial_data))

    new_data = initial_data
    new_index = 2
    for data_type, data_item in object_data.items():
        if data_type in ["string_null", "float_null"]:
            # won't be able to infer schema from null value
            continue
        if data_type == "time":
            # time type is not supported by lancedb
            continue

        print('trying to add column of data type', data_type)
        new_data = {
            **new_data,
            "id": new_index,
            data_type: data_item[0],
        }
        print('new_data', new_data)
        new_index += 1

        info = pipeline.run(identity_resource(new_data))
        assert_load_info(info)
        # get data from destination
        with pipeline.destination_client() as client:
            table_name = client.make_qualified_table_name("all_types_table")
            tbl = client.db_client.open_table(table_name)
            actual_columns = set(tbl.schema.names)
            assert data_type in actual_columns, f"Expected {data_type} column to be present in destination table. Actual columns: {actual_columns}"
            print('passed for data type', data_type)
            # todo? check the actual datatype?


def test_new_column_in_second_load() -> None:
    """Test that new columns in source are added to the target."""
    pipeline = dlt.pipeline(
        pipeline_name="test_new_column_in_second_load",
        destination="lancedb",
        dataset_name=f"test_new_column_in_second_load_{uniq_id()}",
        dev_mode=True,
    )

    @dlt.resource(
        table_name="all_types_table",
        primary_key="string",
    )
    def identity_resource(data: pa.Table) -> Generator[pa.Table, None, None]:
        yield data

    arrow_table: pa.Table = None
    arrow_table, _, _ = arrow_table_all_data_types(
        object_format="arrow-table", num_rows=1, include_decimal=False
    )
    info = pipeline.run(identity_resource(arrow_table))
    assert_load_info(info)

    # Add an extra column to the source
    next_arrow_table, _, _ = arrow_table_all_data_types(
        object_format="arrow-table", num_rows=1, include_decimal=True
    )
    info = pipeline.run(identity_resource(next_arrow_table))
    assert_load_info(info)

    schema_in_pipeline = pipeline.default_schema
    assert "decimal" in schema_in_pipeline.tables["all_types_table"]["columns"]

    # Verify that the extra column is in the actual destination table
    with pipeline.destination_client() as client:
        table_name = client.make_qualified_table_name("all_types_table")
        tbl = client.db_client.open_table(table_name)

        # Get the actual table schema from the destination
        actual_columns = set(tbl.schema.names)

        # Check that the decimal column was added
        assert "decimal" in actual_columns, (
            "Expected 'decimal' column to be present in destination table. Actual columns:"
            f" {actual_columns}"
        )


def test_missing_column_in_second_load() -> None:
    """
    Test that missing columns in subsequent loads remove column from schema if merge strategy is
    used
    """
    pipeline = dlt.pipeline(
        pipeline_name="test_missing_column_in_second_load",
        destination="duckdb",
        dataset_name=f"test_missing_column_in_second_load_{uniq_id()}",
        dev_mode=True,
    )

    @dlt.resoource(
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        table_name="all_types_table",
        primary_key="string",
    )
    def identity_resource(data: pa.Table) -> Generator[pa.Table, None, None]:
        yield data

    arrow_table: pa.Table = None
    arrow_table, _, _ = arrow_table_all_data_types(object_format="arrow-table")

    info = pipeline.run(identity_resource(arrow_table))
    assert_load_info(info)

    # Remove a column from the source
    next_arrow_table, _, _ = arrow_table_all_data_types(
        object_format="arrow-table", num_rows=1, include_decimal=False
    )
    info = pipeline.run(identity_resource(next_arrow_table))
    assert_load_info(info)

    # ? what should actually happen here...
    # schema should no longer have the decimal column
    # schema_in_pipeline = pipeline.default_schema
    # assert "decimal" not in schema_in_pipeline.tables["all_types_table"]["columns"]

    # Verify that the column is missing in the actual destination table
    # with pipeline.destination_client() as client:
    #     table_name = client.make_qualified_table_name("all_types_table")
    #     tbl = client.db_client.open_table(table_name)

    #     # Get the actual table schema from the destination
    #     actual_columns = set(tbl.schema.names)

    #     # Check that the decimal column is missing
    #     assert "decimal" not in actual_columns, f"Expected 'decimal' column to be missing from destination table. Actual columns: {actual_columns}"

# TOOD
def test_type_casting() -> None:
    # Verify the time column has the correct type
    # assert time_field.type in [pa.time32("ms"), pa.time64("us")], f"Expected time type, got {time_field.type}"
    pass


#             pa.field("name", pa.string()),
#         ]
#     )

#     data = [{"id": 1, "value": 1.5, "name": "test1"}, {"id": 2, "value": 2.5, "name": "test2"}]
#     records = pa.Table.from_pylist(data, schema=source_schema)

#     result = align_schema(records, target_schema)

#     assert result.schema == target_schema
#     assert result.column_names == ["name", "id", "value"]

# def test_case_insensitive_matching(self) -> None:
#     """Test that column names are matched case-insensitively."""
#     target_schema = pa.schema(
#         [
#             pa.field("ID", pa.int64()),
#             pa.field("Name", pa.string()),
#             pa.field("VALUE", pa.float64()),
#         ]
#     )

#     source_schema = pa.schema(
#         [
#             pa.field("id", pa.int64()),
#             pa.field("name", pa.string()),
#             pa.field("value", pa.float64()),
#         ]
#     )

#     data = [{"id": 1, "name": "test1", "value": 1.5}, {"id": 2, "name": "test2", "value": 2.5}]
#     records = pa.Table.from_pylist(data, schema=source_schema)

#     result = align_schema(records, target_schema)

#     assert result.schema == target_schema
#     assert result.column_names == ["ID", "Name", "VALUE"]

# def test_type_casting(self) -> None:
#     """Test that incompatible types are cast when possible."""
#     target_schema = pa.schema(
#         [
#             pa.field("id", pa.int32()),  # Different type
#             pa.field("name", pa.string()),
#             pa.field("value", pa.float32()),  # Different type
#         ]
#     )

#     source_schema = pa.schema(
#         [
#             pa.field("id", pa.int64()),
#             pa.field("name", pa.string()),
#             pa.field("value", pa.float64()),
#         ]
#     )

#     data = [{"id": 1, "name": "test1", "value": 1.5}, {"id": 2, "name": "test2", "value": 2.5}]
#     records = pa.Table.from_pylist(data, schema=source_schema)

#     result = align_schema(records, target_schema)

#     assert result.schema == target_schema
#     assert result["id"].type == pa.int32()
#     assert result["value"].type == pa.float32()

# def test_type_casting_failure(self) -> None:
#     """Test that failed type casting results in NULL columns."""
#     target_schema = pa.schema(
#         [
#             pa.field("id", pa.int64()),
#             pa.field("name", pa.string()),
#             pa.field("value", pa.string()),  # Cannot cast float to string
#         ]
#     )

#     source_schema = pa.schema(
#         [
#             pa.field("id", pa.int64()),
#             pa.field("name", pa.string()),
#             pa.field("value", pa.float64()),
#         ]
#     )

#     data = [{"id": 1, "name": "test1", "value": 1.5}, {"id": 2, "name": "test2", "value": 2.5}]
#     records = pa.Table.from_pylist(data, schema=source_schema)

#     result = align_schema(records, target_schema)

#     assert result.schema == target_schema
#     assert result["value"].type == pa.string()
#     # PyArrow can actually cast float to string, so we check the values
#     assert result["value"].to_pylist() == ["1.5", "2.5"]

# def test_mixed_scenarios(self) -> None:
#     """Test a complex scenario with multiple issues."""
#     target_schema = pa.schema(
#         [
#             pa.field("Name", pa.string()),
#             pa.field("ID", pa.int32()),
#             pa.field("missing", pa.string()),
#             pa.field("VALUE", pa.float32()),
#         ]
#     )

#     source_schema = pa.schema(
#         [
#             pa.field("id", pa.int64()),
#             pa.field("extra", pa.string()),
#             pa.field("name", pa.string()),
#             pa.field("value", pa.float64()),
#         ]
#     )

#     data = [
#         {"id": 1, "extra": "ignored", "name": "test1", "value": 1.5},
#         {"id": 2, "extra": "ignored", "name": "test2", "value": 2.5},
#     ]
#     records = pa.Table.from_pylist(data, schema=source_schema)

#     result = align_schema(records, target_schema)

#     assert result.schema == target_schema
#     assert result.column_names == ["Name", "ID", "missing", "VALUE"]
#     assert result["missing"].null_count == 2  # Missing column should be NULL
#     assert "extra" not in result.column_names  # Extra column should be ignored

# def test_empty_table(self) -> None:
#     """Test with an empty table."""
#     target_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])

#     source_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])

#     records = pa.Table.from_pylist([], schema=source_schema)

#     result = align_schema(records, target_schema)

#     assert result.schema == target_schema
#     assert len(result) == 0

# def test_single_column(self) -> None:
#     """Test with a single column."""
#     target_schema = pa.schema([pa.field("id", pa.int64())])

#     source_schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])

#     data = [{"id": 1, "name": "test"}]
#     records = pa.Table.from_pylist(data, schema=source_schema)

#     result = align_schema(records, target_schema)

#     assert result.schema == target_schema
#     assert len(result.columns) == 1
#     assert result.column_names == ["id"]
