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
from tests.utils import TestDataItemFormat


@pytest.mark.parametrize("object_format", ["object", "pandas", "arrow-table"])
def test_identical_schemas_all_types(object_format: TestDataItemFormat) -> None:
    """Test that identical schemas return the original table."""
    pipeline = dlt.pipeline(
        pipeline_name="test_identical_schemas_arrow_table_all_types",
        destination="lancedb",
        dataset_name=f"test_identical_schemas_arrow_table_all_types_{uniq_id()}",
        dev_mode=True,
    )

    arrow_table: pa.Table = None
    arrow_table, _, _ = arrow_table_all_data_types(object_format=object_format)

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
        object_format="arrow-table", num_rows=4
    )
    info = pipeline.run(identity_resource(arrow_table_more_data))
    assert_load_info(info)

    # Verify schema should not have changed
    assert schema_after_first_load == pipeline.default_schema


@pytest.mark.parametrize("object_format", ["object", "pandas", "arrow-table"])
def test_add_columns_of_new_types_one_by_one(object_format: TestDataItemFormat) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_slow_schema_evolution",
        destination="lancedb",
        dataset_name=f"test_slow_schema_evolution_{uniq_id()}",
        dev_mode=True,
    )

    _, _, object_data = arrow_table_all_data_types(
        object_format=object_format,
        include_null=False,
        include_not_normalized_name=False,
        # include_decimal_arrow_max_precision=True,  # -> breaks normalizer
        include_json=False,  # ->  breaks on subsquent loads -> todo:
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

        print("trying to add column of data type", data_type)
        new_data = {**new_data, "id": new_index, data_type: data_item[0]}
        print("new_data", new_data)
        new_index += 1

        info = pipeline.run(identity_resource(new_data))
        assert_load_info(info)
        # get data from destination
        with pipeline.destination_client() as client:
            table_name = client.make_qualified_table_name("all_types_table")  # type: ignore[attr-defined]
            tbl = client.db_client.open_table(table_name)  # type: ignore[attr-defined]
            actual_columns = set(tbl.schema.names)
            if data_type == "json":
                data_type = "json__a"
            assert data_type in actual_columns, (
                f"Expected {data_type} column to be present in destination table. Actual columns:"
                f" {actual_columns}"
            )
            print("passed for data type", data_type)
            # todo? check the actual datatype?


@pytest.mark.parametrize("object_format", ["object", "pandas", "arrow-table"])
def test_new_column_in_second_load(object_format: TestDataItemFormat) -> None:
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
        object_format=object_format, num_rows=1, include_decimal=False, include_json=False
    )

    info = pipeline.run(identity_resource(arrow_table))
    assert_load_info(info)

    # Add an extra column to the source
    next_arrow_table, _, _ = arrow_table_all_data_types(
        object_format=object_format, num_rows=1, include_decimal=True, include_json=False
    )
    info = pipeline.run(identity_resource(next_arrow_table))
    assert_load_info(info)

    schema_in_pipeline = pipeline.default_schema
    assert "decimal" in schema_in_pipeline.tables["all_types_table"]["columns"]

    # Verify that the extra column is in the actual destination table
    with pipeline.destination_client() as client:
        table_name = client.make_qualified_table_name("all_types_table")  # type: ignore[attr-defined]
        tbl = client.db_client.open_table(table_name)  # type: ignore[attr-defined]

        # Get the actual table schema from the destination
        actual_columns = set(tbl.schema.names)

        # Check that the decimal column was added
        assert "decimal" in actual_columns, (
            "Expected 'decimal' column to be present in destination table. Actual columns:"
            f" {actual_columns}"
        )


# @pytest.mark.skip(reason="not completely understood what should happen")
# this fails not sure why
@pytest.mark.parametrize("object_format", ["object", "pandas", "arrow-table"])
def test_missing_column_in_second_load(object_format: TestDataItemFormat) -> None:
    """
    Test if same data is loaded with missing column and merge stragegy is present, column
    is removed from lancedb table.
    """
    pipeline = dlt.pipeline(
        pipeline_name="test_missing_column_in_second_load",
        destination="lancedb",
        dataset_name=f"test_missing_column_in_second_load_{uniq_id()}",
        dev_mode=True,
    )

    @dlt.resource(
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        table_name="all_types_table",
        primary_key="int",
        merge_key="int",
    )
    def identity_resource(data: pa.Table) -> Generator[pa.Table, None, None]:
        yield data

    arrow_table: pa.Table = None
    arrow_table, _, _ = arrow_table_all_data_types(
        object_format=object_format,
        include_null=False,
        include_json=False,
        num_rows=1,
    )
    # remove columns string_null and float_null
    arrow_table = arrow_table.drop(["string_null", "float_null"])

    # do a first run to establish schema
    info = pipeline.run(identity_resource(arrow_table))
    assert_load_info(info)

    # Remove a column from the data
    removed_column = "bool"
    next_arrow_table = arrow_table.drop([removed_column])
    assert removed_column not in next_arrow_table.schema.names

    # second load, data with same id, but one less column
    info = pipeline.run(identity_resource(next_arrow_table))
    assert_load_info(info)

    # schema should no longer have the decimal column
    schema_in_pipeline = pipeline.default_schema
    assert removed_column not in schema_in_pipeline.tables["all_types_table"]["columns"]

    # Verify that the column is missing in the actual destination table
    with pipeline.destination_client() as client:
        table_name = client.make_qualified_table_name("all_types_table")  # type: ignore[attr-defined]
        tbl = client.db_client.open_table(table_name)  # type: ignore[attr-defined]

        # Get the actual table schema from the destination
        actual_columns = set(tbl.schema.names)

        # Check that the decimal column is missing
        assert removed_column not in actual_columns


# @pytest.mark.xfail(reason="normalizer issue?")
def test_json_nesting_evolution() -> None:
    """Test that json nesting evolution is handled correctly."""
    pipeline = dlt.pipeline(
        pipeline_name="test_json_nesting_evolution",
        destination="lancedb",
        dataset_name=f"test_json_nesting_evolution_{uniq_id()}",
        dev_mode=True,
    )

    @dlt.resource(
        table_name="nesting_table",
        primary_key="id",
    )
    def identity_resource(data: pa.Table) -> Generator[pa.Table, None, None]:
        yield data

    data = [
        {"id": 1, "json": {"a": 1, "b": {"c": 2}}},
        {"id": 2, "json": {"a": 3, "b": {"c": 4}}},
    ]

    info = pipeline.run(identity_resource(data))
    assert_load_info(info)

    schema_in_pipeline = pipeline.default_schema
    assert "json__a" in schema_in_pipeline.tables["nesting_table"]["columns"]
    assert "json__b__c" in schema_in_pipeline.tables["nesting_table"]["columns"]

    with pipeline.destination_client() as client:
        table_name = client.make_qualified_table_name("nesting_table")  # type: ignore[attr-defined]
        tbl = client.db_client.open_table(table_name)  # type: ignore[attr-defined]
        print("tbl.schema.names", tbl.schema.names)
        assert "json__a" in tbl.schema.names
        assert "json__b__c" in tbl.schema.names

    # morph the json to a new structure, more nesting inside b and a new key too
    data = [
        {"id": 1, "json": {"a": 1, "b": {"c": {"c1": 1}, "d": 3}}},
    ]

    info = pipeline.run(identity_resource(data))
    assert_load_info(info)
    assert "json__b__c__c1" in schema_in_pipeline.tables["nesting_table"]["columns"]
    assert "json__b__d" in schema_in_pipeline.tables["nesting_table"]["columns"]
    # and json__b__c still be there too
    assert "json__b__c" in schema_in_pipeline.tables["nesting_table"]["columns"]

    # print("both schemas", schema_in_pipeline, pipeline.default_schema)
    # print("schema_in_pipeline.tables", schema_in_pipeline.tables["nesting_table"]["columns"].keys())
    with pipeline.destination_client() as client:
        table_name = client.make_qualified_table_name("nesting_table")  # type: ignore[attr-defined]
        tbl = client.db_client.open_table(table_name)  # type: ignore[attr-defined]
        assert "json__b__c__c1" in tbl.schema.names
        assert "json__b__d" in tbl.schema.names
        assert "json__b__c" in tbl.schema.names
