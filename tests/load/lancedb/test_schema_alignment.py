import os
import pytest
import pyarrow as pa
from typing import TYPE_CHECKING, Any, Generator, List, cast
from decimal import Decimal

import dlt
from dlt.common.storages.file_storage import FileStorage
from dlt.common.typing import DictStrAny
from dlt.pipeline.exceptions import PipelineStepFailed
from tests.load.lancedb.utils import (
    LANCE_DEST_CONFS,
    get_adapter,
    open_lance_table,
    read_arrow_table,
)
from tests.load.utils import DestinationTestConfiguration, destinations_configs
from tests.pipeline.utils import assert_load_info
from tests.cases import arrow_table_all_data_types, remove_column_from_data
from tests.utils import TestDataItemFormat

if TYPE_CHECKING:
    from tests.load.lancedb.utils import TLanceDestinationClient
else:
    TLanceDestinationClient = Any


@pytest.mark.parametrize("object_format", ["object", "pandas", "arrow-table"])
@pytest.mark.parametrize(
    "destination_config",
    LANCE_DEST_CONFS,
    ids=lambda x: x.name,
)
def test_identical_schemas_all_types(
    destination_config: DestinationTestConfiguration, object_format: TestDataItemFormat
) -> None:
    """Test that identical schemas return the original table."""
    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_identical_schemas_all_types",
        dev_mode=True,
    )

    table: pa.Table = None
    table, _, _ = arrow_table_all_data_types(
        object_format=object_format,
        include_decimal_high_precision=True,
    )

    @dlt.resource(
        table_name="all_types_table",
        primary_key="string",
    )
    def identity_resource(data: Any) -> Generator[Any, None, None]:
        yield data

    info = pipeline.run(identity_resource(table))
    assert_load_info(info)
    schema_after_first_load = pipeline.default_schema

    # Second load: Same schema, different data (more)
    table_more_data, _, _ = arrow_table_all_data_types(object_format=object_format, num_rows=4)
    info = pipeline.run(identity_resource(table_more_data))
    assert_load_info(info)

    # Verify schema should not have changed
    assert schema_after_first_load == pipeline.default_schema


@pytest.mark.parametrize(
    "destination_config",
    LANCE_DEST_CONFS,
    ids=lambda x: x.name,
)
def test_add_columns_of_new_types_one_by_one(
    destination_config: DestinationTestConfiguration,
) -> None:
    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_add_columns_of_new_types_one_by_one",
        dev_mode=True,
    )

    _, _, object_data = arrow_table_all_data_types(
        object_format="object",
        include_null=False,
        include_not_normalized_name=False,
        include_decimal_high_precision=True,
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

        new_data = {**new_data, "id": new_index, data_type: data_item[0]}
        new_index += 1

        info = pipeline.run(identity_resource(new_data))
        assert_load_info(info)
        # get data from destination
        with pipeline.destination_client() as client:
            client = cast(TLanceDestinationClient, client)
            tbl = open_lance_table(client, "all_types_table")
            actual_columns = set(tbl.schema.names)
            if data_type == "json":
                data_type = "json__a"
            assert data_type in actual_columns, (
                f"Expected {data_type} column to be present in destination table. Actual columns:"
                f" {actual_columns}"
            )


@pytest.mark.parametrize("object_format", ["object", "pandas", "arrow-table"])
@pytest.mark.parametrize(
    "destination_config",
    LANCE_DEST_CONFS,
    ids=lambda x: x.name,
)
def test_new_column_in_second_load(
    destination_config: DestinationTestConfiguration, object_format: TestDataItemFormat
) -> None:
    """Test that new columns in source are added to the target."""
    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_new_column_in_second_load",
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
        client = cast(TLanceDestinationClient, client)
        tbl = open_lance_table(client, "all_types_table")

        # Get the actual table schema from the destination
        actual_columns = set(tbl.schema.names)

        # Check that the decimal column was added
        assert "decimal" in actual_columns, (
            "Expected 'decimal' column to be present in destination table. Actual columns:"
            f" {actual_columns}"
        )


@pytest.mark.parametrize(
    "destination_config",
    LANCE_DEST_CONFS,
    ids=lambda x: x.name,
)
def test_arrow_precision_types(destination_config: DestinationTestConfiguration):
    # create a table with all those types as columns
    import numpy as np

    table = pa.table(
        {
            "int8": pa.array([1, 2, 3], pa.int8()),
            "int16": pa.array([1, 2, 3], pa.int16()),
            "int32": pa.array([1, 2, 3], pa.int32()),
            "int64": pa.array([1, 2, 3], pa.int64()),
            "uint8": pa.array([1, 2, 3], pa.uint8()),
            "uint16": pa.array([1, 2, 3], pa.uint16()),
            "uint32": pa.array([1, 2, 3], pa.uint32()),
            "uint64": pa.array([1, 2, 3], pa.uint64()),
            "float16": pa.array([np.float16(1), np.float16(2), np.float16(3)], pa.float16()),
            "float32": pa.array([np.float32(1), np.float32(2), np.float32(3)], pa.float32()),
            "float64": pa.array([np.float64(1), np.float64(2), np.float64(3)], pa.float64()),
            "high_decimal_precision": pa.array(
                [Decimal("9" * 20 + "." + "9" * 18) for _ in range(3)], pa.decimal128(38, 18)
            ),
        }
    )

    # now run a pipeline with this table
    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_arrow_precision_types",
        dev_mode=True,
    )

    @dlt.resource(
        table_name="all_precision_types",
        primary_key="int8",
    )
    def identity_resource(data: pa.Table) -> Generator[pa.Table, None, None]:
        yield data

    info = pipeline.run(identity_resource(table))
    assert_load_info(info)

    # check the types of the column in the destination table and in the pipeline schema
    with pipeline.destination_client() as client:
        client = cast(TLanceDestinationClient, client)
        tbl = open_lance_table(client, "all_precision_types")

        # Check that all original types are preserved in the destination
        expected_types = [
            pa.int8(),
            pa.int16(),
            pa.int32(),
            pa.int64(),
            pa.int8(),  # uint8 -> int8
            pa.int16(),  # uint16 -> int16
            pa.int32(),  # uint32 -> int32
            pa.int64(),  # uint64 -> int64
            pa.float64(),  # float16 -> float64
            pa.float64(),  # float32 -> float64
            pa.float64(),  # float64 -> float64
            pa.decimal128(38, 18),  # high_decimal_precision
        ]

        for i, (expected_type, actual_type) in enumerate(zip(expected_types, tbl.schema.types)):
            assert (
                expected_type == actual_type
            ), f"Column {tbl.schema.names[i]}: expected {expected_type}, got {actual_type}"


@pytest.mark.parametrize("remove_orphans", [True, False])
@pytest.mark.parametrize("object_format", ["object", "pandas", "arrow-table"])
@pytest.mark.parametrize(
    "destination_config",
    LANCE_DEST_CONFS,
    ids=lambda x: x.name,
)
def test_missing_column_in_second_load(
    destination_config: DestinationTestConfiguration,
    object_format: TestDataItemFormat,
    remove_orphans: bool,
) -> None:
    """
    Test if same data is loaded with missing column and merge stragegy is present, column
    is removed from lancedb table.
    """
    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_missing_column_in_second_load",
        dev_mode=True,
    )

    @dlt.resource(
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        table_name="all_types_table",
        primary_key="int",
        merge_key="int",
    )
    def identity_resource_with_orphan_removal(data: pa.Table) -> Generator[pa.Table, None, None]:
        yield data

    @dlt.resource(
        write_disposition={"disposition": "append"},
        table_name="all_types_table",
        primary_key="int",
    )
    def identity_resource_without_orphan_removal(data: pa.Table) -> Generator[pa.Table, None, None]:
        yield data

    get_adapter(destination_config)(identity_resource_with_orphan_removal, remove_orphans=True)

    resource = (
        identity_resource_with_orphan_removal
        if remove_orphans
        else identity_resource_without_orphan_removal
    )

    table, _, _ = arrow_table_all_data_types(
        object_format=object_format,
        include_null=False,
        include_json=False,
        num_rows=1,
    )
    # remove columns string_null and float_null
    table = remove_column_from_data(object_format, table, "string_null")
    table = remove_column_from_data(object_format, table, "float_null")

    # do a first run to establish schema
    if (
        remove_orphans
        and object_format in ["arrow-table", "pandas"]
        and destination_config.destination_type == "lancedb"
    ):
        with pytest.raises(PipelineStepFailed) as e:
            info = pipeline.run(resource(table))
        assert "_dlt_load_id` column is required" in str(e.value)
        # with this setting it should work
        os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID"] = "true"
        os.environ["NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_ID"] = "true"
        pipeline.drop()
    info = pipeline.run(resource(table))
    assert_load_info(info)

    # Remove a column from the data
    removed_column = "bool"
    next_table = remove_column_from_data(object_format, table, removed_column)

    # second load, data with same id, but one less column
    info = pipeline.run(resource(next_table))
    assert_load_info(info)

    # no other thing to test here on the schema is as the column will stay in the schema, correct?


# @pytest.mark.xfail(reason="normalizer issue?")
@pytest.mark.parametrize(
    "destination_config",
    LANCE_DEST_CONFS,
    ids=lambda x: x.name,
)
def test_json_nesting_evolution(destination_config: DestinationTestConfiguration) -> None:
    """Test that json nesting evolution is handled correctly."""
    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_json_nesting_evolution",
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
        client = cast(TLanceDestinationClient, client)
        tbl = open_lance_table(client, "nesting_table")
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
        client = cast(TLanceDestinationClient, client)
        tbl = open_lance_table(client, "nesting_table")
        assert "json__b__c__c1" in tbl.schema.names
        assert "json__b__d" in tbl.schema.names
        assert "json__b__c" in tbl.schema.names


@pytest.mark.parametrize(
    "destination_config",
    LANCE_DEST_CONFS,
    ids=lambda x: x.name,
)
def test_column_added_between_job_files(
    destination_config: DestinationTestConfiguration, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A column that first appears part way through a load survives the merge.

    The writer rotates the job file when the column count grows, so the load writes one file
    without the column and one with it, and the merge must read them as one payload.
    """
    # the writer only rotates once the buffer has flushed, so a small buffer splits the table
    monkeypatch.setenv("DATA_WRITER__BUFFER_MAX_ITEMS", "2")

    # `os.scandir` does not order its results: pin write order so the file lacking the column
    # is read first, which is the order that used to drop it
    original_list = FileStorage.list_folder_files

    def oldest_first(self: FileStorage, relative_path: str, to_root: bool = True) -> List[str]:
        def modified_at(file_path: str) -> float:
            # `to_root=False` returns bare names, which do not resolve on their own
            if not to_root:
                file_path = os.path.join(relative_path, file_path)
            return os.path.getmtime(self.make_full_path(file_path))

        return sorted(original_list(self, relative_path, to_root), key=modified_at)

    monkeypatch.setattr(FileStorage, "list_folder_files", oldest_first)

    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_column_added_between_job_files", dev_mode=True
    )

    @dlt.resource(
        table_name="doc",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        primary_key="id",
    )
    def growing_resource() -> Generator[DictStrAny, None, None]:
        for i in range(4):
            yield {"id": i, "a": f"a{i}"}
        for i in range(4, 8):
            yield {"id": i, "a": f"a{i}", "b": f"b{i}"}

    info = pipeline.run(growing_resource())
    assert_load_info(info)

    # the premise of the test: the load wrote the table as more than one job file
    parquet_jobs = [
        job
        for job in info.load_packages[0].jobs["completed_jobs"]
        if job.job_file_info.table_name == "doc" and job.job_file_info.file_format == "parquet"
    ]
    assert len(parquet_jobs) > 1

    with pipeline.destination_client() as client:
        client = cast(TLanceDestinationClient, client)
        rows = read_arrow_table(open_lance_table(client, "doc")).to_pylist()

    assert {row["id"]: row["b"] for row in rows} == {
        0: None,
        1: None,
        2: None,
        3: None,
        4: "b4",
        5: "b5",
        6: "b6",
        7: "b7",
    }
