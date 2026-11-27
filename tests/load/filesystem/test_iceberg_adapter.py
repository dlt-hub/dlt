import pytest

import dlt
import pyarrow as pa
from dlt.common.schema.typing import TTableSchema
from dlt.destinations.adapters import iceberg_adapter, iceberg_partition
from dlt.destinations.impl.filesystem.iceberg_adapter import (
    PARTITION_HINT,
    TABLE_PROPERTIES_HINT,
    PartitionSpec,
    parse_partition_hints,
    create_identity_specs,
    get_partition_specs,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "partition_input,expected_column",
    [
        (["region"], "region"),
        ("category", "category"),
    ],
    ids=["list_input", "string_input"],
)
def test_iceberg_adapter_identity_partition(partition_input, expected_column) -> None:
    @dlt.resource
    def my_data():
        yield [{"id": 1, "region": "US", "category": "A"}]

    resource = iceberg_adapter(my_data, partition=partition_input)

    table_schema = resource.compute_table_schema()
    partition_hints = table_schema.get(PARTITION_HINT, [])

    assert partition_hints == [{"transform": "identity", "source_column": expected_column}]


@pytest.mark.parametrize(
    "transform_type",
    ["year", "month", "day", "hour"],
)
def test_iceberg_adapter_temporal_partition(transform_type) -> None:
    @dlt.resource
    def events():
        yield [{"timestamp": "2024-03-15T10:30:45Z"}]

    factory = getattr(iceberg_partition, transform_type)
    resource = iceberg_adapter(events, partition=[factory("timestamp")])
    table_schema = resource.compute_table_schema()
    partition_hints = table_schema.get(PARTITION_HINT, [])

    assert partition_hints == [{"transform": transform_type, "source_column": "timestamp"}]


@pytest.mark.parametrize(
    "transform_type,param_value,column_name",
    [
        ("bucket", 16, "user_id"),
        ("truncate", 4, "name"),
    ],
    ids=["bucket", "truncate"],
)
def test_iceberg_adapter_parameterized_partition(transform_type, param_value, column_name) -> None:
    @dlt.resource
    def data():
        yield [{"user_id": "123", "name": "example_string"}]

    factory = getattr(iceberg_partition, transform_type)
    resource = iceberg_adapter(data, partition=[factory(param_value, column_name)])

    table_schema = resource.compute_table_schema()
    partition_hints = table_schema.get(PARTITION_HINT, [])

    assert partition_hints == [
        {"transform": transform_type, "source_column": column_name, "param_value": param_value}
    ]


def test_iceberg_adapter_multiple_partitions() -> None:
    @dlt.resource
    def sales_data():
        yield [
            {
                "id": 1,
                "timestamp": "2024-01-15T10:30:00Z",
                "region": "US",
                "amount": 1250.00,
            }
        ]

    resource = iceberg_adapter(
        sales_data,
        partition=[iceberg_partition.month("timestamp"), "region"],
    )

    table_schema = resource.compute_table_schema()
    partition_hints = table_schema.get(PARTITION_HINT, [])

    assert partition_hints == [
        {"transform": "month", "source_column": "timestamp"},
        {"transform": "identity", "source_column": "region"},
    ]


def test_iceberg_adapter_custom_partition_field_name() -> None:
    @dlt.resource
    def activity():
        yield [{"user_id": 123, "activity_time": "2024-01-01T00:00:00Z"}]

    resource = iceberg_adapter(
        activity,
        partition=[
            iceberg_partition.year("activity_time", "activity_year"),
            iceberg_partition.bucket(8, "user_id", "user_bucket"),
        ],
    )

    table_schema = resource.compute_table_schema()
    partition_hints = table_schema.get(PARTITION_HINT, [])

    assert partition_hints == [
        {"transform": "year", "source_column": "activity_time", "partition_field": "activity_year"},
        {
            "transform": "bucket",
            "source_column": "user_id",
            "param_value": 8,
            "partition_field": "user_bucket",
        },
    ]


def test_iceberg_adapter_no_args_raises() -> None:
    @dlt.resource
    def my_data():
        yield [{"id": 1}]

    with pytest.raises(
        ValueError, match="At least one of `partition` or `table_properties` must be specified"
    ):
        iceberg_adapter(my_data)


def test_iceberg_adapter_table_properties() -> None:
    @dlt.resource
    def my_data():
        yield [{"id": 1, "region": "US"}]

    resource = iceberg_adapter(
        my_data,
        partition="region",
        table_properties={"write.format.default": "parquet"},
    )

    table_schema = resource.compute_table_schema()
    assert table_schema.get(PARTITION_HINT) == [
        {"transform": "identity", "source_column": "region"}
    ]
    assert table_schema.get(TABLE_PROPERTIES_HINT) == {"write.format.default": "parquet"}


def test_iceberg_adapter_table_properties_only() -> None:
    @dlt.resource
    def my_data():
        yield [{"id": 1}]

    resource = iceberg_adapter(
        my_data,
        table_properties={"write.target-file-size-bytes": "536870912"},
    )

    table_schema = resource.compute_table_schema()
    assert table_schema.get(PARTITION_HINT) is None
    assert table_schema.get(TABLE_PROPERTIES_HINT) == {"write.target-file-size-bytes": "536870912"}


@pytest.mark.parametrize(
    "bad_properties,match",
    [
        ("not_a_dict", "`table_properties` must be a dictionary"),
        ({1: "val"}, "Table property keys must be strings"),
        ({"key": 123}, "Table property values must be strings"),
    ],
    ids=["not_dict", "non_string_key", "non_string_value"],
)
def test_iceberg_adapter_invalid_table_properties(bad_properties, match) -> None:
    @dlt.resource
    def my_data():
        yield [{"id": 1}]

    with pytest.raises(ValueError, match=match):
        iceberg_adapter(my_data, table_properties=bad_properties)


def test_iceberg_adapter_with_raw_data() -> None:
    """Test iceberg_adapter with raw data (not a DltResource)."""

    data = [{"id": 1, "category": "A"}, {"id": 2, "category": "B"}]

    resource = iceberg_adapter(data, partition=["category"])

    table_schema = resource.compute_table_schema()
    partition_hints = table_schema.get(PARTITION_HINT, [])

    assert partition_hints == [{"transform": "identity", "source_column": "category"}]


def test_iceberg_adapter_complex_partitioning() -> None:
    @dlt.resource
    def orders():
        yield [
            {
                "order_id": "ord_001",
                "status": "completed",
                "order_date": "2024-01-15",
                "customer_id": "cust_123",
            }
        ]

    resource = iceberg_adapter(
        orders,
        partition=[
            iceberg_partition.year("order_date", "yearly_partition"),
            "status",
            iceberg_partition.bucket(32, "customer_id", "customer_bucket"),
        ],
    )

    table_schema = resource.compute_table_schema()
    partition_hints = table_schema.get(PARTITION_HINT, [])

    assert partition_hints == [
        {"transform": "year", "source_column": "order_date", "partition_field": "yearly_partition"},
        {"transform": "identity", "source_column": "status"},
        {
            "transform": "bucket",
            "source_column": "customer_id",
            "param_value": 32,
            "partition_field": "customer_bucket",
        },
    ]


@pytest.mark.parametrize(
    "table_schema,expected_specs",
    [
        (
            {"name": "test_table", "columns": {"id": {"name": "id", "data_type": "bigint"}}},
            [],
        ),
        (
            {
                "name": "test_table",
                PARTITION_HINT: [
                    {"transform": "year", "source_column": "created_at"},
                    {"transform": "identity", "source_column": "region"},
                ],
            },
            [("year", "created_at"), ("identity", "region")],
        ),
    ],
    ids=["empty", "with_specs"],
)
def test_parse_partition_hints(table_schema, expected_specs) -> None:
    specs = parse_partition_hints(table_schema)
    assert len(specs) == len(expected_specs)
    for spec, (expected_transform, expected_column) in zip(specs, expected_specs):
        assert spec.transform == expected_transform
        assert spec.source_column == expected_column


def test_create_identity_specs() -> None:
    column_names = ["region", "category", "status"]
    specs = create_identity_specs(column_names)

    assert len(specs) == 3
    for i, col_name in enumerate(column_names):
        assert specs[i].source_column == col_name
        assert specs[i].transform == "identity"


@pytest.mark.parametrize(
    "source_column,transform,partition_field,param_value",
    [
        ("created_at", "year", "created_year", None),
        ("created_at", "month", "month_partition", None),
        ("user_id", "bucket", "user_bucket", 32),
    ],
    ids=["year", "month", "bucket"],
)
def test_partition_spec_roundtrip(source_column, transform, partition_field, param_value) -> None:
    spec = PartitionSpec(
        source_column=source_column,
        transform=transform,
        partition_field=partition_field,
        param_value=param_value,
    )

    assert spec.source_column == source_column
    assert spec.transform == transform
    assert spec.partition_field == partition_field
    assert spec.param_value == param_value

    spec_dict = spec.to_dict()
    assert spec_dict["source_column"] == source_column
    assert spec_dict["transform"] == transform
    assert spec_dict["partition_field"] == partition_field

    restored_spec = PartitionSpec.from_dict(spec_dict)
    assert restored_spec.source_column == source_column
    assert restored_spec.transform == transform
    assert restored_spec.partition_field == partition_field
    assert restored_spec.param_value == param_value


def test_partition_spec_get_transform() -> None:
    from pyiceberg.transforms import IdentityTransform

    spec = PartitionSpec(source_column="region", transform="identity")

    assert isinstance(spec.get_transform(), IdentityTransform)


def test_partition_spec_invalid_transform() -> None:
    spec = PartitionSpec(source_column="col", transform="invalid_transform")

    with pytest.raises(ValueError, match="Unknown partition transformation type"):
        spec.get_transform()


def test_duplicate_partition_column_in_legacy_and_hints() -> None:
    from dlt.common.schema.exceptions import SchemaCorruptedException

    table_schema: TTableSchema = {
        "name": "test_table",
        "columns": {
            # Partition defined in columns
            "region": {"name": "region", "data_type": "text", "partition": True},
            "id": {"name": "id", "data_type": "bigint"},
        },
        PARTITION_HINT: [  # type: ignore[misc]
            {"transform": "identity", "source_column": "region"},
        ],
        "write_disposition": "append",
    }

    with pytest.raises(SchemaCorruptedException) as exc_info:
        get_partition_specs(table_schema, "test_schema")

    assert "region" in str(exc_info.value)
    assert "defined both as a partition column" in str(exc_info.value)

    # partition columns go first, then partition hints
    table_schema[PARTITION_HINT] = [  # type: ignore[literal-required]
        {"transform": "day", "source_column": "created_at"}
    ]
    assert get_partition_specs(table_schema, "test_schema") == [
        iceberg_partition.identity("region"),
        iceberg_partition.day("created_at"),
    ]
