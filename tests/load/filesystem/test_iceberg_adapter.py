import pytest
from typing import Any, Dict

import dlt
import pyarrow as pa
from dlt.destinations.adapters import iceberg_adapter, iceberg_partition
from dlt.destinations.impl.filesystem.iceberg_adapter import (
    PARTITION_HINT,
    PartitionSpec,
    parse_partition_hints,
    create_identity_specs,
)
from dlt.destinations.impl.filesystem.iceberg_partition_spec import (
    build_iceberg_partition_spec,
    get_partition_transform,
    _default_field_name,
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


def test_iceberg_adapter_no_partition_raises() -> None:
    @dlt.resource
    def my_data():
        yield [{"id": 1}]

    with pytest.raises(ValueError, match="A value for `partition` must be specified"):
        iceberg_adapter(my_data, partition=None)


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


def test_build_iceberg_partition_spec() -> None:
    from pyiceberg.types import LongType, TimestampType, StringType
    from pyiceberg.transforms import YearTransform, IdentityTransform

    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("created_at", pa.timestamp("us")),
            pa.field("region", pa.string()),
        ]
    )

    partition_specs = [
        iceberg_partition.year("created_at"),
        iceberg_partition.identity("region"),
    ]

    iceberg_partition_spec, iceberg_schema = build_iceberg_partition_spec(
        arrow_schema, partition_specs
    )

    assert iceberg_partition_spec is not None
    assert len(iceberg_partition_spec.fields) == 2

    year_field = iceberg_partition_spec.fields[0]
    assert year_field.name == "created_at_year"
    assert isinstance(year_field.transform, YearTransform)
    assert year_field.source_id == iceberg_schema.find_field("created_at").field_id

    identity_field = iceberg_partition_spec.fields[1]
    assert identity_field.name == "region"
    assert isinstance(identity_field.transform, IdentityTransform)
    assert identity_field.source_id == iceberg_schema.find_field("region").field_id

    id_field = iceberg_schema.find_field("id")
    assert id_field.name == "id"
    assert isinstance(id_field.field_type, LongType)

    created_at_field = iceberg_schema.find_field("created_at")
    assert created_at_field.name == "created_at"
    assert isinstance(created_at_field.field_type, TimestampType)

    region_field = iceberg_schema.find_field("region")
    assert region_field.name == "region"
    assert isinstance(region_field.field_type, StringType)


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
    from pyiceberg.transforms import IdentityTransform, Transform

    spec = PartitionSpec(source_column="region", transform="identity")
    transform: Transform = get_partition_transform(spec)  # type: ignore[type-arg]

    assert isinstance(transform, IdentityTransform)


def test_partition_spec_invalid_transform() -> None:
    spec = PartitionSpec(source_column="col", transform="invalid_transform")

    with pytest.raises(ValueError, match="Unknown partition transformation type"):
        get_partition_transform(spec)


@pytest.mark.parametrize(
    "transform,source_column,param_value,expected_name",
    [
        ("identity", "region", None, "region"),
        ("year", "created_at", None, "created_at_year"),
        ("month", "event_time", None, "event_time_month"),
        ("day", "timestamp", None, "timestamp_day"),
        ("hour", "log_time", None, "log_time_hour"),
        ("bucket", "user_id", 16, "user_id_bucket_16"),
        ("truncate", "name", 4, "name_trunc_4"),
    ],
    ids=["identity", "year", "month", "day", "hour", "bucket", "truncate"],
)
def test_default_field_name_generation(
    transform: str, source_column: str, param_value: int, expected_name: str
) -> None:
    spec = PartitionSpec(
        source_column=source_column,
        transform=transform,
        param_value=param_value,
    )

    generated_name = _default_field_name(spec)

    assert generated_name == expected_name


def test_default_field_name_with_custom_partition_field() -> None:
    spec = PartitionSpec(
        source_column="created_at",
        transform="year",
        partition_field="custom_year_field",
    )

    generated_name = _default_field_name(spec)

    assert generated_name == "custom_year_field"


def test_duplicate_partition_column_in_legacy_and_hints() -> None:
    from dlt.common.schema.exceptions import SchemaCorruptedException
    from dlt.destinations.impl.filesystem.filesystem import IcebergLoadFilesystemJob
    from unittest.mock import MagicMock

    mock_load_table = {
        "name": "test_table",
        "columns": {
            # Partition defined in columns
            "region": {"name": "region", "data_type": "text", "partition": True},
            "id": {"name": "id", "data_type": "bigint"},
        },
        PARTITION_HINT: [
            {"transform": "identity", "source_column": "region"},
        ],
        "write_disposition": "append",
    }

    mock_job = MagicMock(spec=IcebergLoadFilesystemJob)
    mock_job._load_table = mock_load_table
    mock_job._partition_columns = ["region"]
    mock_job._schema = MagicMock()
    mock_job._schema.name = "test_schema"

    with pytest.raises(SchemaCorruptedException) as exc_info:
        IcebergLoadFilesystemJob._get_partition_spec_list(mock_job)

    assert "region" in str(exc_info.value)
    assert "defined both as a partition column" in str(exc_info.value)
