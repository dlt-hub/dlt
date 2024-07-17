import pytest

import dlt
from dlt.destinations import filesystem
from dlt.destinations.adapters import athena_adapter, athena_partition

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


def test_iceberg_partition_hints():
    """Create a table with athena partition hints and check that the SQL is generated correctly."""

    @dlt.resource(table_format="iceberg")
    def partitioned_table():
        yield {
            "product_id": 1,
            "name": "product 1",
            "created_at": "2021-01-01T00:00:00Z",
            "category": "category 1",
            "price": 100.0,
            "quantity": 10,
        }

    @dlt.resource(table_format="iceberg")
    def not_partitioned_table():
        yield {"a": 1, "b": 2}

    athena_adapter(
        partitioned_table,
        partition=[
            "category",
            athena_partition.month("created_at"),
            athena_partition.bucket(10, "product_id"),
            athena_partition.truncate(2, "name"),
        ],
    )

    pipeline = dlt.pipeline(
        "athena_test",
        destination="athena",
        staging=filesystem("s3://not-a-real-bucket"),
        dev_mode=True,
    )

    pipeline.extract([partitioned_table, not_partitioned_table])
    pipeline.normalize()

    with pipeline._sql_job_client(pipeline.default_schema) as client:
        sql_partitioned = client._get_table_update_sql(
            "partitioned_table",
            list(pipeline.default_schema.tables["partitioned_table"]["columns"].values()),
            False,
        )[0]
        sql_not_partitioned = client._get_table_update_sql(
            "not_partitioned_table",
            list(pipeline.default_schema.tables["not_partitioned_table"]["columns"].values()),
            False,
        )[0]

    # Partition clause is generated with original order
    expected_clause = (
        "PARTITIONED BY (`category`, month(`created_at`), bucket(10, `product_id`), truncate(2,"
        " `name`))"
    )
    assert expected_clause in sql_partitioned

    # No partition clause otherwise
    assert "PARTITIONED BY" not in sql_not_partitioned
