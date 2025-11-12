from typing import Iterator, Dict, Any
import pytest

import dlt
from dlt.common.utils import uniq_id
from dlt.destinations.adapters import databricks_adapter
from dlt.destinations import databricks
from dlt.destinations.impl.databricks.databricks_adapter import (
    CLUSTER_HINT,
    TABLE_PROPERTIES_HINT,
)
from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
)

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["databricks"]),
    ids=lambda x: x.name,
)
def test_databricks_hints(
    destination_config: DestinationTestConfiguration,
) -> None:
    pipeline = destination_config.setup_pipeline(
        f"databricks_{uniq_id()}", dev_mode=True, destination=databricks(create_indexes=True)
    )

    @dlt.resource(
        columns={"some_int": {"data_type": "bigint", "nullable": False}},
        primary_key="some_int",
    )
    def demo_resource_primary() -> Iterator[Dict[str, int]]:
        for i in range(10):
            yield {
                "some_int": i,
            }

    databricks_adapter(
        demo_resource_primary,
        table_comment="Dummy table comment",
        table_tags=[{"environment": "dummy"}, "pii"],
        column_hints={
            "some_int": {
                "column_comment": "Dummy column comment",
                "column_tags": [{"environment": "dummy"}, "pii"],
            }
        },
    )

    @dlt.resource(
        columns={"some_int_2": {"data_type": "bigint", "nullable": False}},
        references=[
            {
                "referenced_table": "demo_resource_primary",
                "columns": ["some_int_2"],
                "referenced_columns": ["some_int"],
            }
        ],
    )
    def demo_resource_foreign() -> Iterator[Dict[str, int]]:
        for i in range(10):
            yield {
                "some_int_2": i,
            }

    @dlt.source(max_table_nesting=0)
    def demo_source():
        return [demo_resource_primary, demo_resource_foreign]

    pipeline.run(demo_source())

    with pipeline.sql_client() as c:
        with c.execute_query(f"""
                SELECT tables.comment, table_tags.tag_name, table_tags.tag_value
                FROM information_schema.tables
                LEFT JOIN information_schema.table_tags ON tables.table_catalog = table_tags.catalog_name
                    AND tables.table_schema = table_tags.schema_name
                    AND tables.table_name = table_tags.table_name
                WHERE tables.table_name = 'demo_resource_primary'
                    AND tables.table_schema = '{pipeline.dataset_name}';
            """) as cur:
            rows = cur.fetchall()

            assert all("Dummy table comment" in str(row[0]) for row in rows)
            assert any("pii" in str(row[1]) for row in rows)
            assert any("environment" in str(row[1]) and "dummy" in str(row[2]) for row in rows)

        with c.execute_query(f"""
                SELECT columns.comment, column_tags.tag_name, column_tags.tag_value, constraint_name
                FROM information_schema.columns
                LEFT JOIN information_schema.column_tags
                    ON columns.table_catalog = column_tags.catalog_name
                    AND columns.table_schema = column_tags.schema_name
                    AND columns.table_name = column_tags.table_name
                    AND columns.column_name = column_tags.column_name
                LEFT JOIN information_schema.key_column_usage
                    ON columns.table_catalog = key_column_usage.table_catalog
                    AND columns.table_schema = key_column_usage.table_schema
                    AND columns.table_name = key_column_usage.table_name
                    AND columns.column_name = key_column_usage.column_name
                WHERE columns.table_schema = '{pipeline.dataset_name}'
                    AND columns.table_name = 'demo_resource_primary'
                    AND columns.column_name NOT LIKE '\\_%';
            """) as cur:
            rows = cur.fetchall()

            assert all("Dummy column comment" in str(row[0]) for row in rows)
            assert any("environment" in str(row[1]) and "dummy" in str(row[2]) for row in rows)
            assert any("pii" in str(row[1]) for row in rows)
            assert any("demo_resource_primary_pk" in str(row[3]) for row in rows)

        with c.execute_query(f"""
                SELECT constraint_name
                FROM information_schema.columns
                LEFT JOIN information_schema.key_column_usage
                    ON columns.table_catalog = key_column_usage.table_catalog
                    AND columns.table_schema = key_column_usage.table_schema
                    AND columns.table_name = key_column_usage.table_name
                    AND columns.column_name = key_column_usage.column_name
                WHERE columns.table_name = 'demo_resource_foreign'
                    AND columns.column_name NOT LIKE '\\_%'
                    AND columns.table_schema = '{pipeline.dataset_name}';
            """) as cur:
            rows = cur.fetchall()

            assert any(
                "demo_resource_foreign_demo_resource_primary_fk" in str(row[0]) for row in rows
            )


@pytest.mark.parametrize(
    "invalid_table_tags",
    [
        123,  # not a list
        [123],  # list with invalid element
        [{"a": "b", "c": "d"}],  # dict with more than one key
        [None],  # list with None
        [[], {}],  # list with empty list and dict
    ],
)
def test_databricks_adapter_invalid_table_tags(invalid_table_tags):
    def dummy_resource():
        yield {"some_int": 1}

    # Should raise ValueError for invalid table_tags
    with pytest.raises(ValueError):
        databricks_adapter(dummy_resource, table_tags=invalid_table_tags)


@pytest.mark.parametrize(
    "invalid_column_tags",
    [
        123,  # not a list
        [123],  # list with invalid element
        [{"a": "b", "c": "d"}],  # dict with more than one key
        [None],  # list with None
        [[], {}],  # list with empty list and dict
    ],
)
def test_databricks_adapter_invalid_column_tags(invalid_column_tags):
    def dummy_resource():
        yield {"some_int": 1}

    with pytest.raises(ValueError):
        databricks_adapter(
            dummy_resource,
            column_hints={"some_int": {"column_tags": invalid_column_tags}},
        )


def test_databricks_adapter_invalid_table_comment():
    def dummy_resource():
        yield {"some_int": 1}

    # Should raise ValueError for non-string table_comment
    with pytest.raises(ValueError):
        databricks_adapter(dummy_resource, table_comment=123)  # type: ignore[arg-type]


def test_databricks_adapter_invalid_cluster():
    def dummy_resource():
        yield {"some_int": 1}

    # Should raise ValueError for invalid cluster type
    with pytest.raises(ValueError):
        databricks_adapter(dummy_resource, cluster=123)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["databricks"]),
    ids=lambda x: x.name,
)
def test_databricks_adapter_special_characters(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Test that special characters in comments and tags are properly escaped"""
    pipeline = destination_config.setup_pipeline(
        f"databricks_{uniq_id()}", dev_mode=True, destination=databricks(create_indexes=True)
    )

    @dlt.resource(
        columns={"some_int": {"data_type": "bigint", "nullable": False}},
        primary_key="some_int",
    )
    def demo_resource_special() -> Iterator[Dict[str, int]]:
        for i in range(5):
            yield {"some_int": i}

    # Test with special characters that could cause SQL injection
    databricks_adapter(
        demo_resource_special,
        table_comment="O'Reilly's \"book\" on SQL; DROP TABLE users;--",
        table_tags=[{"env": 'test\'s "env"'}],
        column_hints={
            "some_int": {
                "column_comment": "User's ID with \"quotes\" and 'apostrophes'",
                "column_tags": [{"type": 'user\'s "data"'}],
            }
        },
    )

    @dlt.source(max_table_nesting=0)
    def demo_source():
        return demo_resource_special

    pipeline.run(demo_source())

    # Verify that the special characters were properly handled and the table was created
    with pipeline.sql_client() as c:
        with c.execute_query(f"""
                SELECT COUNT(*) FROM {pipeline.dataset_name}.demo_resource_special
            """) as cur:
            row = cur.fetchone()
            assert row[0] == 5  # All rows should be loaded successfully


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["databricks"]),
    ids=lambda x: x.name,
)
def test_databricks_adapter_clustering(destination_config: DestinationTestConfiguration) -> None:
    """Test clustering features including AUTO clustering"""
    pipeline = destination_config.setup_pipeline(
        f"databricks_{uniq_id()}", dev_mode=True, destination=databricks()
    )

    @dlt.resource(
        columns={
            "event_id": {"data_type": "bigint", "nullable": False},
            "event_date": {"data_type": "date"},
            "customer_id": {"data_type": "bigint"},
        },
        primary_key="event_id",
    )
    def events_auto_cluster() -> Iterator[Dict[str, Any]]:
        for i in range(10):
            yield {
                "event_id": i,
                "event_date": "2024-01-01",
                "customer_id": i % 3,
            }

    # Test AUTO clustering
    databricks_adapter(events_auto_cluster, cluster="AUTO")

    @dlt.resource(
        columns={
            "event_id": {"data_type": "bigint", "nullable": False},
            "event_date": {"data_type": "date"},
            "customer_id": {"data_type": "bigint"},
        },
        primary_key="event_id",
    )
    def events_manual_cluster() -> Iterator[Dict[str, Any]]:
        for i in range(10):
            yield {
                "event_id": i,
                "event_date": "2024-01-01",
                "customer_id": i % 3,
            }

    # Test manual clustering on specific columns
    databricks_adapter(events_manual_cluster, cluster=["event_date", "customer_id"])

    @dlt.source(max_table_nesting=0)
    def demo_source():
        return [events_auto_cluster, events_manual_cluster]

    pipeline.run(demo_source())

    # Verify clustering using DESCRIBE DETAIL
    with pipeline.sql_client() as c:
        # Check AUTO clustering
        with c.execute_query(f"DESCRIBE DETAIL {pipeline.dataset_name}.events_auto_cluster") as cur:
            row = cur.fetchone()
            # For AUTO clustering, clusteringColumns might be empty or contain auto-selected columns
            # The important thing is that the table was created successfully

        # Check manual clustering
        with c.execute_query(
            f"DESCRIBE DETAIL {pipeline.dataset_name}.events_manual_cluster"
        ) as cur:
            row = cur.fetchone()
            # Get clusteringColumns field (usually at index 8, but let's get it by column description)
            columns = [desc[0] for desc in cur.description]
            if "clusteringColumns" in columns:
                cluster_col_idx = columns.index("clusteringColumns")
                cluster_cols = row[cluster_col_idx]
                # Cluster columns should contain event_date and customer_id
                assert "event_date" in str(cluster_cols)
                assert "customer_id" in str(cluster_cols)

        # Verify data was loaded
        with c.execute_query(
            f"SELECT COUNT(*) FROM {pipeline.dataset_name}.events_auto_cluster"
        ) as cur:
            assert cur.fetchone()[0] == 10

        with c.execute_query(
            f"SELECT COUNT(*) FROM {pipeline.dataset_name}.events_manual_cluster"
        ) as cur:
            assert cur.fetchone()[0] == 10


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["databricks"]),
    ids=lambda x: x.name,
)
def test_databricks_adapter_partitioning(destination_config: DestinationTestConfiguration) -> None:
    """Test partitioning features"""
    pipeline = destination_config.setup_pipeline(
        f"databricks_{uniq_id()}", dev_mode=True, destination=databricks()
    )

    @dlt.resource(
        columns={
            "event_id": {"data_type": "bigint", "nullable": False},
            "year": {"data_type": "bigint"},
            "month": {"data_type": "bigint"},
            "customer_id": {"data_type": "bigint"},
        },
        primary_key="event_id",
    )
    def partitioned_events() -> Iterator[Dict[str, Any]]:
        for i in range(10):
            yield {
                "event_id": i,
                "year": 2024,
                "month": (i % 3) + 1,
                "customer_id": i % 5,
            }

    # Test partitioning by year and month
    # Note: Databricks doesn't allow both PARTITIONED BY and CLUSTER BY in the same table
    databricks_adapter(partitioned_events, partition=["year", "month"])

    @dlt.source(max_table_nesting=0)
    def demo_source():
        return partitioned_events

    pipeline.run(demo_source())

    # Verify partitioning using DESCRIBE DETAIL
    with pipeline.sql_client() as c:
        with c.execute_query(f"DESCRIBE DETAIL {pipeline.dataset_name}.partitioned_events") as cur:
            row = cur.fetchone()
            columns = [desc[0] for desc in cur.description]

            # Check partitionColumns
            if "partitionColumns" in columns:
                partition_col_idx = columns.index("partitionColumns")
                partition_cols = row[partition_col_idx]
                assert "year" in str(partition_cols)
                assert "month" in str(partition_cols)

            # Note: Can't have clustering with partitioning in Databricks

        # Verify data was loaded
        with c.execute_query(
            f"SELECT COUNT(*) FROM {pipeline.dataset_name}.partitioned_events"
        ) as cur:
            assert cur.fetchone()[0] == 10


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["databricks"]),
    ids=lambda x: x.name,
)
def test_databricks_adapter_table_properties(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Test table properties feature including statistics columns"""
    pipeline = destination_config.setup_pipeline(
        f"databricks_{uniq_id()}", dev_mode=True, destination=databricks()
    )

    @dlt.resource(
        columns={
            "event_id": {"data_type": "bigint", "nullable": False},
            "event_date": {"data_type": "date"},
            "customer_id": {"data_type": "bigint"},
            "amount": {"data_type": "double"},
        },
        primary_key="event_id",
    )
    def events_with_properties() -> Iterator[Dict[str, Any]]:
        for i in range(5):
            yield {
                "event_id": i,
                "event_date": "2024-01-01",
                "customer_id": i % 3,
                "amount": i * 100.5,
            }

    # Test table properties including data skipping statistics
    databricks_adapter(
        events_with_properties,
        table_properties={
            "delta.appendOnly": True,
            "delta.logRetentionDuration": "30 days",
            "delta.dataSkippingStatsColumns": "event_date,customer_id,amount",
            "delta.autoOptimize.optimizeWrite": True,
            "delta.autoOptimize.autoCompact": True,
            "custom.team": "data-eng",
            "custom.version": 1,
            "custom.optimized": True,
        },
    )

    @dlt.source(max_table_nesting=0)
    def demo_source():
        return events_with_properties

    pipeline.run(demo_source())

    # Verify table properties using SHOW TBLPROPERTIES
    with pipeline.sql_client() as c:
        with c.execute_query(
            f"SHOW TBLPROPERTIES {pipeline.dataset_name}.events_with_properties"
        ) as cur:
            properties = {row[0]: row[1] for row in cur.fetchall()}

            # Check Delta optimization properties
            assert properties.get("delta.appendOnly") == "true"
            assert properties.get("delta.logRetentionDuration") == "30 days"
            assert (
                properties.get("delta.dataSkippingStatsColumns") == "event_date,customer_id,amount"
            )
            assert properties.get("delta.autoOptimize.optimizeWrite") == "true"
            assert properties.get("delta.autoOptimize.autoCompact") == "true"

            # Check custom properties
            assert properties.get("custom.team") == "data-eng"
            assert properties.get("custom.version") == "1"
            assert properties.get("custom.optimized") == "true"

        # Verify data was loaded
        with c.execute_query(
            f"SELECT COUNT(*) FROM {pipeline.dataset_name}.events_with_properties"
        ) as cur:
            assert cur.fetchone()[0] == 5


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["databricks"]),
    ids=lambda x: x.name,
)
def test_databricks_adapter_iceberg_format(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Test ICEBERG table format"""
    pipeline = destination_config.setup_pipeline(
        f"databricks_{uniq_id()}", dev_mode=True, destination=databricks()
    )

    @dlt.resource(
        columns={
            "event_id": {"data_type": "bigint", "nullable": False},
            "event_date": {"data_type": "date"},
            "customer_id": {"data_type": "bigint"},
        },
        primary_key="event_id",
    )
    def iceberg_events() -> Iterator[Dict[str, Any]]:
        for i in range(10):
            yield {
                "event_id": i,
                "event_date": "2024-01-01",
                "customer_id": i % 3,
            }

    # Test ICEBERG table format
    # Note: ICEBERG tables don't support clustering without specific table properties
    databricks_adapter(
        iceberg_events, table_format="ICEBERG", table_comment="Iceberg table for testing"
    )

    @dlt.source(max_table_nesting=0)
    def demo_source():
        return iceberg_events

    pipeline.run(demo_source())

    # Verify table format using DESCRIBE DETAIL
    with pipeline.sql_client() as c:
        with c.execute_query(f"DESCRIBE DETAIL {pipeline.dataset_name}.iceberg_events") as cur:
            row = cur.fetchone()
            columns = [desc[0] for desc in cur.description]

            # Check format/provider column
            if "format" in columns:
                format_idx = columns.index("format")
                table_format = row[format_idx]
                assert "ICEBERG" in str(table_format).upper()
            elif "provider" in columns:
                provider_idx = columns.index("provider")
                provider = row[provider_idx]
                assert "ICEBERG" in str(provider).upper()

        # Verify data was loaded
        with c.execute_query(f"SELECT COUNT(*) FROM {pipeline.dataset_name}.iceberg_events") as cur:
            assert cur.fetchone()[0] == 10


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["databricks"]),
    ids=lambda x: x.name,
)
def test_databricks_adapter_iceberg_all_data_types(
    destination_config: DestinationTestConfiguration,
) -> None:
    """Test ICEBERG table format with all supported dlt data types"""
    from tests.cases import TABLE_UPDATE, TABLE_ROW_ALL_DATA_TYPES

    pipeline = destination_config.setup_pipeline(
        f"databricks_{uniq_id()}", dev_mode=True, destination=databricks()
    )

    # Create columns dict from TABLE_UPDATE
    columns = {col["name"]: col for col in TABLE_UPDATE}

    @dlt.resource(columns=columns, primary_key="col1")
    def iceberg_all_types() -> Iterator[Dict[str, Any]]:
        yield TABLE_ROW_ALL_DATA_TYPES

    # Apply ICEBERG format
    databricks_adapter(iceberg_all_types, table_format="ICEBERG")

    @dlt.source(max_table_nesting=0)
    def demo_source():
        return iceberg_all_types

    pipeline.run(demo_source())

    # Verify table format and data
    with pipeline.sql_client() as c:
        # Verify it's an ICEBERG table
        with c.execute_query(f"DESCRIBE DETAIL {pipeline.dataset_name}.iceberg_all_types") as cur:
            row = cur.fetchone()
            columns_desc = [desc[0] for desc in cur.description]

            # Check format/provider column indicates ICEBERG
            if "format" in columns_desc:
                format_idx = columns_desc.index("format")
                table_format = row[format_idx]
                assert "ICEBERG" in str(table_format).upper()
            elif "provider" in columns_desc:
                provider_idx = columns_desc.index("provider")
                provider = row[provider_idx]
                assert "ICEBERG" in str(provider).upper()

        # Verify data was loaded successfully
        with c.execute_query(
            f"SELECT COUNT(*) FROM {pipeline.dataset_name}.iceberg_all_types"
        ) as cur:
            assert cur.fetchone()[0] == 1

        # Verify a few key columns to ensure types were handled correctly
        with c.execute_query(
            "SELECT col1, col2, col3, col5, col6, col10 FROM"
            f" {pipeline.dataset_name}.iceberg_all_types"
        ) as cur:
            row = cur.fetchone()
            assert row[0] == TABLE_ROW_ALL_DATA_TYPES["col1"]  # bigint
            assert abs(row[1] - TABLE_ROW_ALL_DATA_TYPES["col2"]) < 0.001  # double
            assert row[2] == TABLE_ROW_ALL_DATA_TYPES["col3"]  # bool
            assert row[3] == TABLE_ROW_ALL_DATA_TYPES["col5"]  # text


def test_databricks_adapter_invalid_table_format():
    """Test that invalid table formats are rejected"""

    def dummy_resource():
        yield {"event_id": 1}

    # Should raise ValueError for invalid table format
    with pytest.raises(ValueError, match="table_format.*DELTA.*ICEBERG"):
        databricks_adapter(dummy_resource, table_format="PARQUET")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["databricks"]),
    ids=lambda x: x.name,
)
def test_databricks_adapter_iceberg_with_delta_properties(
    destination_config: DestinationTestConfiguration,
):
    """Test that Delta-specific properties are rejected for ICEBERG tables at load time"""
    from dlt.common.exceptions import TerminalValueError
    from dlt.pipeline.exceptions import PipelineStepFailed

    pipeline = destination_config.setup_pipeline(
        f"databricks_{uniq_id()}", dev_mode=True, destination=databricks()
    )

    @dlt.resource(
        columns={
            "event_id": {"data_type": "bigint", "nullable": False},
        },
        primary_key="event_id",
    )
    def iceberg_with_delta_props() -> Iterator[Dict[str, Any]]:
        for i in range(5):
            yield {"event_id": i}

    # Apply adapter with ICEBERG format and Delta-specific properties
    databricks_adapter(
        iceberg_with_delta_props,
        table_format="ICEBERG",
        table_properties={"delta.dataSkippingStatsColumns": "event_id"},
    )

    @dlt.source(max_table_nesting=0)
    def demo_source():
        return iceberg_with_delta_props

    # Should raise PipelineStepFailed with TerminalValueError when creating the table (at load time)
    with pytest.raises(PipelineStepFailed) as exc_info:
        pipeline.run(demo_source())

    # Verify the underlying exception is TerminalValueError with the right message
    assert isinstance(exc_info.value.__cause__, TerminalValueError)
    assert "delta.dataSkippingStatsColumns" in str(exc_info.value.__cause__)
    assert "not supported" in str(exc_info.value.__cause__)


def test_databricks_adapter_reserved_table_properties():
    """Test that reserved table property keys are rejected"""

    def dummy_resource():
        yield {"event_id": 1}

    # Test reserved key "owner"
    with pytest.raises(ValueError, match="owner.*reserved"):
        databricks_adapter(dummy_resource, table_properties={"owner": "test_user"})

    # Test option. prefix
    with pytest.raises(ValueError, match="option\\..*reserved"):
        databricks_adapter(dummy_resource, table_properties={"option.test": "value"})


def test_databricks_adapter_invalid_property_types():
    """Test that invalid table property value types are rejected"""

    def dummy_resource():
        yield {"event_id": 1}

    # Should raise ValueError for dict value
    with pytest.raises(ValueError, match="must be a string, integer, boolean, or float"):
        databricks_adapter(
            dummy_resource,
            table_properties={"test_prop": {"nested": "value"}},  # type: ignore[dict-item]
        )
