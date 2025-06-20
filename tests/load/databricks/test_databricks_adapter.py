from typing import Iterator, Dict
import pytest

import dlt
from dlt.common.utils import uniq_id
from dlt.destinations.adapters import databricks_adapter
from dlt.destinations.impl.databricks.databricks_adapter import (
    CLUSTER_HINT,
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
    pipeline = destination_config.setup_pipeline(f"databricks_{uniq_id()}", dev_mode=True)

    @dlt.resource(
        columns={"some_int": {"data_type": "bigint", "nullable": False}},
        primary_key="some_int",
    )
    def demo_resource_primary() -> Iterator[Dict[str, int]]:
        for i in range(10):
            yield {
                "some_int": i,
            }

    databricks_adapter(demo_resource_primary,
        table_comment="Dummy table comment",
        table_tags=[{"environment": "dummy"}, "pii"],
        column_hints={
            "some_int": {
                "column_comment": "Dummy column comment",
                "column_tags": [{"environment": "dummy"}, "pii"],
            }
        }
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

            assert any("demo_resource_foreign_demo_resource_primary_fk" in str(row[0]) for row in rows)