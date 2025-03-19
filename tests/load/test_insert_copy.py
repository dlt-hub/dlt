import pytest
import dlt
from typing import List

from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
)


DESTINATIONS: List[str] = [
    # NOTE: The following destinations
    # support simple INSERT INTO FROM expressions
    "duckdb",
    "athena",
    "bigquery",
    "clickhouse",
    "databricks",
    "motherduck",
    "redshift",
    "snowflake",
    "sqlalchemy",
    "mssql",
    "postgres",
    "dremio",
]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_execute_insert_query(destination_config: DestinationTestConfiguration) -> None:
    import sqlglot.expressions as sge

    # we will later insert everything from this table to the next one
    @dlt.resource(table_name="select_from_table")
    def select_from_table():
        yield from [{"id": i} for i in range(10, 20)]

    @dlt.resource(table_name="insert_into_table")
    def insert_into_table():
        yield from [{"id": i} for i in range(10)]

    pipeline = destination_config.setup_pipeline(
        "read_pipeline",
        dataset_name="read_test",
        dev_mode=True,
    )

    pipeline.run(select_from_table, loader_file_format=destination_config.file_format)
    pipeline.run(insert_into_table, loader_file_format=destination_config.file_format)

    dataset = pipeline.dataset()
    select_relation_query = dataset.table("select_from_table").query()  # type: ignore
    dialect = dataset._destination.capabilities().sqlglot_dialect

    # NOTE: The resulting insert into sql queries for these destinations
    # are being double-parsed (presumably by ibis and sqlglot)
    # so we just define cases for special handling.
    # The reason - they don't recognize double quotes ("") for identifiers
    specical_handling = [
        "bigquery",
        "databricks",
        "sqlalchemy",
    ]

    with pipeline.sql_client() as client:
        if destination_config.destination_type in specical_handling:
            target_table = client.make_qualified_table_name("insert_into_table", escape=False)
            select_relation_query = str(select_relation_query).replace("`", "")
        else:
            target_table = client.make_qualified_table_name("insert_into_table")

        query = sge.insert(
            expression=select_relation_query,
            into=target_table,
            dialect=dialect,
        ).sql()

        client.execute_sql(query)

        with client.execute_query(f"SELECT * FROM {target_table}") as curr:
            df = curr.df()
            assert df.shape[0] == 20
