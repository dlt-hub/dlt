import pytest
from typing import Iterator, Any

import dlt
from tests.load.utils import DestinationTestConfiguration, destinations_configs
from tests.pipeline.utils import load_table_counts

from dlt.destinations.exceptions import DatabaseTerminalException

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        with_table_format="iceberg",
        subset=["athena"],
    ),
    ids=lambda x: x.name,
)
def test_iceberg(destination_config: DestinationTestConfiguration) -> None:
    """
    We write two tables, one with the iceberg flag, one without. We expect the iceberg table and its subtables to accept update commands
    and the other table to reject them.
    """

    pipeline = destination_config.setup_pipeline("test_iceberg", dev_mode=True)

    def items() -> Iterator[Any]:
        yield {
            "id": 1,
            "name": "item",
            "sub_items": [{"id": 101, "name": "sub item 101"}, {"id": 101, "name": "sub item 102"}],
        }

    @dlt.resource(name="items_normal", write_disposition="append")
    def items_normal():
        yield from items()

    @dlt.resource(name="items_iceberg", write_disposition="append", table_format="iceberg")
    def items_iceberg():
        yield from items()

    print(pipeline.run([items_normal, items_iceberg]))

    # see if we have athena tables with items
    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema._schema_tables.values()]
    )
    assert table_counts["items_normal"] == 1
    assert table_counts["items_normal__sub_items"] == 2
    assert table_counts["_dlt_loads"] == 1

    assert table_counts["items_iceberg"] == 1
    assert table_counts["items_iceberg__sub_items"] == 2

    with pipeline.sql_client() as client:
        client.execute_sql("SELECT * FROM items_normal")

        # modifying regular athena table will fail
        with pytest.raises(DatabaseTerminalException) as dbex:
            client.execute_sql("UPDATE items_normal SET name='new name'")
        assert "Modifying Hive table rows is only supported for transactional tables" in str(dbex)
        with pytest.raises(DatabaseTerminalException) as dbex:
            client.execute_sql("UPDATE items_normal__sub_items SET name='super new name'")
        assert "Modifying Hive table rows is only supported for transactional tables" in str(dbex)

        # modifying iceberg table will succeed
        client.execute_sql("UPDATE items_iceberg SET name='new name'")
        client.execute_sql("UPDATE items_iceberg__sub_items SET name='super new name'")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        with_table_format="iceberg",
        subset=["athena"],
    ),
    ids=lambda x: x.name,
)
def test_force_iceberg_deprecation(destination_config: DestinationTestConfiguration) -> None:
    """Fails on deprecated force_iceberg option"""
    destination_config.force_iceberg = True
    pipeline = destination_config.setup_pipeline("test_force_iceberg_deprecation", dev_mode=True)

    def items() -> Iterator[Any]:
        yield {
            "id": 1,
            "name": "item",
            "sub_items": [{"id": 101, "name": "sub item 101"}, {"id": 101, "name": "sub item 102"}],
        }

    @dlt.resource(name="items_normal", write_disposition="append")
    def items_normal():
        yield from items()

    @dlt.resource(name="items_hive", write_disposition="append", table_format="hive")
    def items_hive():
        yield from items()

    print(pipeline.run([items_normal, items_hive]))

    # items_normal should load as iceberg
    # _dlt_pipeline_state should load as iceberg (IMPORTANT for backward comp)

    with pipeline.sql_client() as client:
        client.execute_sql("SELECT * FROM items_normal")
        client.execute_sql("SELECT * FROM items_hive")

        with pytest.raises(DatabaseTerminalException) as dbex:
            client.execute_sql("UPDATE items_hive SET name='new name'")
        assert "Modifying Hive table rows is only supported for transactional tables" in str(dbex)

        # modifying iceberg table will succeed
        client.execute_sql("UPDATE items_normal SET name='new name'")
        client.execute_sql("UPDATE items_normal__sub_items SET name='super new name'")
        client.execute_sql("UPDATE _dlt_pipeline_state SET pipeline_name='new name'")

    # trigger deprecation warning
    from dlt.destinations import athena

    athena_c = athena(force_iceberg=True).configuration(athena().spec()._bind_dataset_name("ds"))
    assert athena_c.force_iceberg is True
