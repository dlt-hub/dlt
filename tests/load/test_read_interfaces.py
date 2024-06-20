import pytest
from pandas import DataFrame

from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True),
    ids=lambda x: x.name,
)
def test_read_interfaces(destination_config: DestinationTestConfiguration) -> None:
    # we load a table with child table and check wether ibis works
    pipeline = destination_config.setup_pipeline(
        "ibis_pipeline", dataset_name="ibis_test", dev_mode=True
    )
    pipeline.run(
        [{"id": i + 10, "children": [{"id": i + 100}, {"id": i + 1000}]} for i in range(5)],
        table_name="items",
    )

    with pipeline.sql_client() as c:
        with c.execute_query("SELECT * FROM items") as cursor:
            df = DataFrame(cursor.fetchmany(10))
            df.columns = [x.name for x in cursor.description]

        print(df)

    assert False
