import pytest

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
        [{"id": i + 10, "children": [{"id": i + 100}, {"id": i + 1000}]} for i in range(300)],
        table_name="items",
    )

    for df in pipeline.dataset.iter_df(table="items", batch_size=5):
        assert len(df.index) == 5
