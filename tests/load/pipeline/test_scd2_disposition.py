import pytest, dlt

from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb"]),
    ids=lambda x: x.name,
)
def test_simple_scd2_load(destination_config: DestinationTestConfiguration) -> None:
    @dlt.resource(name="items", write_disposition="scd2", primary_key="id")
    def load_items():
        yield from [{
            "id": 1,
            "name": "one",
        },
        {
            "id": 2,
            "name": "two",
        },
        {
            "id": 3,
            "name": "three",
        }]
    p = destination_config.setup_pipeline("test", full_refresh=True)
    p.run(load_items())
    print(p.default_schema.to_pretty_yaml())
    assert False