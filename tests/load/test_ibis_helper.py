import pytest
import ibis
import dlt

from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration
from dlt.helpers.ibis_helper import ibis_helper


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True, exclude=["athena", "dremio", "redshift", "databricks", "synapse"]
    ),
    ids=lambda x: x.name,
)
def test_ibis_helper(destination_config: DestinationTestConfiguration) -> None:
    # we load a table with child table and check wether ibis works
    pipeline = destination_config.setup_pipeline(
        "ibis_pipeline", dataset_name="ibis_test", dev_mode=True
    )
    pipeline.run(
        [{"id": i + 10, "children": [{"id": i + 100}, {"id": i + 1000}]} for i in range(5)],
        table_name="ibis_items",
    )

    with ibis_helper(pipeline) as ibis_backend:
        # check we can read table names
        assert {tname.lower() for tname in ibis_backend.list_tables()} >= {
            "_dlt_loads",
            "_dlt_pipeline_state",
            "_dlt_version",
            "ibis_items",
            "ibis_items__children",
        }

        id_identifier = "id"
        if destination_config.destination == "snowflake":
            id_identifier = id_identifier.upper()

        # check we can read data
        assert ibis_backend.sql("SELECT id FROM ibis_items").to_pandas()[
            id_identifier
        ].tolist() == [
            10,
            11,
            12,
            13,
            14,
        ]
        assert ibis_backend.sql("SELECT id FROM ibis_items__children").to_pandas()[
            id_identifier
        ].tolist() == [
            100,
            1000,
            101,
            1001,
            102,
            1002,
            103,
            1003,
            104,
            1004,
        ]
