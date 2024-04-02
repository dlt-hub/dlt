import os
import pytest

import dlt

from tests.pipeline.utils import assert_load_info, load_table_counts
from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["duckdb", "synapse"]),
    ids=lambda x: x.name,
)
def test_buffering(destination_config: DestinationTestConfiguration) -> None:
    @dlt.resource(write_disposition="replace")
    def items():
        yield [{"id": i} for i in range(10)]

    # set buffer size less than number of data items
    os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "5"

    # ensure both writer types are tested
    p = destination_config.setup_pipeline("abstract", full_refresh=True)
    if destination_config.destination == "duckdb":
        assert p.destination.capabilities().insert_values_writer_type == "default"
    elif destination_config.destination == "synapse":
        assert p.destination.capabilities().insert_values_writer_type == "select_union"

    # run pipeline and assert expectations
    info = p.run(items())
    assert_load_info(info)
    assert load_table_counts(p, "items")["items"] == 10
