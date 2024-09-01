import pytest
from typing import Iterator, Any

import dlt
from tests.pipeline.utils import load_table_counts
from tests.load.utils import DestinationTestConfiguration, destinations_configs


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["dremio"]),
    ids=lambda x: x.name,
)
def test_dremio(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline("dremio-test", dataset_name="bar", dev_mode=True)

    @dlt.resource(name="items", write_disposition="replace")
    def items() -> Iterator[Any]:
        yield {
            "id": 1,
            "name": "item",
            "sub_items": [{"id": 101, "name": "sub item 101"}, {"id": 101, "name": "sub item 102"}],
        }

    print(pipeline.run([items], **destination_config.run_kwargs))

    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema._schema_tables.values()]
    )
    assert table_counts["items"] == 1
    assert table_counts["items__sub_items"] == 2
    assert table_counts["_dlt_loads"] == 1
