from typing import Iterator, Any

import dlt
from tests.load.pipeline.utils import load_table_counts


def test_dremio() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="dremio-test",
        destination="dremio",
        staging="filesystem",
        dataset_name="bar",
        # dataset_name="foo",
        # full_refresh=True,
    )

    @dlt.resource(name="items", write_disposition="replace")
    def items() -> Iterator[Any]:
        yield {
            "id": 1,
            "name": "item",
            "sub_items": [{"id": 101, "name": "sub item 101"}, {"id": 101, "name": "sub item 102"}],
        }

    print(pipeline.run([items]))

    table_counts = load_table_counts(
        pipeline, *[t["name"] for t in pipeline.default_schema._schema_tables.values()]
    )
    assert table_counts["items"] == 1
    assert table_counts["items__sub_items"] == 2
    assert table_counts["_dlt_loads"] == 1
