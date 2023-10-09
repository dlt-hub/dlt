
import pytest
import os
import datetime  # noqa: I251
from typing import Iterator, Any

import dlt
from dlt.common import pendulum
from dlt.common.utils import uniq_id
from tests.load.pipeline.utils import  load_table_counts
from tests.cases import table_update_and_row, assert_all_data_types_row
from tests.pipeline.utils import assert_load_info

from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration

from tests.utils import skip_if_not_active

skip_if_not_active("athena")


def test_iceberg() -> None:
    os.environ['DESTINATION__FILESYSTEM__BUCKET_URL'] = "s3://dlt-ci-test-bucket"

    pipeline = dlt.pipeline(pipeline_name="aaathena-iceberg", destination="athena", staging="filesystem", full_refresh=True)

    def items() -> Iterator[Any]:
        yield {
            "id": 1,
            "name": "item",
            "sub_items": [{
                "id": 101,
                "name": "sub item 101"
            },{
                "id": 101,
                "name": "sub item 102"
            }]
        }

    @dlt.resource(name="items_normal", write_disposition="append")
    def items_normal():
        yield from items()

    @dlt.resource(name="items_iceberg", write_disposition="append")
    def items_iceberg():
        yield from items()

    print(pipeline.run([items_normal, items_iceberg]))

    return

    # see if we have athena tables with items
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema._schema_tables.values() ])
    assert table_counts["items"] == 1
    assert table_counts["items__sub_items"] == 2
    assert table_counts["_dlt_loads"] == 1

    pipeline.run(items)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema._schema_tables.values() ])
    assert table_counts["items"] == 2
    assert table_counts["items__sub_items"] == 4
    assert table_counts["_dlt_loads"] == 2