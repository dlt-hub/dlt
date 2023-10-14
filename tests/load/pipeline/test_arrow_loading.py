import pytest
from datetime import datetime  # noqa: I251

from typing import Any, Union, List, Dict, Tuple, Literal

import dlt
from dlt.common import Decimal
from dlt.common import pendulum
from dlt.common.utils import uniq_id
from tests.load.utils import destinations_configs, DestinationTestConfiguration
from tests.load.pipeline.utils import assert_table, assert_query_data, select_data
from tests.utils import preserve_environ
from tests.cases import arrow_table_all_data_types


@pytest.mark.parametrize("destination_config", destinations_configs(file_format="parquet", default_sql_configs=True, default_staging_configs=True, all_buckets_filesystem_configs=True), ids=lambda x: x.name)
@pytest.mark.parametrize("item_type", ["pandas", "table", "record_batch"])
def test_load_item(item_type: Literal["pandas", "table", "record_batch"], destination_config: DestinationTestConfiguration):
    include_time = destination_config.destination != "athena"  # athena can't load TIME type
    item, records = arrow_table_all_data_types(item_type, include_json=False, include_time=include_time)

    pipeline = destination_config.setup_pipeline("arrow_" + uniq_id())

    @dlt.resource
    def some_data():
        yield item

    pipeline.run(some_data())

    rows = [list(row) for row in select_data(pipeline, "SELECT * FROM some_data ORDER BY 1")]

    for row in rows:
        for i in range(len(row)):
            if isinstance(row[i], datetime):
                row[i] = pendulum.instance(row[i])

    expected = [list(r.values()) for r in records]

    assert rows == expected
