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


@pytest.mark.parametrize("destination_config", destinations_configs(default_sql_configs=True, default_staging_configs=True, all_staging_configs=True), ids=lambda x: x.name)
@pytest.mark.parametrize("item_type", ["pandas", "table", "record_batch"])
def test_load_item(item_type: Literal["pandas", "table", "record_batch"], destination_config: DestinationTestConfiguration) -> None:
    include_time = destination_config.destination not in ("athena", "redshift")  # athena/redshift can't load TIME columns from parquet
    item, records = arrow_table_all_data_types(item_type, include_json=False, include_time=include_time)

    pipeline = destination_config.setup_pipeline("arrow_" + uniq_id())

    @dlt.resource
    def some_data():
        yield item

    pipeline.run(some_data())
    # assert the table types
    some_table_columns = pipeline.default_schema.get_table("some_data")["columns"]
    assert some_table_columns["string"]["data_type"] == "text"
    assert some_table_columns["float"]["data_type"] == "double"
    assert some_table_columns["int"]["data_type"] == "bigint"
    assert some_table_columns["datetime"]["data_type"] == "timestamp"
    assert some_table_columns["binary"]["data_type"] == "binary"
    assert some_table_columns["decimal"]["data_type"] == "decimal"
    assert some_table_columns["bool"]["data_type"] == "bool"
    if include_time:
        assert some_table_columns["time"]["data_type"] == "time"

    rows = [list(row) for row in select_data(pipeline, "SELECT * FROM some_data ORDER BY 1")]

    for row in rows:
        for i in range(len(row)):
            # Postgres returns memoryview for binary columns
            if isinstance(row[i], memoryview):
                row[i] = row[i].tobytes()


    if destination_config.destination == "redshift":
        # Binary columns are hex formatted in results
        for record in records:
            if "binary" in record:
                record["binary"] = record["binary"].hex()

    for row in rows:
        for i in range(len(row)):
            if isinstance(row[i], datetime):
                row[i] = pendulum.instance(row[i])

    expected = [list(r.values()) for r in records]

    assert rows == expected
