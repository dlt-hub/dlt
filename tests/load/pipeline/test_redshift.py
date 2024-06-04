from typing import Any, Iterator

import pytest

import dlt
from dlt.common.utils import uniq_id
from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration
from tests.cases import table_update_and_row, assert_all_data_types_row
from tests.pipeline.utils import assert_load_info

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_staging_configs=True, subset=["redshift"]),
    ids=lambda x: x.name,
)
def test_redshift_blocks_time_column(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline("redshift_" + uniq_id(), dev_mode=True)

    column_schemas, data_types = table_update_and_row()

    # apply the exact columns definitions so we process complex and wei types correctly!
    @dlt.resource(table_name="data_types", write_disposition="append", columns=column_schemas)
    def my_resource() -> Iterator[Any]:
        nonlocal data_types
        yield [data_types] * 10

    @dlt.source(max_table_nesting=0)
    def my_source() -> Any:
        return my_resource

    info = pipeline.run(my_source(), loader_file_format=destination_config.file_format)

    assert info.has_failed_jobs

    assert (
        "Redshift cannot load TIME columns from"
        in info.load_packages[0].jobs["failed_jobs"][0].failed_message
    )
