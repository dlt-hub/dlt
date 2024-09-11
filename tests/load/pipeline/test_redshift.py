from typing import Any, Iterator

import pytest

import dlt
from dlt.common.destination.exceptions import UnsupportedDataType
from dlt.common.utils import uniq_id
from dlt.pipeline.exceptions import PipelineStepFailed
from tests.load.utils import destinations_configs, DestinationTestConfiguration
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

    # apply the exact columns definitions so we process nested and wei types correctly!
    @dlt.resource(table_name="data_types", write_disposition="append", columns=column_schemas)
    def my_resource() -> Iterator[Any]:
        nonlocal data_types
        yield [data_types] * 10

    @dlt.source(max_table_nesting=0)
    def my_source() -> Any:
        return my_resource

    with pytest.raises(PipelineStepFailed) as pip_ex:
        pipeline.run(my_source(), **destination_config.run_kwargs)
    assert isinstance(pip_ex.value.__cause__, UnsupportedDataType)
    if destination_config.file_format == "parquet":
        assert pip_ex.value.__cause__.data_type == "time"
    else:
        assert pip_ex.value.__cause__.data_type in ("time", "binary")
