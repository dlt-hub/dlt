from typing import Dict
import yaml
import dlt, os, pytest
from dlt.common.utils import uniq_id

from tests.pipeline.utils import  assert_load_info
from tests.load.pipeline.utils import  drop_active_pipeline_data, load_table_counts, load_tables_to_dicts
from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration
from dlt.pipeline.exceptions import PipelineStepFailed

@pytest.mark.parametrize("destination_config", destinations_configs(default_sql_configs=True, subset=["duckdb"]), ids=lambda x: x.name)
def test_freeze_schema(destination_config: DestinationTestConfiguration) -> None:

    pipeline = destination_config.setup_pipeline("test_freeze_schema", dataset_name="freeze" + uniq_id())

    @dlt.resource(name="items", write_disposition="append")
    def load_items():
        global offset
        for _, index in enumerate(range(0, 120), 1):
            yield {
                "id": index,
                "name": f"item {index}"
            }

    @dlt.resource(name="items", write_disposition="append")
    def load_items_with_subitems():
        global offset
        for _, index in enumerate(range(0, 120), 1):
            yield {
                "id": index,
                "name": f"item {index}",
                "new_attribute": "hello",
                "sub_items": [{
                    "id": index + 1000,
                    "name": f"sub item {index + 1000}"
                },{
                    "id": index + 2000,
                    "name": f"sub item {index + 2000}"
                }]
            }

    pipeline.run([load_items], loader_file_format=destination_config.file_format)

    # freeze pipeline, drop additional values
    os.environ['NORMALIZE__SCHEMA_UPDATE_MODE'] = "freeze-and-discard"
    pipeline.run([load_items_with_subitems], loader_file_format=destination_config.file_format)

    # schema was not migrated to contain new subtable
    assert "items__sub_items" not in pipeline.default_schema.tables
    # schema was not migrated to contain new attribute
    assert "new_attribute" not in pipeline.default_schema.tables["items"]["columns"]

    # now raise on migration
    os.environ['NORMALIZE__SCHEMA_UPDATE_MODE'] = "freeze-and-raise"
    with pytest.raises(PipelineStepFailed) as py_ex:
        pipeline.run([load_items_with_subitems], loader_file_format=destination_config.file_format)
        assert isinstance(py_ex.value.__context__, Exception)

