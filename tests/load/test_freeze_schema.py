import dlt, os, pytest
from dlt.common.utils import uniq_id
import duckdb

from tests.load.pipeline.utils import load_table_counts
from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.common.schema.exceptions import SchemaFrozenException
from dlt.common.schema import utils

SCHEMA_UPDATE_MODES = ["evolve", "freeze-and-trim", "freeze-and-raise", "freeze-and-discard"]

@pytest.mark.parametrize("update_mode", SCHEMA_UPDATE_MODES)
def test_freeze_schema(update_mode: str) -> None:

    # freeze pipeline, drop additional values
    # this will allow for the first run to create the schema, but will not accept further updates after that
    pipeline = dlt.pipeline(pipeline_name=uniq_id(), destination='duckdb', credentials=duckdb.connect(':memory:'))

    @dlt.resource(name="items", write_disposition="append", schema_evolution_settings=update_mode)
    def load_items():
        global offset
        for _, index in enumerate(range(0, 10), 1):
            yield {
                "id": index,
                "name": f"item {index}"
            }

    @dlt.resource(name="items", write_disposition="append", schema_evolution_settings=update_mode)
    def load_items_with_subitems():
        global offset
        for _, index in enumerate(range(0, 10), 1):
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

    pipeline.run([load_items])
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    # check data
    assert table_counts["items"] == 10
    schema_hash = utils.generate_version_hash(pipeline.default_schema.to_dict())

    # on freeze and raise we expect an exception
    if update_mode == "freeze-and-raise":
        with pytest.raises(PipelineStepFailed) as py_ex:
            pipeline.run([load_items_with_subitems])
            assert isinstance(py_ex.value.__context__, SchemaFrozenException)
    else:
        pipeline.run([load_items_with_subitems])

    # check data
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    assert table_counts["items"] == 20 if update_mode not in ["freeze-and-raise", "freeze-and-discard"] else 10

    # frozen schemas should not have changed
    if update_mode != "evolve":
        assert schema_hash == utils.generate_version_hash(pipeline.default_schema.to_dict())
        assert "items__sub_items" not in table_counts
        # schema was not migrated to contain new attribute
        assert "new_attribute" not in pipeline.default_schema.tables["items"]["columns"]
    # regular mode evolves the schema
    else:
        assert table_counts["items__sub_items"] == 20
        # schema was not migrated to contain new attribute
        assert "new_attribute" in pipeline.default_schema.tables["items"]["columns"]

