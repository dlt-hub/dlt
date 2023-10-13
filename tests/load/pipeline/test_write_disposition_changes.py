import pytest
import dlt
from typing import Any
from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration, assert_data_table_counts
from tests.pipeline.utils import assert_load_info
from dlt.pipeline.exceptions import PipelineStepFailed

def data_with_subtables(offset: int) -> Any:
    for _, index in enumerate(range(offset, offset+100), 1):
        yield {
            "id": index,
            "name": f"item {index}",
            "sub_items": [{
                "id": index + 1000,
                "name": f"sub item {index + 1000}"
            }]
        }

@pytest.mark.parametrize("destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name)
def test_switch_from_merge(destination_config: DestinationTestConfiguration):
    pipeline = destination_config.setup_pipeline(pipeline_name='test_switch_from_merge', full_refresh=True)

    info = (pipeline.run(data_with_subtables(10), table_name="items", write_disposition="merge"))
    assert_data_table_counts(pipeline, {
        "items": 100,
        "items__sub_items": 100
    })
    assert pipeline.default_schema._normalizers_config["json"]["config"]["propagation"]["tables"]["items"] == {'_dlt_id': '_dlt_root_id'}

    info = (pipeline.run(data_with_subtables(10), table_name="items", write_disposition="merge"))
    assert_load_info(info)
    assert_data_table_counts(pipeline, {
        "items": 100 if destination_config.supports_merge else 200,
        "items__sub_items": 100 if destination_config.supports_merge else 200
    })
    assert pipeline.default_schema._normalizers_config["json"]["config"]["propagation"]["tables"]["items"] == {'_dlt_id': '_dlt_root_id'}

    info = (pipeline.run(data_with_subtables(10), table_name="items", write_disposition="append"))
    assert_load_info(info)
    assert_data_table_counts(pipeline, {
        "items": 200 if destination_config.supports_merge else 300,
        "items__sub_items": 200 if destination_config.supports_merge else 300
    })
    assert pipeline.default_schema._normalizers_config["json"]["config"]["propagation"]["tables"]["items"] == {'_dlt_id': '_dlt_root_id'}

    info = (pipeline.run(data_with_subtables(10), table_name="items", write_disposition="replace"))
    assert_load_info(info)
    assert_data_table_counts(pipeline, {
        "items": 100,
        "items__sub_items": 100
    })
    assert pipeline.default_schema._normalizers_config["json"]["config"]["propagation"]["tables"]["items"] == {'_dlt_id': '_dlt_root_id'}


@pytest.mark.parametrize("destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name)
@pytest.mark.parametrize("with_root_key", [True, False])
def test_switch_to_merge(destination_config: DestinationTestConfiguration, with_root_key: bool):
    pipeline = destination_config.setup_pipeline(pipeline_name='test_switch_to_merge', full_refresh=True)

    @dlt.resource()
    def resource():
        yield data_with_subtables(10)

    @dlt.source()
    def source():
        return resource()

    s = source()
    s.root_key = with_root_key

    info = (pipeline.run(s, table_name="items", write_disposition="append"))
    assert_data_table_counts(pipeline, {
        "items": 100,
        "items__sub_items": 100
    })

    if with_root_key:
        assert pipeline.default_schema._normalizers_config["json"]["config"]["propagation"]["root"] == {'_dlt_id': '_dlt_root_id'}
    else:
        assert "propagation" not in pipeline.default_schema._normalizers_config["json"]["config"]

    # without a root key this will fail, it is expected
    if not with_root_key and destination_config.supports_merge:
        with pytest.raises(PipelineStepFailed):
            pipeline.run(s, table_name="items", write_disposition="merge")
        return

    info = (pipeline.run(s, table_name="items", write_disposition="merge"))
    assert_load_info(info)
    assert_data_table_counts(pipeline, {
        "items": 100 if destination_config.supports_merge else 200,
        "items__sub_items": 100 if destination_config.supports_merge else 200,
    })
    assert pipeline.default_schema._normalizers_config["json"]["config"]["propagation"]["tables"]["items"] == {'_dlt_id': '_dlt_root_id'}

