import pytest
import dlt
from typing import Any
from tests.load.pipeline.utils import (
    destinations_configs,
    DestinationTestConfiguration,
)

from tests.pipeline.utils import assert_data_table_counts

from tests.pipeline.utils import assert_load_info
from dlt.pipeline.exceptions import PipelineStepFailed


@dlt.resource(primary_key="id")
def data_with_subtables(offset: int) -> Any:
    for _, index in enumerate(range(offset, offset + 100), 1):
        yield {
            "id": index,
            "name": f"item {index}",
            "sub_items": [{"id": index + 1000, "name": f"sub item {index + 1000}"}],
        }


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
def test_switch_from_merge(destination_config: DestinationTestConfiguration):
    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_switch_from_merge", dev_mode=True
    )

    info = pipeline.run(
        data_with_subtables(10),
        table_name="items",
        write_disposition="merge",
        loader_file_format=destination_config.file_format,
    )
    assert_data_table_counts(pipeline, {"items": 100, "items__sub_items": 100})
    assert pipeline.default_schema._normalizers_config["json"]["config"]["propagation"]["tables"][
        "items"
    ] == {"_dlt_id": "_dlt_root_id"}

    info = pipeline.run(
        data_with_subtables(10),
        table_name="items",
        write_disposition="merge",
        loader_file_format=destination_config.file_format,
    )
    assert_load_info(info)
    assert_data_table_counts(
        pipeline,
        {
            "items": 100 if destination_config.supports_merge else 200,
            "items__sub_items": 100 if destination_config.supports_merge else 200,
        },
    )
    assert pipeline.default_schema._normalizers_config["json"]["config"]["propagation"]["tables"][
        "items"
    ] == {"_dlt_id": "_dlt_root_id"}

    info = pipeline.run(
        data_with_subtables(10),
        table_name="items",
        write_disposition="append",
        loader_file_format=destination_config.file_format,
    )
    assert_load_info(info)
    assert_data_table_counts(
        pipeline,
        {
            "items": 200 if destination_config.supports_merge else 300,
            "items__sub_items": 200 if destination_config.supports_merge else 300,
        },
    )
    assert pipeline.default_schema._normalizers_config["json"]["config"]["propagation"]["tables"][
        "items"
    ] == {"_dlt_id": "_dlt_root_id"}

    info = pipeline.run(
        data_with_subtables(10),
        table_name="items",
        write_disposition="replace",
        loader_file_format=destination_config.file_format,
    )
    assert_load_info(info)
    assert_data_table_counts(pipeline, {"items": 100, "items__sub_items": 100})
    assert pipeline.default_schema._normalizers_config["json"]["config"]["propagation"]["tables"][
        "items"
    ] == {"_dlt_id": "_dlt_root_id"}


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
@pytest.mark.parametrize("with_root_key", [True, False])
def test_switch_to_merge(destination_config: DestinationTestConfiguration, with_root_key: bool):
    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_switch_to_merge", dev_mode=True
    )

    @dlt.source()
    def source():
        return data_with_subtables(10)

    s = source()
    s.root_key = with_root_key

    info = pipeline.run(
        s,
        table_name="items",
        write_disposition="append",
        loader_file_format=destination_config.file_format,
    )
    assert_data_table_counts(pipeline, {"items": 100, "items__sub_items": 100})

    if with_root_key:
        assert pipeline.default_schema._normalizers_config["json"]["config"]["propagation"][
            "root"
        ] == {"_dlt_id": "_dlt_root_id"}
    else:
        assert "propagation" not in pipeline.default_schema._normalizers_config["json"].get(
            "config", {}
        )

    # schemaless destinations allow adding of root key without the pipeline failing
    # for now this is only the case for dremio
    # doing this will result in somewhat useless behavior
    destination_allows_adding_root_key = destination_config.destination in ["dremio", "clickhouse"]

    if destination_allows_adding_root_key and not with_root_key:
        pipeline.run(
            s,
            table_name="items",
            write_disposition="merge",
            loader_file_format=destination_config.file_format,
        )
        return

    # without a root key this will fail, it is expected as adding non-nullable columns should not work
    if not with_root_key and destination_config.supports_merge:
        with pytest.raises(PipelineStepFailed):
            pipeline.run(
                s,
                table_name="items",
                write_disposition="merge",
                loader_file_format=destination_config.file_format,
            )
        return

    info = pipeline.run(
        s,
        table_name="items",
        write_disposition="merge",
        loader_file_format=destination_config.file_format,
    )
    assert_load_info(info)
    assert_data_table_counts(
        pipeline,
        {
            "items": 100 if destination_config.supports_merge else 200,
            "items__sub_items": 100 if destination_config.supports_merge else 200,
        },
    )
    assert pipeline.default_schema._normalizers_config["json"]["config"]["propagation"]["tables"][
        "items"
    ] == {"_dlt_id": "_dlt_root_id"}
