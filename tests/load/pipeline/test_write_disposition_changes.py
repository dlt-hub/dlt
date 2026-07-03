import pytest
from pytest_mock import MockType, MockerFixture
from typing import Any

import dlt
from dlt.common import logger
from dlt.common.normalizers.typing import TJSONNormalizer
from dlt.pipeline.exceptions import PipelineStepFailed

from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
)

from tests.pipeline.utils import assert_table_counts, assert_load_info


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
        **destination_config.run_kwargs,
    )
    assert_table_counts(pipeline, {"items": 100, "items__sub_items": 100})
    # if merge is not supported, root key propagation is disabled
    if destination_config.supports_merge:
        assert pipeline.default_schema._normalizers_config["json"]["config"]["propagation"][
            "tables"
        ]["items"] == {"_dlt_id": "_dlt_root_id"}

    info = pipeline.run(
        data_with_subtables(10),
        table_name="items",
        write_disposition="merge",
        **destination_config.run_kwargs,
    )
    assert_load_info(info)
    assert_table_counts(
        pipeline,
        {
            "items": 100 if destination_config.supports_merge else 200,
            "items__sub_items": 100 if destination_config.supports_merge else 200,
        },
    )
    if destination_config.supports_merge:
        assert pipeline.default_schema._normalizers_config["json"]["config"]["propagation"][
            "tables"
        ]["items"] == {"_dlt_id": "_dlt_root_id"}

    info = pipeline.run(
        data_with_subtables(10),
        table_name="items",
        write_disposition="append",
        **destination_config.run_kwargs,
    )
    assert_load_info(info)
    assert_table_counts(
        pipeline,
        {
            "items": 200 if destination_config.supports_merge else 300,
            "items__sub_items": 200 if destination_config.supports_merge else 300,
        },
    )
    if destination_config.supports_merge:
        assert pipeline.default_schema._normalizers_config["json"]["config"]["propagation"][
            "tables"
        ]["items"] == {"_dlt_id": "_dlt_root_id"}

    info = pipeline.run(
        data_with_subtables(10),
        table_name="items",
        write_disposition="replace",
        **destination_config.run_kwargs,
    )
    assert_load_info(info)
    assert_table_counts(pipeline, {"items": 100, "items__sub_items": 100})
    if destination_config.supports_merge:
        assert pipeline.default_schema._normalizers_config["json"]["config"]["propagation"][
            "tables"
        ]["items"] == {"_dlt_id": "_dlt_root_id"}


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, supports_merge=True),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("with_root_key", [True, False, None])
def test_switch_to_merge(
    destination_config: DestinationTestConfiguration, with_root_key: bool, mocker: MockerFixture
):
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
        **destination_config.run_kwargs,
    )
    assert_table_counts(pipeline, {"items": 100, "items__sub_items": 100})

    norm_config: TJSONNormalizer = pipeline.default_schema._normalizers_config["json"]
    if with_root_key is True:
        assert norm_config["config"]["root_key_propagation"] is True
        assert "root" not in norm_config["config"]["propagation"]
        assert "items" in norm_config["config"]["propagation"]["tables"]
    elif with_root_key is False:
        assert norm_config["config"]["root_key_propagation"] is False
        assert "propagation" not in norm_config["config"]
    else:
        assert "config" not in norm_config

    # schemaless destinations allow adding of root key without the pipeline failing
    # they do not mind adding NOT NULL columns to tables with existing data (id NOT NULL is supported at all)
    # doing this will result in somewhat useless behavior
    destination_allows_adding_root_key = (
        not pipeline.destination_client().capabilities.enforces_nulls_on_alter
    )

    def _assert_root_key_warn(spy: MockType) -> None:
        for args_ in spy.call_args_list[0]:
            if args_[0].startswith("You are adding a root_key in column"):
                return
        # just to show all options
        spy.assert_not_called()

    if destination_allows_adding_root_key and with_root_key is None:
        logger_spy = mocker.spy(logger, "warning")
        pipeline.run(
            s,
            table_name="items",
            write_disposition="merge",
            **destination_config.run_kwargs,
        )
        _assert_root_key_warn(logger_spy)
        return

    # without a root key this will fail, it is expected as adding non-nullable columns should not work
    if with_root_key is None and destination_config.supports_merge:
        logger_spy = mocker.spy(logger, "warning")
        with pytest.raises(PipelineStepFailed):
            pipeline.run(
                s,
                table_name="items",
                write_disposition="merge",
                **destination_config.run_kwargs,
            )
        _assert_root_key_warn(logger_spy)
        return

    # if root key is enabled - this should work because root keys are present
    # if root key is explicitly disabled - still should work because we have just one nesting level
    #   and parent key will be used as fallback
    logger_spy = mocker.spy(logger, "warning")
    info = pipeline.run(
        s,
        table_name="items",
        write_disposition="merge",
        **destination_config.run_kwargs,
    )
    logger_spy.assert_not_called()
    assert_load_info(info)
    assert_table_counts(
        pipeline,
        {
            "items": 100 if destination_config.supports_merge else 200,
            "items__sub_items": 100 if destination_config.supports_merge else 200,
        },
    )
    # propagation only for enabled
    if with_root_key is True:
        assert pipeline.default_schema._normalizers_config["json"]["config"]["propagation"][
            "tables"
        ]["items"] == {"_dlt_id": "_dlt_root_id"}
    else:
        # still no propagation setup
        assert "propagation" not in norm_config["config"]


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
@pytest.mark.essential
def test_incremental_merge_full_refresh_with_replace(
    destination_config: DestinationTestConfiguration,
) -> None:
    # non-normalized table and column names so identifiers are exercised through normalization
    @dlt.source(name="items_source")
    def items_source(records: Any) -> Any:
        @dlt.resource(
            name="items", table_name="MyItems", write_disposition="merge", primary_key="Id"
        )
        def items(updated_at: Any = dlt.sources.incremental("UpdatedAt")) -> Any:
            yield from records

        return items

    def source(records: Any) -> Any:
        s = items_source(records)
        # propagations not needed up to 2 nesting levels
        s.root_key = False
        return s

    seed = [
        {"Id": 1, "UpdatedAt": "2026-05-28", "SubItems": [{"Id": 101}]},
        {"Id": 2, "UpdatedAt": "2020-05-29", "SubItems": [{"Id": 201}]},
    ]

    pipeline = destination_config.setup_pipeline(
        pipeline_name="test_incremental_merge_after_replace", dev_mode=True
    )

    # full refresh seeds the root and nested tables and advances the incremental cursor
    info = pipeline.run(source(seed), write_disposition="replace", **destination_config.run_kwargs)
    assert_load_info(info)
    assert_table_counts(pipeline, {"my_items": 2, "my_items__sub_items": 2})

    # incremental merge run with no new rows (all filtered by the cursor) produces no load package
    # and must keep root and nested data (the #3998 regression truncated them here)
    info = pipeline.run(source(seed), **destination_config.run_kwargs)
    assert_load_info(info, expected_load_packages=0)
    assert_table_counts(pipeline, {"my_items": 2, "my_items__sub_items": 2})

    # a later merge run with new data still loads to root and nested tables
    info = pipeline.run(
        source([{"Id": 3, "UpdatedAt": "2026-05-30", "SubItems": [{"Id": 301}]}]),
        **destination_config.run_kwargs,
    )
    assert_load_info(info)
    assert_table_counts(pipeline, {"my_items": 3, "my_items__sub_items": 3})

    # an explicit replace still resets the destination: prior rows are dropped, only new data remains
    info = pipeline.run(
        source([{"Id": 4, "UpdatedAt": "2026-06-01", "SubItems": [{"Id": 401}]}]),
        write_disposition="replace",
        **destination_config.run_kwargs,
    )
    assert_load_info(info)
    assert_table_counts(pipeline, {"my_items": 1, "my_items__sub_items": 1})

    # and merging continues to load on top of the reset data
    info = pipeline.run(
        source([{"Id": 5, "UpdatedAt": "2026-06-02", "SubItems": [{"Id": 501}]}]),
        **destination_config.run_kwargs,
    )
    assert_load_info(info)
    assert_table_counts(pipeline, {"my_items": 2, "my_items__sub_items": 2})

    # a replace run with no data truncates the (non-normalized) root and nested tables to empty: the
    # empty-table handling matches the normalized table name, so a replace resource truncates
    # correctly when it has none (and the runs above show it does not drop data when data is present)
    pipeline.run(source([]), write_disposition="replace", **destination_config.run_kwargs)
    assert_table_counts(pipeline, {"my_items": 0, "my_items__sub_items": 0})
