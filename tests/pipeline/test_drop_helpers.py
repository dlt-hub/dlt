from typing import List, Dict, Any
import pytest
from copy import deepcopy
import re

import dlt
from dlt.common.schema.typing import LOADS_TABLE_NAME, PIPELINE_STATE_TABLE_NAME, VERSION_TABLE_NAME
from dlt.common.versioned_state import decompress_state
from dlt.pipeline.drop import drop_resources, drop_columns
from dlt.pipeline.helpers import DropCommand, refresh_source

from tests.pipeline.utils import airtable_emojis, assert_load_info


@pytest.mark.parametrize("seen_data", [True, False], ids=["seen_data", "no_data"])
def test_drop_helper_utils(seen_data: bool) -> None:
    pipeline = dlt.pipeline("test_drop_helpers_no_table_drop", destination="duckdb")
    # extract first which should produce tables that didn't seen data
    source = airtable_emojis().with_resources(
        "📆 Schedule", "🦚Peacock", "🦚WidePeacock", "💰Budget"
    )
    if seen_data:
        pipeline.run(source)
    else:
        pipeline.extract(source)

    # drop nothing
    drop_info = drop_resources(pipeline.default_schema.clone(), pipeline.state)
    assert drop_info.modified_tables == []
    assert drop_info.info["tables"] == []

    # drop all resources
    drop_info = drop_resources(pipeline.default_schema.clone(), pipeline.state, drop_all=True)
    # no tables to drop
    tables_to_drop = (
        {"_schedule", "_peacock", "_wide_peacock", "_peacock__peacock", "_wide_peacock__peacock"}
        if seen_data
        else set()
    )
    tables_to_drop_schema = (
        tables_to_drop if seen_data else {"_schedule", "_peacock", "_wide_peacock"}
    )
    assert {t["name"] for t in drop_info.modified_tables} == tables_to_drop
    # no state mods
    assert drop_info.state["sources"]["airtable_emojis"] == {"resources": {}}
    assert set(drop_info.info["tables"]) == tables_to_drop_schema
    assert set(drop_info.info["tables_with_data"]) == tables_to_drop
    # all tables got dropped
    assert drop_info.schema.data_tables(include_incomplete=True) == []
    # dlt tables still there
    assert set(drop_info.schema.dlt_table_names()) == {
        VERSION_TABLE_NAME,
        LOADS_TABLE_NAME,
        PIPELINE_STATE_TABLE_NAME,
    }
    # same but with refresh
    source_clone = source.clone()
    source_clone.schema = pipeline.default_schema.clone()
    with pipeline.managed_state() as state:
        emoji_state = deepcopy(state["sources"]["airtable_emojis"])
        package_state = refresh_source(pipeline, source_clone, refresh="drop_sources")
        # managed state modified
        assert state["sources"]["airtable_emojis"] == {"resources": {}}
        # restore old state for next tests
        state["sources"]["airtable_emojis"] = emoji_state
    if seen_data:
        assert {t["name"] for t in package_state["dropped_tables"]} == tables_to_drop
    else:
        assert package_state == {}
    assert source_clone.schema.data_tables(include_incomplete=True) == []

    # drop only selected resources
    tables_to_drop = {"_schedule"} if seen_data else set()
    # seen_data means full run so we generate child tables in that case
    left_in_schema = (
        {"_peacock", "_wide_peacock", "_peacock__peacock", "_wide_peacock__peacock"}
        if seen_data
        else {"_peacock", "_wide_peacock"}
    )
    drop_info = drop_resources(
        pipeline.default_schema.clone(), pipeline.state, resources=["📆 Schedule"]
    )
    assert set(t["name"] for t in drop_info.modified_tables) == tables_to_drop
    # no changes in state
    assert drop_info.state == pipeline.state
    assert set(drop_info.info["tables"]) == {"_schedule"}
    assert set(drop_info.schema.data_table_names(include_incomplete=True)) == left_in_schema
    source_clone = source_clone.with_resources("📆 Schedule")
    source_clone.schema = pipeline.default_schema.clone()
    with pipeline.managed_state() as state:
        package_state = refresh_source(pipeline, source_clone, refresh="drop_resources")
        # state not modified
        assert state["sources"]["airtable_emojis"] == {"resources": {"🦚Peacock": {"🦚🦚🦚": "🦚"}}}
    if seen_data:
        assert {t["name"] for t in package_state["dropped_tables"]} == tables_to_drop
    else:
        assert package_state == {}
    assert set(source_clone.schema.data_table_names(include_incomplete=True)) == left_in_schema

    # truncate only
    tables_to_truncate = (
        {"_peacock", "_wide_peacock", "_peacock__peacock", "_wide_peacock__peacock"}
        if seen_data
        else set()
    )
    all_in_schema = (
        {"_schedule", "_peacock", "_wide_peacock", "_peacock__peacock", "_wide_peacock__peacock"}
        if seen_data
        else {"_schedule", "_peacock", "_wide_peacock"}
    )
    drop_info = drop_resources(
        pipeline.default_schema.clone(),
        pipeline.state,
        resources=["🦚Peacock", "🦚WidePeacock"],
        state_only=True,
    )
    assert set(t["name"] for t in drop_info.modified_tables) == tables_to_truncate
    # state is modified
    assert drop_info.state["sources"]["airtable_emojis"] == {"resources": {}}
    assert drop_info.info["tables"] == []
    # no tables with data will be dropped
    assert drop_info.info["tables_with_data"] == []
    assert set(drop_info.schema.data_table_names(include_incomplete=True)) == all_in_schema
    source_clone = source_clone.with_resources("🦚Peacock", "🦚WidePeacock")
    source_clone.schema = pipeline.default_schema.clone()
    with pipeline.managed_state() as state:
        package_state = refresh_source(pipeline, source_clone, refresh="drop_data")
        # state modified
        assert state["sources"]["airtable_emojis"] == {"resources": {}}
    if seen_data:
        assert {t["name"] for t in package_state["truncated_tables"]} == tables_to_truncate
    else:
        assert package_state == {}
    assert set(source_clone.schema.data_table_names(include_incomplete=True)) == all_in_schema


@pytest.mark.parametrize("seen_data", [True, False], ids=["seen_data", "no_data"])
@pytest.mark.parametrize(
    # These are equivalent and will output all droppable columns
    "params",
    [
        {"from_resources": ["re:.*"], "from_tables": ["re:.*"], "columns": ["re:.*"]},
        {"from_resources": ["re:.*"], "columns": ["re:.*"]},
        {"from_resources": ["re:.*"], "from_tables": ["re:.*"]},
        {"from_resources": ["re:.*"]},
        {"from_tables": ["re:.*"]},
        {"columns": ["re:.*"]},
        {},
    ],
    ids=lambda x: ("+".join(x.keys()) or "default"),
)
def test_drop_helper_utils_drop_columns(seen_data: bool, params: Dict[str, Any]) -> None:
    pipeline = dlt.pipeline("test_drop_helpers_no_table_drop", destination="duckdb")
    # extract first which should produce tables that didn't seen data
    source = airtable_emojis().with_resources(
        "📆 Schedule", "🦚Peacock", "🦚WidePeacock", "💰Budget"
    )
    if seen_data:
        pipeline.run(source)
    else:
        pipeline.extract(source)

    # drop nothing
    drop_result = drop_columns(pipeline.default_schema.clone())
    assert drop_result.modified_tables == []
    assert drop_result.info["tables"] == []

    # attempt to drop all droppable columns with equivalent commands
    drop_result = drop_columns(schema=pipeline.default_schema.clone(), **params)

    # nothing should be selected as the source doesn't have droppable columns
    assert drop_result.modified_tables == []
    assert drop_result.info["tables"] == []

    # add a droppable columns to the peacock and wide_peacock resources
    def add_droppable_column(data_item):
        data_item["droppable_col1"] = 1
        data_item["droppable_col2"] = 1
        return data_item

    source.resources["🦚Peacock"].add_map(add_droppable_column)
    if seen_data:
        pipeline.run(source)
    else:
        pipeline.extract(source)

    drop_result = drop_columns(schema=pipeline.default_schema.clone(), columns=["re:.*"])
    if seen_data:
        assert drop_result.modified_tables[0]["name"] == "_peacock"
        assert drop_result.info["tables"] == ["_peacock"]
        assert drop_result.info["tables_with_data"] == ["_peacock"]
        assert drop_result.info["resource_states"] == []
        assert drop_result.info["state_paths"] == []
        assert drop_result.info["resource_names"] == ["🦚Peacock"]
        assert len(drop_result.info["warnings"]) == 3
        assert (
            "After dropping matched droppable columns ['value'] from table '_wide_peacock__peacock'"
            " only internal dlt columns will remain. This is not allowed."
            in drop_result.info["warnings"]
        )
        assert (
            "After dropping matched droppable columns ['value'] from table '_peacock__peacock' only"
            " internal dlt columns will remain. This is not allowed."
            in drop_result.info["warnings"]
        )
        assert (
            "After dropping matched droppable columns ['value'] from table '_schedule' only"
            " internal dlt columns will remain. This is not allowed."
            in drop_result.info["warnings"]
        )
        assert drop_result.info["schema_name"] == "airtable_emojis"
        assert drop_result.info["drop_all"] is False
        assert drop_result.info["resource_pattern"] == re.compile(".*")
        assert drop_result.info["drop_columns"] is True
        assert "droppable_col1" not in drop_result.schema.get_table_columns("_peacock")
        assert "droppable_col2" not in drop_result.schema.get_table_columns("_peacock")
    else:
        assert drop_result.modified_tables == []
        assert drop_result.info["tables"] == []
        assert drop_result.info["tables_with_data"] == []
        assert drop_result.info["resource_states"] == []
        assert drop_result.info["state_paths"] == []
        assert drop_result.info["resource_names"] == []
        assert drop_result.info["warnings"] == []
        assert drop_result.info["schema_name"] == "airtable_emojis"
        assert drop_result.info["drop_all"] is False
        assert drop_result.info["resource_pattern"] == re.compile(".*")
        assert drop_result.info["drop_columns"] is False


def test_drop_unknown_resource() -> None:
    pipeline = dlt.pipeline("test_drop_unknown_resource", destination="duckdb")
    # extract first which should produce tables that didn't seen data
    source = airtable_emojis().with_resources(
        "📆 Schedule", "🦚Peacock", "🦚WidePeacock", "💰Budget"
    )
    info = pipeline.run(source)
    assert_load_info(info)
    drop = DropCommand(pipeline, resources=["💰Budget"])
    assert drop.is_empty

    source.schema = pipeline.default_schema
    package_state = refresh_source(
        pipeline, source.with_resources("💰Budget"), refresh="drop_resources"
    )
    assert package_state == {}

    info = pipeline.run(source.with_resources("💰Budget"), refresh="drop_resources")
    # nothing loaded
    assert_load_info(info, 0)


def test_modified_state_in_package() -> None:
    pipeline = dlt.pipeline("test_modified_state_in_package", destination="duckdb")
    # extract first which should produce tables that didn't seen data
    source = airtable_emojis().with_resources(
        "📆 Schedule", "🦚Peacock", "🦚WidePeacock", "💰Budget"
    )
    pipeline.extract(source)
    # run again to change peacock state again
    info = pipeline.extract(source)
    normalize_storage = pipeline._get_normalize_storage()
    package_state = normalize_storage.extracted_packages.get_load_package_state(info.loads_ids[0])
    pipeline_state = decompress_state(package_state["pipeline_state"]["state"])
    assert pipeline_state["sources"]["airtable_emojis"] == {
        "resources": {"🦚Peacock": {"🦚🦚🦚": "🦚🦚"}}
    }

    # remove state
    info = pipeline.extract(airtable_emojis().with_resources("🦚Peacock"), refresh="drop_resources")
    normalize_storage = pipeline._get_normalize_storage()
    package_state = normalize_storage.extracted_packages.get_load_package_state(info.loads_ids[0])
    # nothing to drop
    assert "dropped_tables" not in package_state
    pipeline_state = decompress_state(package_state["pipeline_state"]["state"])
    # the state was reset to the original
    assert pipeline_state["sources"]["airtable_emojis"] == {
        "resources": {"🦚Peacock": {"🦚🦚🦚": "🦚"}}
    }


def test_drop_tables_force_extract_state() -> None:
    # if any tables will be dropped, state must be extracted even if it is not changed
    pipeline = dlt.pipeline("test_drop_tables_force_extract_state", destination="duckdb")
    source = airtable_emojis().with_resources(
        "📆 Schedule", "🦚Peacock", "🦚WidePeacock", "💰Budget"
    )
    info = pipeline.run(source)
    assert_load_info(info)
    # dropping schedule should not change the state
    info = pipeline.run(airtable_emojis().with_resources("📆 Schedule"), refresh="drop_resources")
    assert_load_info(info)
    storage = pipeline._get_load_storage()
    package_state = storage.get_load_package_state(info.loads_ids[0])
    assert package_state["dropped_tables"][0]["name"] == "_schedule"
    assert "pipeline_state" in package_state

    # here we drop and set state to original, so without forcing state extract state would not be present
    info = pipeline.run(airtable_emojis().with_resources("🦚Peacock"), refresh="drop_resources")
    assert_load_info(info)
    storage = pipeline._get_load_storage()
    package_state = storage.get_load_package_state(info.loads_ids[0])
    # child table also dropped
    assert len(package_state["dropped_tables"]) == 2
    assert "pipeline_state" in package_state
