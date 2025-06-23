from typing import Any, List, Dict
import pytest

from dlt.pipeline import helpers, Pipeline
from dlt.pipeline.drop import DISQUALIFYING_HINTS
from dlt.common.schema.utils import (
    is_nullable_column,
    group_tables_by_resource,
    compile_simple_regexes,
)
from dlt.common.schema.typing import TSimpleRegex, DLT_NAME_PREFIX

from tests.load.pipeline.test_drop import droppable_source
from tests.load.utils import destinations_configs, DestinationTestConfiguration
from tests.pipeline.utils import assert_load_info, load_table_counts


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True,
        local_filesystem_configs=True,
        all_buckets_filesystem_configs=True,
        table_format_filesystem_configs=True,
    ),
    ids=lambda x: x.name,
)
def test_drop_column_command_resources(destination_config: DestinationTestConfiguration) -> None:
    """Test the drop command with resource and verify correct data is deleted from destination"""
    source: Any = droppable_source()

    pipeline = destination_config.setup_pipeline("droppbale", dev_mode=True)
    info = pipeline.run(source, **destination_config.run_kwargs)
    assert_load_info(info)

    # TODO: remove this because repeated
    all_tables = list(pipeline.default_schema.tables.keys())
    assert load_table_counts(pipeline, *all_tables) == {
        "_dlt_version": 1,
        "_dlt_loads": 1,
        "droppable_a": 2,
        "droppable_b": 1,
        "droppable_c": 1,
        "droppable_d": 2,
        "droppable_no_state": 3,
        "_dlt_pipeline_state": 1,
        "droppable_b__items": 2,
        "droppable_c__items": 1,
        "droppable_c__items__labels": 2,
    }

    from_resources = ["droppable_a", "droppable_b", "droppable_c", "droppable_d"]

    # This should drop all droppable columns
    # If the table's droppable columns leave only dlt tables, nothing will be dropped
    drop_cmd = helpers.DropCommand(
        pipeline,
        from_resources=from_resources,
    )
    must_drop_infos = drop_cmd.from_tables_drop_cols

    assert (
        "After dropping matched droppable columns ['name', 'value'] from table"
        " 'droppable_c__items__labels' only internal dlt columns will remain. This is not allowed."
        in drop_cmd.info["warnings"]
    )
    assert (
        "After dropping matched droppable columns ['k', 'r'] from table 'droppable_c__items' only"
        " internal dlt columns will remain. This is not allowed."
        in drop_cmd.info["warnings"]
    )
    assert (
        "After dropping matched droppable columns ['m', 'n'] from table 'droppable_b__items' only"
        " internal dlt columns will remain. This is not allowed."
        in drop_cmd.info["warnings"]
    )

    drop_cmd()

    for must_drop_info in must_drop_infos:
        remaining_cols = pipeline.default_schema.get_table_columns(must_drop_info["from_table"])
        for dropped_col in must_drop_info["drop_columns"]:
            assert dropped_col not in remaining_cols
