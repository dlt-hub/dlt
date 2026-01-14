from typing import Any, Dict, Literal, Optional

from dlt.destinations.impl.clickhouse.clickhouse_adapter import clickhouse_adapter
from dlt.destinations.impl.clickhouse.typing import TTableEngineType
from dlt.extract.resource import DltResource
from tests.common.test_validation import TTableHintTemplate


CREATE_DISTRIBUTED_TABLE_HINT: Literal["x-create-distributed-table"] = "x-create-distributed-table"
DISTRIBUTED_TABLE_SUFFIX_HINT: Literal["x-distributed-table-suffix"] = "x-distributed-table-suffix"
SHARDING_KEY_HINT: Literal["x-sharding-key"] = "x-sharding-key"

# maps ClickHouseClusterClientConfiguration keys to corresponding table hints
CONFIG_HINT_MAP = {
    "create_distributed_tables": CREATE_DISTRIBUTED_TABLE_HINT,
    "distributed_table_suffix": DISTRIBUTED_TABLE_SUFFIX_HINT,
    "sharding_key": SHARDING_KEY_HINT,
}


def clickhouse_cluster_adapter(
    data: Any,
    table_engine_type: TTableEngineType = None,
    create_distributed_table: Optional[bool] = None,
    distributed_table_suffix: Optional[str] = None,
    sharding_key: Optional[str] = None,
) -> DltResource:
    resource = clickhouse_adapter(
        data,
        table_engine_type=table_engine_type,
    )

    additional_table_hints: Dict[str, TTableHintTemplate[Any]] = {}

    if create_distributed_table is not None:
        additional_table_hints[CREATE_DISTRIBUTED_TABLE_HINT] = create_distributed_table

    if distributed_table_suffix is not None:
        additional_table_hints[DISTRIBUTED_TABLE_SUFFIX_HINT] = distributed_table_suffix

    if sharding_key is not None:
        additional_table_hints[SHARDING_KEY_HINT] = sharding_key

    # convert to None if empty, to prevent removing existing hints
    additional_table_hints = additional_table_hints or None

    resource.apply_hints(additional_table_hints=additional_table_hints)

    return resource
