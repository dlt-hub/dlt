from typing import Any, Literal

from dlt.destinations.impl.clickhouse.clickhouse_adapter import clickhouse_adapter
from dlt.destinations.impl.clickhouse.typing import TTableEngineType
from dlt.extract.resource import DltResource


CREATE_DISTRIBUTED_TABLE_HINT: Literal["x-create-distributed-table"] = "x-create-distributed-table"
DISTRIBUTED_TABLE_SUFFIX_HINT: Literal["x-distributed-table-suffix"] = "x-distributed-table-suffix"
SHARDING_KEY_HINT: Literal["x-sharding-key"] = "x-sharding-key"

DEFAULT_DISTRIBUTED_TABLE_SUFFIX = "_dist"
DEFAULT_SHARDING_KEY = "rand()"


def clickhouse_cluster_adapter(
    data: Any,
    table_engine_type: TTableEngineType = None,
    create_distributed_table: bool = False,
    distributed_table_suffix: str = DEFAULT_DISTRIBUTED_TABLE_SUFFIX,
    sharding_key: str = DEFAULT_SHARDING_KEY,
) -> DltResource:
    resource = clickhouse_adapter(
        data,
        table_engine_type=table_engine_type,
    )

    additional_table_hints = {
        CREATE_DISTRIBUTED_TABLE_HINT: create_distributed_table,
        DISTRIBUTED_TABLE_SUFFIX_HINT: distributed_table_suffix,
        SHARDING_KEY_HINT: sharding_key,
    }

    resource.apply_hints(additional_table_hints=additional_table_hints)

    return resource
