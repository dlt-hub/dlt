from typing import Literal, Dict, get_args, Set

from dlt.common.schema import TColumnHint

TSecureConnection = Literal[0, 1]
TTableEngineType = Literal[
    "merge_tree",
    "shared_merge_tree",
    "replicated_merge_tree",
]

HINT_TO_CLICKHOUSE_ATTR: Dict[TColumnHint, str] = {
    "primary_key": "PRIMARY KEY",
    "unique": "",  # No unique constraints available in ClickHouse.
    "foreign_key": "",  # No foreign key constraints support in ClickHouse.
}

TABLE_ENGINE_TYPE_TO_CLICKHOUSE_ATTR: Dict[TTableEngineType, str] = {
    "merge_tree": "MergeTree",
    "shared_merge_tree": "SharedMergeTree",
    "replicated_merge_tree": "ReplicatedMergeTree",
}

TDeployment = Literal["ClickHouseOSS", "ClickHouseCloud"]

SUPPORTED_FILE_FORMATS = Literal["jsonl", "parquet"]
FILE_FORMAT_TO_TABLE_FUNCTION_MAPPING: Dict[SUPPORTED_FILE_FORMATS, str] = {
    "jsonl": "JSONEachRow",
    "parquet": "Parquet",
}
TABLE_ENGINE_TYPES: Set[TTableEngineType] = set(get_args(TTableEngineType))
TABLE_ENGINE_TYPE_HINT: Literal["x-table-engine-type"] = "x-table-engine-type"
