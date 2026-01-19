from typing import Literal, Dict, Set, Union

from dlt.common.schema import TColumnHint
from dlt.common.typing import get_args


TSecureConnection = Literal[0, 1]
TTableEngineType = Literal[
    "merge_tree",
    "shared_merge_tree",
    "replicated_merge_tree",
]
TColumnName = str
TSQLExprOrColumnSeq = Union[str, list[TColumnName], tuple[TColumnName]]
TMergeTreeSettingsValue = Union[str, int, float, bool]
TMergeTreeSettings = Dict[str, TMergeTreeSettingsValue]
TCodecExpr = str
TColumnCodecs = Dict[TColumnName, TCodecExpr]

HINT_TO_CLICKHOUSE_ATTR: Dict[TColumnHint, str] = {
    "primary_key": "PRIMARY KEY",
    "unique": "",  # No unique constraints available in ClickHouse.
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
SORT_HINT: Literal["x-clickhouse-sort"] = "x-clickhouse-sort"
PARTITION_HINT: Literal["x-clickhouse-partition"] = "x-clickhouse-partition"
SETTINGS_HINT: Literal["x-clickhouse-settings"] = "x-clickhouse-settings"
CODEC_HINT: Literal["x-clickhouse-codec"] = "x-clickhouse-codec"
