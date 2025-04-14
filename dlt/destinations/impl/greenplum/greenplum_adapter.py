from typing import Dict, Literal, Set

from dlt.common.schema import TColumnHint
from dlt.common.typing import get_args

GreenplumStorageType = Literal[
    "appendonly", "blocksize", "compresstype", "compresslevel", "orientation"
]

HINT_TO_GREENPLUM_ATTR: Dict[TColumnHint, str] = {
    "distributed_by": "DISTRIBUTED BY",
    "distributed_randomly": "DISTRIBUTED RANDOMLY"
}