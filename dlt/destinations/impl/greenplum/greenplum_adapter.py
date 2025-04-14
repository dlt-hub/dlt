from typing import Dict, Literal, Set

from dlt.common.schema import TColumnHint
from dlt.common.typing import get_args

DIsrtibytedEngineType = Literal[
    "appendonly", "blocksize", "compresslevel", "orientation"
]

HINT_TO_GREENPLUM_ATTR: Dict[TColumnHint, str] = {
    "distibyted by": get_args(DISTIRBYTED_BY),
    "distibyted by": "randomly",  # No unique constraints available in ClickHouse.