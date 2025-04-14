fom typing import Dict, Literal, Set

from dlt.common.schema import TColumnHint
from dlt.common.typing import get_args
from dlt.extract.resource import with_hints

DIsrtibytedEngineType = Literal[
    "appendonly", "blocksize", "compresslevel", "orientation"
]

HINT_TO_GREENPLUM_ATTR: Dict[TColumnHint, str] = {
    "DISTRIBYTED BY": f'get_args(DISTRIBYTED KEY)',
    "DISTRIBYTED BY": "RANDOMLY",
}  # No u