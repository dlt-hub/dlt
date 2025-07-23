from typing import Tuple, Union, Optional

from dlt.common.typing import TimedeltaSeconds

# Either a single timeout or tuple (connect,read) timeout
TRequestTimeout = Union[TimedeltaSeconds, Tuple[TimedeltaSeconds, TimedeltaSeconds]]
