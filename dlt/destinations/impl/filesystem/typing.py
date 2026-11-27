from datetime import datetime
from typing import Callable, Dict, Union

from typing_extensions import TypeAlias


TCurrentDateTimeCallback: TypeAlias = Callable[[], datetime]
"""A callback function which returns a `datetime`"""

TCurrentDateTime: TypeAlias = Union[datetime, TCurrentDateTimeCallback]
"""A `datetime` or a callable which returns a `datetime`. A naive value is UTC"""

TLayoutPlaceholderCallback: TypeAlias = Callable[[str, str, str, str, str], str]
"""A callback which should return prepared string value the following arguments passed
`schema name`, `table name`, `load_id`, `file_id` and an `extension`
"""

TExtraPlaceholders: TypeAlias = Dict[
    str, Union[Union[str, int, datetime], TLayoutPlaceholderCallback]
]
"""Extra placeholders for filesystem layout"""
