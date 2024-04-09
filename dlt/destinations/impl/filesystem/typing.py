from typing import Callable, Dict, Union

from pendulum.datetime import DateTime
from typing_extensions import TypeAlias


TCurrentDateTimeCallback: TypeAlias = Callable[[], DateTime]
"""A callback function to which should return pendulum.DateTime instance"""

TCurrentDateTime: TypeAlias = Union[DateTime, TCurrentDateTimeCallback]
"""pendulum.DateTime instance or a callable which should return pendulum.DateTime"""

TLayoutPlaceholderCallback: TypeAlias = Callable[[str, str, str, str, str], str]
"""A callback which should return prepared string value the following arguments passed
`schema name`, `table name`, `load_id`, `file_id` and an `extension`
"""

TExtraPlaceholders: TypeAlias = Dict[str, Union[str, TLayoutPlaceholderCallback]]
"""Extra placeholders for filesystem layout"""
