from typing import Callable, Optional, Union

from pendulum.datetime import DateTime
from typing_extensions import TypeAlias


TCurrentDateTime: TypeAlias = Optional[DateTime]
"""pendulum.DateTime instance or a callable which should return pendulum.DateTime"""

TLayoutParamCallback: TypeAlias = Callable[[str, str, str, str, str, DateTime], str]
"""A callback which should return prepared string value the following arguments passed
`schema name`, `table name`, `load_id`, `file_id`, `extension` and `current_datetime`.
"""
