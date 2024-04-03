from typing import Callable, Optional, Union

from pendulum.datetime import DateTime
from typing_extensions import TypeAlias


TCurrentDatetimeCallback: TypeAlias = Callable[[], DateTime]
"""A callback which should return current datetime"""

TCurrentDateTime: TypeAlias = Optional[Union[DateTime, TCurrentDatetimeCallback]]
"""pendulum.DateTime instance or a callable which should return pendulum.DateTime"""

TDatetimeFormat: TypeAlias = str
"""Datetime format or formatter callback"""

TLayoutParamCallback: TypeAlias = Callable[[str, str, str, str, str, DateTime], str]
"""A callback which should return prepared string value the following arguments passed
`schema name`, `table name`, `load_id`, `file_id`, `extension` and `current_datetime`.
"""
