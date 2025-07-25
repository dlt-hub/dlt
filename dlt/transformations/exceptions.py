from typing import Optional

from dlt.common.exceptions import DltException
from dlt.extract.exceptions import DltResourceException


class LineageFailedException(DltException):
    def __init__(self, msg: Optional[str] = None, *, resource_name: Optional[str] = None):
        super().__init__(msg)
        self.resource_name = resource_name
