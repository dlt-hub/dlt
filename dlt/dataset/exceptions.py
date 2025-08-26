from typing import Optional
from dlt.common.exceptions import DltException


class DatasetException(DltException):
    pass


class RelationHasQueryException(DatasetException):
    def __init__(self, attempted_change: str) -> None:
        msg = (
            "This readable relation was created with a provided sql query. You cannot change"
            f" `{attempted_change}`. Please change the orignal sql query."
        )
        super().__init__(msg)


class RelationUnknownColumnException(DatasetException):
    def __init__(self, column_name: str) -> None:
        msg = (
            f"The selected column `{column_name}` is not known in the dlt schema for this relation."
        )
        super().__init__(msg)


class LineageFailedException(DltException):
    def __init__(self, msg: Optional[str] = None, *, resource_name: Optional[str] = None):
        super().__init__(msg)
        self.resource_name = resource_name
