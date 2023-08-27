from dlt.common.exceptions import DltException

class NormalizeException(DltException):
    def __init__(self, msg: str) -> None:
        super().__init__(msg)

class SchemaFrozenException(DltException):
    def __init__(self, msg: str) -> None:
        super().__init__(msg)