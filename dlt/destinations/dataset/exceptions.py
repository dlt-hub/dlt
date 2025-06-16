from dlt.common.exceptions import DltException


class DatasetException(DltException):
    pass


class ReadableRelationUnknownColumnException(DatasetException):
    def __init__(self, column_name: str) -> None:
        msg = (
            f"The selected column `{column_name}` is not known in the dlt schema for this relation."
        )
        super().__init__(msg)
