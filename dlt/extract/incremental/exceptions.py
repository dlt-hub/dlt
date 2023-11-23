from dlt.extract.exceptions import PipeException
from dlt.common.typing import TDataItem


class IncrementalCursorPathMissing(PipeException):
    def __init__(self, pipe_name: str, json_path: str, item: TDataItem, msg: str = None) -> None:
        self.json_path = json_path
        self.item = item
        msg = (
            msg
            or f"Cursor element with JSON path {json_path} was not found in extracted data item. All data items must contain this path. Use the same names of fields as in your JSON document - if those are different from the names you see in database."
        )
        super().__init__(pipe_name, msg)


class IncrementalPrimaryKeyMissing(PipeException):
    def __init__(self, pipe_name: str, primary_key_column: str, item: TDataItem) -> None:
        self.primary_key_column = primary_key_column
        self.item = item
        msg = (
            f"Primary key column {primary_key_column} was not found in extracted data item. All"
            " data items must contain this column. Use the same names of fields as in your JSON"
            " document."
        )
        super().__init__(pipe_name, msg)
