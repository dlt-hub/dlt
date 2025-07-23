from typing import Any

from dlt.extract.exceptions import PipeException
from dlt.common.typing import TDataItem


class IncrementalCursorPathMissing(PipeException):
    def __init__(
        self, pipe_name: str, json_path: str, item: TDataItem = None, msg: str = None
    ) -> None:
        self.json_path = json_path
        self.item = item
        msg = (
            msg
            or f"Cursor element with JSON path `{json_path}` was not found in extracted data item. All data items must contain this path. Use the same names of fields as in your JSON document because they can be different from the names you see in database."
        )
        super().__init__(pipe_name, msg)


class IncrementalCursorPathHasValueNone(PipeException):
    def __init__(
        self, pipe_name: str, json_path: str, item: TDataItem = None, msg: str = None
    ) -> None:
        self.json_path = json_path
        self.item = item
        msg = (
            msg
            or f"Cursor element with JSON path `{json_path}` has the value `None` in extracted data item. All data items must contain a value != None. Construct the incremental with `on_cursor_value_none='include'` if you want to include such rows"
        )
        super().__init__(pipe_name, msg)


class IncrementalCursorInvalidCoercion(PipeException):
    def __init__(
        self,
        pipe_name: str,
        cursor_path: str,
        cursor_value: TDataItem,
        cursor_value_type: str,
        item: TDataItem,
        item_type: Any,
        details: str,
    ) -> None:
        self.cursor_path = cursor_path
        self.cursor_value = cursor_value
        self.cursor_value_type = cursor_value_type
        self.item = item
        msg = (
            f"Could not coerce `{cursor_value_type}` with value `{cursor_value}` and type"
            f" `{type(cursor_value)}` to actual data item `{item}` at path `{cursor_path}` with"
            f" type `{item_type}`: {details}. You need to use different data type for"
            f" `{cursor_value_type}` or cast your data ie. by using `.add_map()` on this resource."
        )
        super().__init__(pipe_name, msg)


class IncrementalPrimaryKeyMissing(PipeException):
    def __init__(self, pipe_name: str, primary_key_column: str, item: TDataItem) -> None:
        self.primary_key_column = primary_key_column
        self.item = item
        msg = (
            f"Primary key column `{primary_key_column}` was not found in extracted data item. All"
            " data items must contain this column. Use the same names of fields as in your JSON"
            " document."
        )
        super().__init__(pipe_name, msg)
