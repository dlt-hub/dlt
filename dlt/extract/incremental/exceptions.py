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
            or f"Cursor element with JSON path `{json_path}` has the value `None` in extracted data item. All data items must contain a value != None. Construct the incremental with `on_cursor_value_missing='include'` if you want to include such rows"
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


class IncrementalCursorThresholdExceeded(PipeException):
    def __init__(self, pipe_name: str, cursor_path: str, hash_count: int, threshold: int) -> None:
        self.cursor_path = cursor_path
        self.hash_count = hash_count
        self.threshold = threshold
        msg = (
            f"Number of records ({hash_count}) sharing the same value of cursor field"
            f" `{cursor_path}` exceeded `duplicate_cursor_error_threshold` ({threshold}). dlt keeps"
            " one deduplication hash per boundary record in the pipeline state, so this many"
            " records at the boundary value would write a large state on every run. Use a cursor"
            " column with higher resolution, or switch to `merge_key` together with"
            " `range_start='open'` to disable boundary deduplication, or raise"
            " `duplicate_cursor_error_threshold` if a large state is acceptable."
        )
        super().__init__(pipe_name, msg)


class JoinSchedulerError(PipeException):
    def __init__(self, pipe_name: str, msg: str) -> None:
        super().__init__(pipe_name, msg)


class ExternalSchedulerNotAvailable(PipeException):
    def __init__(self, pipe_name: str) -> None:
        super().__init__(
            pipe_name,
            "External scheduler interval is not available. The resource has"
            " allow_external_schedulers=True but no interval was provided by the runtime"
            " (no DLT_INTERVAL_START/DLT_INTERVAL_END env vars, no Airflow context, and no"
            " interval injected by the launcher).",
        )
