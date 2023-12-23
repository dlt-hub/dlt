from datetime import datetime, date  # noqa: I251
from typing import Any, Optional, Tuple, List

try:
    import pandas as pd
except ModuleNotFoundError:
    pd = None

try:
    import numpy as np
except ModuleNotFoundError:
    np = None

from dlt.common.exceptions import MissingDependencyException
from dlt.common.utils import digest128
from dlt.common.json import json
from dlt.common import pendulum
from dlt.common.typing import TDataItem, TDataItems
from dlt.common.jsonpath import TJsonPath, find_values, JSONPathFields, compile_path
from dlt.extract.incremental.exceptions import (
    IncrementalCursorPathMissing,
    IncrementalPrimaryKeyMissing,
)
from dlt.extract.incremental.typing import IncrementalColumnState, TCursorValue, LastValueFunc
from dlt.extract.utils import resolve_column_value
from dlt.extract.typing import TTableHintTemplate
from dlt.common.schema.typing import TColumnNames

try:
    from dlt.common.libs import pyarrow
    from dlt.common.libs.pyarrow import pyarrow as pa, TAnyArrowItem
except MissingDependencyException:
    pa = None
    pyarrow = None


class IncrementalTransform:
    def __init__(
        self,
        resource_name: str,
        cursor_path: str,
        start_value: Optional[TCursorValue],
        end_value: Optional[TCursorValue],
        incremental_state: IncrementalColumnState,
        last_value_func: LastValueFunc[TCursorValue],
        primary_key: Optional[TTableHintTemplate[TColumnNames]],
    ) -> None:
        self.resource_name = resource_name
        self.cursor_path = cursor_path
        self.start_value = start_value
        self.end_value = end_value
        self.incremental_state = incremental_state
        self.last_value_func = last_value_func
        self.primary_key = primary_key

        # compile jsonpath
        self._compiled_cursor_path = compile_path(cursor_path)
        # for simple column name we'll fallback to search in dict
        if (
            isinstance(self._compiled_cursor_path, JSONPathFields)
            and len(self._compiled_cursor_path.fields) == 1
            and self._compiled_cursor_path.fields[0] != "*"
        ):
            self.cursor_path = self._compiled_cursor_path.fields[0]
            self._compiled_cursor_path = None

    def __call__(
        self,
        row: TDataItem,
    ) -> Tuple[bool, bool, bool]: ...


class JsonIncremental(IncrementalTransform):
    def unique_value(
        self,
        row: TDataItem,
        primary_key: Optional[TTableHintTemplate[TColumnNames]],
        resource_name: str,
    ) -> str:
        try:
            if primary_key:
                return digest128(json.dumps(resolve_column_value(primary_key, row), sort_keys=True))
            elif primary_key is None:
                return digest128(json.dumps(row, sort_keys=True))
            else:
                return None
        except KeyError as k_err:
            raise IncrementalPrimaryKeyMissing(resource_name, k_err.args[0], row)

    def find_cursor_value(self, row: TDataItem) -> Any:
        """Finds value in row at cursor defined by self.cursor_path.

        Will use compiled JSONPath if present, otherwise it reverts to column search if row is dict
        """
        row_value: Any = None
        if self._compiled_cursor_path:
            row_values = find_values(self._compiled_cursor_path, row)
            if row_values:
                row_value = row_values[0]
        else:
            try:
                row_value = row[self.cursor_path]
            except Exception:
                pass
        if row_value is None:
            raise IncrementalCursorPathMissing(self.resource_name, self.cursor_path, row)
        return row_value

    def __call__(
        self,
        row: TDataItem,
    ) -> Tuple[Optional[TDataItem], bool, bool]:
        """
        Returns:
            Tuple (row, start_out_of_range, end_out_of_range) where row is either the data item or `None` if it is completely filtered out
        """
        start_out_of_range = end_out_of_range = False
        if row is None:
            return row, start_out_of_range, end_out_of_range

        row_value = self.find_cursor_value(row)

        # For datetime cursor, ensure the value is a timezone aware datetime.
        # The object saved in state will always be a tz aware pendulum datetime so this ensures values are comparable
        if isinstance(row_value, datetime):
            row_value = pendulum.instance(row_value)

        last_value = self.incremental_state["last_value"]

        # Check whether end_value has been reached
        # Filter end value ranges exclusively, so in case of "max" function we remove values >= end_value
        if self.end_value is not None and (
            self.last_value_func((row_value, self.end_value)) != self.end_value
            or self.last_value_func((row_value,)) == self.end_value
        ):
            end_out_of_range = True
            return None, start_out_of_range, end_out_of_range

        check_values = (row_value,) + ((last_value,) if last_value is not None else ())
        new_value = self.last_value_func(check_values)
        if last_value == new_value:
            processed_row_value = self.last_value_func((row_value,))
            # we store row id for all records with the current "last_value" in state and use it to deduplicate

            if processed_row_value == last_value:
                unique_value = self.unique_value(row, self.primary_key, self.resource_name)
                # if unique value exists then use it to deduplicate
                if unique_value:
                    if unique_value in self.incremental_state["unique_hashes"]:
                        return None, start_out_of_range, end_out_of_range
                    # add new hash only if the record row id is same as current last value
                    self.incremental_state["unique_hashes"].append(unique_value)
                return row, start_out_of_range, end_out_of_range
            # skip the record that is not a last_value or new_value: that record was already processed
            check_values = (row_value,) + (
                (self.start_value,) if self.start_value is not None else ()
            )
            new_value = self.last_value_func(check_values)
            # Include rows == start_value but exclude "lower"
            if new_value == self.start_value and processed_row_value != self.start_value:
                start_out_of_range = True
                return None, start_out_of_range, end_out_of_range
            else:
                return row, start_out_of_range, end_out_of_range
        else:
            self.incremental_state["last_value"] = new_value
            unique_value = self.unique_value(row, self.primary_key, self.resource_name)
            if unique_value:
                self.incremental_state["unique_hashes"] = [unique_value]

        return row, start_out_of_range, end_out_of_range


class ArrowIncremental(IncrementalTransform):
    _dlt_index = "_dlt_index"

    def unique_values(
        self, item: "TAnyArrowItem", unique_columns: List[str], resource_name: str
    ) -> List[Tuple[int, str]]:
        if not unique_columns:
            return []
        item = item
        indices = item[self._dlt_index].to_pylist()
        rows = item.select(unique_columns).to_pylist()
        return [
            (index, digest128(json.dumps(row, sort_keys=True))) for index, row in zip(indices, rows)
        ]

    def _deduplicate(
        self, tbl: "pa.Table", unique_columns: Optional[List[str]], aggregate: str, cursor_path: str
    ) -> "pa.Table":
        """Creates unique index if necessary."""
        # create unique index if necessary
        if self._dlt_index not in tbl.schema.names:
            tbl = pyarrow.append_column(tbl, self._dlt_index, pa.array(np.arange(tbl.num_rows)))
        return tbl

    def __call__(
        self,
        tbl: "TAnyArrowItem",
    ) -> Tuple[TDataItem, bool, bool]:
        is_pandas = pd is not None and isinstance(tbl, pd.DataFrame)
        if is_pandas:
            tbl = pa.Table.from_pandas(tbl)

        primary_key = self.primary_key(tbl) if callable(self.primary_key) else self.primary_key
        if primary_key:
            # create a list of unique columns
            if isinstance(primary_key, str):
                unique_columns = [primary_key]
            else:
                unique_columns = list(primary_key)
            # check if primary key components are in the table
            for pk in unique_columns:
                if pk not in tbl.schema.names:
                    raise IncrementalPrimaryKeyMissing(self.resource_name, pk, tbl)
            # use primary key as unique index
            if isinstance(primary_key, str):
                self._dlt_index = primary_key
        elif primary_key is None:
            unique_columns = tbl.schema.names
        else:  # deduplicating is disabled
            unique_columns = None

        start_out_of_range = end_out_of_range = False
        if not tbl:  # row is None or empty arrow table
            return tbl, start_out_of_range, end_out_of_range

        last_value = self.incremental_state["last_value"]

        if self.last_value_func is max:
            compute = pa.compute.max
            aggregate = "max"
            end_compare = pa.compute.less
            last_value_compare = pa.compute.greater_equal
            new_value_compare = pa.compute.greater
        elif self.last_value_func is min:
            compute = pa.compute.min
            aggregate = "min"
            end_compare = pa.compute.greater
            last_value_compare = pa.compute.less_equal
            new_value_compare = pa.compute.less
        else:
            raise NotImplementedError(
                "Only min or max last_value_func is supported for arrow tables"
            )

        # TODO: Json path support. For now assume the cursor_path is a column name
        cursor_path = self.cursor_path
        # The new max/min value
        try:
            orig_row_value = compute(tbl[cursor_path])
            row_value = orig_row_value.as_py()
            # dates are not represented as datetimes but I see connector-x represents
            # datetimes as dates and keeping the exact time inside. probably a bug
            # but can be corrected this way
            if isinstance(row_value, date) and not isinstance(row_value, datetime):
                row_value = pendulum.from_timestamp(orig_row_value.cast(pa.int64()).as_py() / 1000)
        except KeyError as e:
            raise IncrementalCursorPathMissing(
                self.resource_name,
                cursor_path,
                tbl,
                f"Column name {cursor_path} was not found in the arrow table. Not nested JSON paths"
                " are not supported for arrow tables and dataframes, the incremental cursor_path"
                " must be a column name.",
            ) from e

        # If end_value is provided, filter to include table rows that are "less" than end_value
        if self.end_value is not None:
            tbl = tbl.filter(end_compare(tbl[cursor_path], self.end_value))
            # Is max row value higher than end value?
            # NOTE: pyarrow bool *always* evaluates to python True. `as_py()` is necessary
            end_out_of_range = not end_compare(row_value, self.end_value).as_py()

        if last_value is not None:
            if self.start_value is not None:
                # Remove rows lower than the last start value
                keep_filter = last_value_compare(tbl[cursor_path], self.start_value)
                start_out_of_range = bool(pa.compute.any(pa.compute.invert(keep_filter)).as_py())
                tbl = tbl.filter(keep_filter)

            # Deduplicate after filtering old values
            tbl = self._deduplicate(tbl, unique_columns, aggregate, cursor_path)
            # Remove already processed rows where the cursor is equal to the last value
            eq_rows = tbl.filter(pa.compute.equal(tbl[cursor_path], last_value))
            # compute index, unique hash mapping
            unique_values = self.unique_values(eq_rows, unique_columns, self.resource_name)
            unique_values = [
                (i, uq_val)
                for i, uq_val in unique_values
                if uq_val in self.incremental_state["unique_hashes"]
            ]
            remove_idx = pa.array(i for i, _ in unique_values)
            # Filter the table
            tbl = tbl.filter(pa.compute.invert(pa.compute.is_in(tbl[self._dlt_index], remove_idx)))

            if (
                new_value_compare(row_value, last_value).as_py() and row_value != last_value
            ):  # Last value has changed
                self.incremental_state["last_value"] = row_value
                # Compute unique hashes for all rows equal to row value
                self.incremental_state["unique_hashes"] = [
                    uq_val
                    for _, uq_val in self.unique_values(
                        tbl.filter(pa.compute.equal(tbl[cursor_path], row_value)),
                        unique_columns,
                        self.resource_name,
                    )
                ]
            else:
                # last value is unchanged, add the hashes
                self.incremental_state["unique_hashes"] = list(
                    set(
                        self.incremental_state["unique_hashes"]
                        + [uq_val for _, uq_val in unique_values]
                    )
                )
        else:
            tbl = self._deduplicate(tbl, unique_columns, aggregate, cursor_path)
            self.incremental_state["last_value"] = row_value
            self.incremental_state["unique_hashes"] = [
                uq_val
                for _, uq_val in self.unique_values(
                    tbl.filter(pa.compute.equal(tbl[cursor_path], row_value)),
                    unique_columns,
                    self.resource_name,
                )
            ]

        if len(tbl) == 0:
            return None, start_out_of_range, end_out_of_range
        try:
            tbl = pyarrow.remove_columns(tbl, ["_dlt_index"])
        except KeyError:
            pass
        if is_pandas:
            return tbl.to_pandas(), start_out_of_range, end_out_of_range
        return tbl, start_out_of_range, end_out_of_range
