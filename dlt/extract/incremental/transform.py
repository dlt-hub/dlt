from datetime import datetime, date  # noqa: I251
from typing import Any, Optional, Set, Tuple, List

from dlt.common.exceptions import MissingDependencyException
from dlt.common.utils import digest128
from dlt.common.json import json
from dlt.common.pendulum import pendulum
from dlt.common.typing import TDataItem
from dlt.common.jsonpath import find_values, JSONPathFields, compile_path
from dlt.extract.incremental.exceptions import (
    IncrementalCursorPathMissing,
    IncrementalPrimaryKeyMissing,
)
from dlt.extract.incremental.typing import TCursorValue, LastValueFunc
from dlt.extract.utils import resolve_column_value
from dlt.extract.items import TTableHintTemplate
from dlt.common.schema.typing import TColumnNames

try:
    from dlt.common.libs import pyarrow
    from dlt.common.libs.numpy import numpy
    from dlt.common.libs.pyarrow import pyarrow as pa, TAnyArrowItem
    from dlt.common.libs.pyarrow import from_arrow_scalar, to_arrow_scalar
except MissingDependencyException:
    pa = None
    pyarrow = None
    numpy = None

# NOTE: always import pandas independently from pyarrow
try:
    from dlt.common.libs.pandas import pandas, pandas_to_arrow
except MissingDependencyException:
    pandas = None


class IncrementalTransform:
    def __init__(
        self,
        resource_name: str,
        cursor_path: str,
        initial_value: Optional[TCursorValue],
        start_value: Optional[TCursorValue],
        end_value: Optional[TCursorValue],
        last_value_func: LastValueFunc[TCursorValue],
        primary_key: Optional[TTableHintTemplate[TColumnNames]],
        unique_hashes: Set[str],
    ) -> None:
        self.resource_name = resource_name
        self.cursor_path = cursor_path
        self.initial_value = initial_value
        self.start_value = start_value
        self.last_value = start_value
        self.end_value = end_value
        self.last_rows: List[TDataItem] = []
        self.last_value_func = last_value_func
        self.primary_key = primary_key
        self.unique_hashes = unique_hashes
        self.start_unique_hashes = set(unique_hashes)

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

    def compute_unique_value(
        self,
        row: TDataItem,
        primary_key: Optional[TTableHintTemplate[TColumnNames]],
    ) -> str:
        try:
            assert not self.deduplication_disabled, (
                f"{self.resource_name}: Attempt to compute unique values when deduplication is"
                " disabled"
            )

            if primary_key:
                return digest128(json.dumps(resolve_column_value(primary_key, row), sort_keys=True))
            elif primary_key is None:
                return digest128(json.dumps(row, sort_keys=True))
            else:
                return None
        except KeyError as k_err:
            raise IncrementalPrimaryKeyMissing(self.resource_name, k_err.args[0], row)

    def __call__(
        self,
        row: TDataItem,
    ) -> Tuple[bool, bool, bool]: ...

    @property
    def deduplication_disabled(self) -> bool:
        """Skip deduplication when length of the key is 0"""
        return isinstance(self.primary_key, (list, tuple)) and len(self.primary_key) == 0


class JsonIncremental(IncrementalTransform):
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
        if row is None:
            return row, False, False

        row_value = self.find_cursor_value(row)
        last_value = self.last_value
        last_value_func = self.last_value_func

        # For datetime cursor, ensure the value is a timezone aware datetime.
        # The object saved in state will always be a tz aware pendulum datetime so this ensures values are comparable
        if (
            isinstance(row_value, datetime)
            and row_value.tzinfo is None
            and isinstance(last_value, datetime)
            and last_value.tzinfo is not None
        ):
            row_value = pendulum.instance(row_value).in_tz("UTC")

        # Check whether end_value has been reached
        # Filter end value ranges exclusively, so in case of "max" function we remove values >= end_value
        if self.end_value is not None and (
            last_value_func((row_value, self.end_value)) != self.end_value
            or last_value_func((row_value,)) == self.end_value
        ):
            return None, False, True

        check_values = (row_value,) + ((last_value,) if last_value is not None else ())
        new_value = last_value_func(check_values)
        # new_value is "less" or equal to last_value (the actual max)
        if last_value == new_value:
            # use func to compute row_value into last_value compatible
            processed_row_value = last_value_func((row_value,))
            # skip the record that is not a start_value or new_value: that record was already processed
            check_values = (row_value,) + (
                (self.start_value,) if self.start_value is not None else ()
            )
            new_value = last_value_func(check_values)
            # Include rows == start_value but exclude "lower"
            # new_value is "less" or equal to start_value (the initial max)
            if new_value == self.start_value:
                # if equal there's still a chance that item gets in
                if processed_row_value == self.start_value:
                    if not self.deduplication_disabled:
                        unique_value = self.compute_unique_value(row, self.primary_key)
                        # if unique value exists then use it to deduplicate
                        if unique_value in self.start_unique_hashes:
                            return None, True, False
                else:
                    # smaller than start value: gets out
                    return None, True, False

            # we store row id for all records with the current "last_value" in state and use it to deduplicate
            if processed_row_value == last_value:
                # add new hash only if the record row id is same as current last value
                self.last_rows.append(row)
        else:
            self.last_value = new_value
            # store rows with "max" values to compute hashes after processing full batch
            self.last_rows = [row]
            self.unique_hashes = set()

        return row, False, False


class ArrowIncremental(IncrementalTransform):
    _dlt_index = "_dlt_index"

    def compute_unique_values(self, item: "TAnyArrowItem", unique_columns: List[str]) -> List[str]:
        if not unique_columns:
            return []
        rows = item.select(unique_columns).to_pylist()
        return [self.compute_unique_value(row, self.primary_key) for row in rows]

    def compute_unique_values_with_index(
        self, item: "TAnyArrowItem", unique_columns: List[str]
    ) -> List[Tuple[int, str]]:
        if not unique_columns:
            return []
        indices = item[self._dlt_index].to_pylist()
        rows = item.select(unique_columns).to_pylist()
        return [
            (index, self.compute_unique_value(row, self.primary_key))
            for index, row in zip(indices, rows)
        ]

    def _add_unique_index(self, tbl: "pa.Table") -> "pa.Table":
        """Creates unique index if necessary."""
        # create unique index if necessary
        if self._dlt_index not in tbl.schema.names:
            tbl = pyarrow.append_column(tbl, self._dlt_index, pa.array(numpy.arange(tbl.num_rows)))
        return tbl

    def __call__(
        self,
        tbl: "TAnyArrowItem",
    ) -> Tuple[TDataItem, bool, bool]:
        is_pandas = pandas is not None and isinstance(tbl, pandas.DataFrame)
        if is_pandas:
            tbl = pandas_to_arrow(tbl)

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

        start_out_of_range = end_out_of_range = False
        if not tbl:  # row is None or empty arrow table
            return tbl, start_out_of_range, end_out_of_range

        if self.last_value_func is max:
            compute = pa.compute.max
            end_compare = pa.compute.less
            last_value_compare = pa.compute.greater_equal
            new_value_compare = pa.compute.greater
        elif self.last_value_func is min:
            compute = pa.compute.min
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
            # NOTE: datetimes are always pendulum in UTC
            row_value = from_arrow_scalar(compute(tbl[cursor_path]))
            cursor_data_type = tbl.schema.field(cursor_path).type
            row_value_scalar = to_arrow_scalar(row_value, cursor_data_type)
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
            end_value_scalar = to_arrow_scalar(self.end_value, cursor_data_type)
            tbl = tbl.filter(end_compare(tbl[cursor_path], end_value_scalar))
            # Is max row value higher than end value?
            # NOTE: pyarrow bool *always* evaluates to python True. `as_py()` is necessary
            end_out_of_range = not end_compare(row_value_scalar, end_value_scalar).as_py()

        if self.start_value is not None:
            start_value_scalar = to_arrow_scalar(self.start_value, cursor_data_type)
            # Remove rows lower or equal than the last start value
            keep_filter = last_value_compare(tbl[cursor_path], start_value_scalar)
            start_out_of_range = bool(pa.compute.any(pa.compute.invert(keep_filter)).as_py())
            tbl = tbl.filter(keep_filter)
            if not self.deduplication_disabled:
                # Deduplicate after filtering old values
                tbl = self._add_unique_index(tbl)
                # Remove already processed rows where the cursor is equal to the start value
                eq_rows = tbl.filter(pa.compute.equal(tbl[cursor_path], start_value_scalar))
                # compute index, unique hash mapping
                unique_values_index = self.compute_unique_values_with_index(eq_rows, unique_columns)
                unique_values_index = [
                    (i, uq_val)
                    for i, uq_val in unique_values_index
                    if uq_val in self.start_unique_hashes
                ]
                # find rows with unique ids that were stored from previous run
                remove_idx = pa.array(i for i, _ in unique_values_index)
                # Filter the table
                tbl = tbl.filter(
                    pa.compute.invert(pa.compute.is_in(tbl[self._dlt_index], remove_idx))
                )

        if (
            self.last_value is None
            or new_value_compare(
                row_value_scalar, to_arrow_scalar(self.last_value, cursor_data_type)
            ).as_py()
        ):  # Last value has changed
            self.last_value = row_value
            if not self.deduplication_disabled:
                # Compute unique hashes for all rows equal to row value
                self.unique_hashes = set(
                    self.compute_unique_values(
                        tbl.filter(pa.compute.equal(tbl[cursor_path], row_value_scalar)),
                        unique_columns,
                    )
                )
        elif self.last_value == row_value and not self.deduplication_disabled:
            # last value is unchanged, add the hashes
            self.unique_hashes.update(
                set(
                    self.compute_unique_values(
                        tbl.filter(pa.compute.equal(tbl[cursor_path], row_value_scalar)),
                        unique_columns,
                    )
                )
            )

        if len(tbl) == 0:
            return None, start_out_of_range, end_out_of_range
        try:
            tbl = pyarrow.remove_columns(tbl, ["_dlt_index"])
        except KeyError:
            pass
        if is_pandas:
            return tbl.to_pandas(), start_out_of_range, end_out_of_range
        return tbl, start_out_of_range, end_out_of_range
