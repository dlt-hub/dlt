from datetime import datetime  # noqa: I251
from typing import Any, Optional, Set, Tuple, List, Type

from dlt.common.exceptions import MissingDependencyException
from dlt.common.utils import digest128
from dlt.common.json import json
from dlt.common.pendulum import pendulum
from dlt.common.typing import TDataItem, TColumnNames
from dlt.common.jsonpath import find_values, compile_path, extract_simple_field_name
from dlt.extract.incremental.exceptions import (
    IncrementalCursorInvalidCoercion,
    IncrementalCursorPathMissing,
    IncrementalPrimaryKeyMissing,
    IncrementalCursorPathHasValueNone,
)
from dlt.common.incremental.typing import (
    TCursorValue,
    LastValueFunc,
    OnCursorValueMissing,
    TIncrementalRange,
)
from dlt.extract.utils import resolve_column_value
from dlt.extract.items import TTableHintTemplate

try:
    from dlt.common.libs import pyarrow
    from dlt.common.libs.pyarrow import pyarrow as pa, TAnyArrowItem
    from dlt.common.libs.pyarrow import from_arrow_scalar, to_arrow_scalar
except MissingDependencyException:
    pa = None
    pyarrow = None

try:
    from dlt.common.libs.numpy import numpy
except MissingDependencyException:
    numpy = None

# NOTE: always import pandas independently from pyarrow
try:
    from dlt.common.libs.pandas import pandas, pandas_to_arrow
except MissingDependencyException:
    pandas = None


class IncrementalTransform:
    """A base class for handling extraction and stateful tracking
    of incremental data from input data items.

    By default, the descendant classes are instantiated within the
    `dlt.extract.incremental.Incremental` class.

    Subclasses must implement the `__call__` method which will be called
    for each data item in the extracted data.
    """

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
        on_cursor_value_missing: OnCursorValueMissing = "raise",
        lag: Optional[float] = None,
        range_start: TIncrementalRange = "closed",
        range_end: TIncrementalRange = "open",
    ) -> None:
        self.resource_name = resource_name
        self.cursor_path = cursor_path
        self.initial_value = initial_value
        self.start_value = start_value
        self.last_value = start_value
        self.end_value = end_value
        self.last_rows: List[TDataItem] = []
        self.last_value_func = last_value_func
        self.unique_hashes = unique_hashes
        self.start_unique_hashes = set(unique_hashes)
        self.on_cursor_value_missing = on_cursor_value_missing
        self.lag = lag
        self.range_start = range_start
        self.range_end = range_end
        # NOTE: self.primary_key is a property
        self.primary_key = primary_key

        # compile jsonpath
        self._compiled_cursor_path = compile_path(cursor_path)
        # for simple column name we'll fallback to search in dict
        if simple_field_name := extract_simple_field_name(self._compiled_cursor_path):
            self.cursor_path = simple_field_name
            self._compiled_cursor_path = None

    def compute_unique_value(
        self,
        row: TDataItem,
        primary_key: Optional[TTableHintTemplate[TColumnNames]],
    ) -> str:
        try:
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

    @staticmethod
    def _adapt_if_datetime(row_value: Any, last_value: Any) -> Any:
        # For datetime cursor, ensure the value is a timezone aware datetime.
        # The object saved in state will always be a tz aware pendulum datetime so this ensures values are comparable
        if (
            isinstance(row_value, datetime)
            and row_value.tzinfo is None
            and isinstance(last_value, datetime)
            and last_value.tzinfo is not None
        ):
            row_value = pendulum.instance(row_value).in_tz("UTC")
        return row_value

    def compute_deduplication_disabled(self) -> bool:
        """Skip deduplication when length of the key is 0 or if lag is applied."""
        # disable deduplication if end value is set - state is not saved
        if self.range_start == "open":
            return True
        if self.end_value is not None:
            return True
        # disable deduplication if lag is applied - destination must deduplicate ranges
        if self.lag and self.last_value_func in (min, max):
            return True
        # disable deduplication if primary_key = ()
        return isinstance(self._primary_key, (list, tuple)) and len(self._primary_key) == 0

    @property
    def primary_key(self) -> Optional[TTableHintTemplate[TColumnNames]]:
        return self._primary_key

    @primary_key.setter
    def primary_key(self, value: str) -> None:
        self._primary_key = value
        self.boundary_deduplication = not self.compute_deduplication_disabled()


class JsonIncremental(IncrementalTransform):
    """Extracts incremental data from JSON data items."""

    def find_cursor_value(self, row: TDataItem) -> Any:
        """Finds value in row at cursor defined by self.cursor_path.

        Will use compiled JSONPath if present.
        Otherwise, reverts to field access if row is dict, Pydantic model, or of other class.
        """
        key_exc: Type[Exception] = IncrementalCursorPathHasValueNone
        if self._compiled_cursor_path:
            # ignores the other found values, e.g. when the path is $data.items[*].created_at
            try:
                row_value = find_values(self._compiled_cursor_path, row)[0]
            except IndexError:
                # empty list so raise a proper exception
                row_value = None
                key_exc = IncrementalCursorPathMissing
        else:
            try:
                try:
                    row_value = row[self.cursor_path]
                except TypeError:
                    # supports Pydantic models and other classes
                    row_value = getattr(row, self.cursor_path)
            except (KeyError, AttributeError):
                # attr not found so raise a proper exception
                row_value = None
                key_exc = IncrementalCursorPathMissing

        # if we have a value - return it
        if row_value is not None:
            return row_value

        if self.on_cursor_value_missing == "raise":
            # raise missing path or None value exception
            raise key_exc(self.resource_name, self.cursor_path, row)
        elif self.on_cursor_value_missing == "exclude":
            return None

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
        if row_value is None:
            if self.on_cursor_value_missing == "exclude":
                return None, False, False
            else:
                return row, False, False
        last_value = self.last_value
        last_value_func = self.last_value_func
        row_value = self._adapt_if_datetime(row_value, last_value)

        # Check whether end_value has been reached
        # Filter end value ranges exclusively, so in case of "max" function we remove values >= end_value
        if self.end_value is not None:
            try:
                if last_value_func((row_value, self.end_value)) != self.end_value:
                    return None, False, True

                if self.range_end == "open" and last_value_func((row_value,)) == self.end_value:
                    return None, False, True
            except Exception as ex:
                raise IncrementalCursorInvalidCoercion(
                    self.resource_name,
                    self.cursor_path,
                    self.end_value,
                    "end_value",
                    row_value,
                    type(row_value).__name__,
                    str(ex),
                ) from ex
        if last_value is None:
            check_values: Tuple[Any, ...] = (row_value,)
        else:
            check_values = (row_value, last_value)

        try:
            new_value = last_value_func(check_values)
        except Exception as ex:
            raise IncrementalCursorInvalidCoercion(
                self.resource_name,
                self.cursor_path,
                last_value,
                "start_value/initial_value",
                row_value,
                type(row_value).__name__,
                str(ex),
            ) from ex
        # new_value is "less" or equal to last_value (the actual max)
        if last_value == new_value:
            # use func to compute row_value into last_value compatible
            processed_row_value = last_value_func((row_value,))

            if self.range_start == "open" and processed_row_value == self.start_value:
                # We only want greater than start_value
                return None, False, False

            # skip the record that is not a start_value or new_value: that record was already processed
            if self.start_value is None:
                check_values = (row_value,)
            else:
                check_values = (row_value, self.start_value)
            new_value = last_value_func(check_values)
            # Include rows == start_value but exclude "lower"
            # new_value is "less" or equal to start_value (the initial max)
            if new_value == self.start_value:
                # if equal there's still a chance that item gets in
                if processed_row_value == self.start_value:
                    if self.boundary_deduplication:
                        unique_value = self.compute_unique_value(row, self._primary_key)
                        # if unique value exists then use it to deduplicate
                        if unique_value in self.start_unique_hashes:
                            return None, True, False
                else:
                    # "smaller" than start value: gets out
                    return None, True, False

            # we store row id for all records with the current "last_value" in state and use it to deduplicate
            if processed_row_value == last_value and self.boundary_deduplication:
                # add new hash only if the record row id is same as current last value
                self.last_rows.append(row)
        else:
            self.last_value = new_value
            if self.boundary_deduplication:
                # store rows with "max" values to compute hashes after processing full batch
                self.last_rows = [row]
                self.unique_hashes = set()

        return row, False, False


class ArrowIncremental(IncrementalTransform):
    _dlt_index = "_dlt_index"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if self.last_value_func is max:
            self.compute = pa.compute.max
            self.end_compare = (
                pa.compute.less if self.range_end == "open" else pa.compute.less_equal
            )
            self.last_value_compare = (
                pa.compute.greater_equal if self.range_start == "closed" else pa.compute.greater
            )
            self.new_value_compare = pa.compute.greater
        elif self.last_value_func is min:
            self.compute = pa.compute.min
            self.end_compare = (
                pa.compute.greater if self.range_end == "open" else pa.compute.greater_equal
            )
            self.last_value_compare = (
                pa.compute.less_equal if self.range_start == "closed" else pa.compute.less
            )
            self.new_value_compare = pa.compute.less
        else:
            raise NotImplementedError(
                "Only `min` or `max` of `last_value_func` is supported for arrow tables"
            )

    def compute_unique_values(self, item: "TAnyArrowItem", unique_columns: List[str]) -> List[str]:
        if not unique_columns:
            return []
        rows = item.select(unique_columns).to_pylist()
        return [self.compute_unique_value(row, self._primary_key) for row in rows]

    def compute_unique_values_with_index(
        self, item: "TAnyArrowItem", unique_columns: List[str]
    ) -> List[Tuple[Any, str]]:
        if not unique_columns:
            return []
        indices = item[self._dlt_index].to_pylist()
        rows = item.select(unique_columns).to_pylist()
        return [
            (index, self.compute_unique_value(row, self._primary_key))
            for index, row in zip(indices, rows)
        ]

    def _add_unique_index(self, tbl: "pa.Table") -> "pa.Table":
        """Creates unique index if necessary."""
        # create unique index if necessary
        if self._dlt_index not in tbl.schema.names:
            # indices = pa.compute.sequence(start=0, step=1, length=tbl.num_rows,
            indices = pa.array(range(tbl.num_rows))
            tbl = pyarrow.append_column(tbl, self._dlt_index, indices)
        return tbl

    def __call__(
        self,
        tbl: "TAnyArrowItem",
    ) -> Tuple[TDataItem, bool, bool]:
        is_pandas = pandas is not None and isinstance(tbl, pandas.DataFrame)
        if is_pandas:
            tbl = pandas_to_arrow(tbl)

        primary_key = self._primary_key(tbl) if callable(self._primary_key) else self._primary_key
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

        # TODO: Json path support. For now assume the cursor_path is a column name
        cursor_path = self.cursor_path
        try:
            cursor_data_type = tbl.schema.field(cursor_path).type
        except KeyError as e:
            raise IncrementalCursorPathMissing(
                self.resource_name,
                cursor_path,
                tbl,
                f"Column name `{cursor_path}` was not found in the arrow table. Nested JSON paths"
                " are not supported for arrow tables and dataframes, the incremental `cursor_path`"
                " must be a column name.",
            ) from e

        # The new max/min value
        row_value_scalar = self.compute(
            tbl[cursor_path]
        )  # to_arrow_scalar(row_value, cursor_data_type)
        row_value = self._adapt_if_datetime(from_arrow_scalar(row_value_scalar), self.last_value)

        if tbl.schema.field(cursor_path).nullable:
            tbl_without_null, tbl_with_null = self._process_null_at_cursor_path(tbl)
            tbl = tbl_without_null

        # If end_value is provided, filter to include table rows that are "less" than end_value
        if self.end_value is not None:
            try:
                end_value_scalar = to_arrow_scalar(self.end_value, cursor_data_type)
            except Exception as ex:
                raise IncrementalCursorInvalidCoercion(
                    self.resource_name,
                    cursor_path,
                    self.end_value,
                    "end_value",
                    "<arrow column>",
                    cursor_data_type,
                    str(ex),
                ) from ex
            tbl = tbl.filter(self.end_compare(tbl[cursor_path], end_value_scalar))
            # Is max row value higher than end value?
            # NOTE: pyarrow bool *always* evaluates to python True. `as_py()` is necessary
            end_out_of_range = not self.end_compare(row_value_scalar, end_value_scalar).as_py()

        if self.start_value is not None:
            try:
                start_value_scalar = to_arrow_scalar(self.start_value, cursor_data_type)
            except Exception as ex:
                raise IncrementalCursorInvalidCoercion(
                    self.resource_name,
                    cursor_path,
                    self.start_value,
                    "start_value/initial_value",
                    "<arrow column>",
                    cursor_data_type,
                    str(ex),
                ) from ex
            # Remove rows lower or equal than the last start value
            keep_filter = self.last_value_compare(tbl[cursor_path], start_value_scalar)
            start_out_of_range = bool(pa.compute.any(pa.compute.invert(keep_filter)).as_py())
            tbl = tbl.filter(keep_filter)
            if self.boundary_deduplication:
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
                if len(unique_values_index) > 0:
                    # find rows with unique ids that were stored from previous run
                    remove_idx = pa.array(i for i, _ in unique_values_index)
                    tbl = tbl.filter(
                        pa.compute.invert(pa.compute.is_in(tbl[self._dlt_index], remove_idx))
                    )

        if (
            self.last_value is None
            or self.new_value_compare(
                row_value_scalar, to_arrow_scalar(self.last_value, cursor_data_type)
            ).as_py()
        ):  # Last value has changed
            self.last_value = row_value
            if self.boundary_deduplication:
                # Compute unique hashes for all rows equal to row value
                self.unique_hashes = set(
                    self.compute_unique_values(
                        tbl.filter(pa.compute.equal(tbl[cursor_path], row_value_scalar)),
                        unique_columns,
                    )
                )
        elif self.last_value == row_value and self.boundary_deduplication:
            # last value is unchanged, add the hashes
            self.unique_hashes.update(
                set(
                    self.compute_unique_values(
                        tbl.filter(pa.compute.equal(tbl[cursor_path], row_value_scalar)),
                        unique_columns,
                    )
                )
            )

        # drop the temp unique index before concat and returning
        if "_dlt_index" in tbl.schema.names:
            tbl = pyarrow.remove_columns(tbl, ["_dlt_index"])

        if self.on_cursor_value_missing == "include":
            if tbl.schema.field(cursor_path).nullable:
                if isinstance(tbl, pa.RecordBatch):
                    assert isinstance(tbl_with_null, pa.RecordBatch)
                    tbl = pa.Table.from_batches([tbl, tbl_with_null])
                else:
                    tbl = pa.concat_tables([tbl, tbl_with_null], promote_options="none")

        if len(tbl) == 0:
            return None, start_out_of_range, end_out_of_range
        if is_pandas:
            tbl = tbl.to_pandas()
        return tbl, start_out_of_range, end_out_of_range

    def _process_null_at_cursor_path(self, tbl: "pa.Table") -> Tuple["pa.Table", "pa.Table"]:
        mask = pa.compute.is_valid(tbl[self.cursor_path])
        rows_without_null = tbl.filter(mask)
        rows_with_null = tbl.filter(pa.compute.invert(mask))
        if self.on_cursor_value_missing == "raise":
            if rows_with_null.num_rows > 0:
                raise IncrementalCursorPathHasValueNone(self.resource_name, self.cursor_path)
        return rows_without_null, rows_with_null
