from __future__ import annotations

from datetime import datetime  # noqa: I251
from typing import Any, Optional, Set, Tuple, List, Type
from pendulum.tz import UTC
from pendulum import DateTime  # noqa: I251

from dlt.common import logger
from dlt.common.exceptions import MissingDependencyException
from dlt.common.utils import digest128
from dlt.common.json import json
from dlt.common.pendulum import create_dt, pendulum
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
from dlt.common.libs.narwhals import narwhals as nw
from dlt.extract.utils import resolve_column_value
from dlt.extract.items import TTableHintTemplate

try:
    from dlt.common.libs.pyarrow import pyarrow as pa, TAnyArrowItem
except MissingDependencyException:
    pa = None

try:
    from dlt.common.libs.numpy import numpy
except MissingDependencyException:
    numpy = None

# NOTE: always import pandas independently from pyarrow
try:
    from dlt.common.libs.pandas import pandas
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
        self.start_value_is_datetime = isinstance(start_value, datetime)
        # self.start_value_tz = start_value.tzinfo if isinstance(start_value, datetime) else None
        self.last_value = start_value
        self.end_value = end_value
        self.end_value_is_datetime = isinstance(end_value, datetime)
        # self.end_value = end_value.tzinfo if isinstance(end_value, datetime) else None
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
        # tells if incremental instance has processed any data
        self.seen_data = False

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
    def _adapt_timezone(
        row_value: datetime, cursor_value: datetime, cursor_value_name: str, resource_name: str
    ) -> Any:
        """Adapt cursor_value tz-awareness to match row_value. This method is called only once in instance lifecycle
        to catch incompatible initial values or adapt to change of tz-awareness in the data itself.
        Returns: adjusted cursor_value (with TZ added or removed)
        """
        if (
            cursor_value.tzinfo is None or row_value.tzinfo is None
        ) and cursor_value.tzinfo != row_value.tzinfo:
            config_value_name = (
                "initial_value" if cursor_value_name == "last_value" else cursor_value_name
            )
            last_value_source_message = ""
            if cursor_value_name == "last_value":
                last_value_source_message = (
                    f"{cursor_value_name} is coming from the {config_value_name} defined on"
                    " Incremental or from previous run state. "
                )
            message = (
                f"In resource {resource_name}: {cursor_value_name} '{cursor_value}' and row value"
                f" {row_value} have different timezone awareness. {last_value_source_message}Row"
                f" value is the actual data. {cursor_value_name} will be corrected to match the row"
                " value timezone. The reason for that may be: (1) you set wrong tz-awareness on"
                f" the {config_value_name} (note that pendulum is tz-aware by default) (2) the data"
                " has changed its tz-awareness across runs. (3) your pipeline state got upgraded"
                " to always store last value with tz-awareness following your data."
            )
            logger.warning(message)
            if row_value.tzinfo is None:
                cursor_value = (
                    create_dt(
                        cursor_value.year,
                        cursor_value.month,
                        cursor_value.day,
                        cursor_value.hour,
                        cursor_value.minute,
                        cursor_value.second,
                        cursor_value.microsecond,
                        tz=cursor_value.tzinfo,
                        fold=cursor_value.fold,
                    )
                    .in_tz(tz=UTC)
                    .naive()
                )
            else:
                cursor_value = create_dt(
                    cursor_value.year,
                    cursor_value.month,
                    cursor_value.day,
                    cursor_value.hour,
                    cursor_value.minute,
                    cursor_value.second,
                    cursor_value.microsecond,
                    tz=cursor_value.tzinfo,
                    fold=cursor_value.fold,
                ).in_tz(
                    row_value.tzinfo  # type: ignore[arg-type]
                )
        return cursor_value

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
    def primary_key(self, value: Optional[TTableHintTemplate[TColumnNames]]) -> None:
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
        # correct tz-awareness if last_value/start_value not matching
        if not self.seen_data and self.start_value_is_datetime and isinstance(row_value, datetime):
            # NOTE: we are making sure that last_value == start_value here (self.seen_data)
            assert self.last_value == self.start_value
            self.last_value = self.start_value = last_value = self._adapt_timezone(
                row_value, last_value, "last_value", self.resource_name
            )

        # Check whether end_value has been reached
        # Filter end value ranges exclusively, so in case of "max" function we remove values >= end_value
        if self.end_value is not None:
            # correct tz-awareness of end value
            if (
                not self.seen_data
                and self.end_value_is_datetime
                and isinstance(row_value, datetime)
            ):
                self.end_value = self._adapt_timezone(
                    row_value, self.end_value, "end_value", self.resource_name
                )
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
        # row was read
        self.seen_data = True

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
                return None, True, False

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
        if self.last_value_func not in (min, max):
            raise NotImplementedError(
                "Only `min` or `max` of `last_value_func` is supported for arrow tables"
            )

    def compute_unique_values(
        self, item: nw.DataFrame, unique_columns: List[str]
    ) -> List[str]:
        """Return a list of unique hash values for rows in a narwhals DataFrame."""
        if not unique_columns:
            return []
        rows = item.select(unique_columns).rows(named=True)
        return [self.compute_unique_value(row, self._primary_key) for row in rows]

    def compute_unique_values_with_index(
        self, item: nw.DataFrame, unique_columns: List[str]
    ) -> List[Tuple[Any, str]]:
        """Return (index_value, unique_hash) pairs for rows in a narwhals DataFrame."""
        if not unique_columns:
            return []
        indices = item[self._dlt_index].to_list()
        rows = item.select(unique_columns).rows(named=True)
        return [
            (index, self.compute_unique_value(row, self._primary_key))
            for index, row in zip(indices, rows)
        ]

    def __call__(
        self,
        tbl: TDataItem,
    ) -> Tuple[TDataItem, bool, bool]:
        return process_arrow_incremental(self, tbl)


def process_arrow_incremental(
    self: ArrowIncremental,
    tbl: TDataItem,
) -> Tuple[TDataItem, bool, bool]:
    """Process incremental filtering and deduplication on a dataframe-like input.

    Accepts any narwhals-compatible eager or lazy frame (pyarrow Table/RecordBatch,
    pandas DataFrame, polars DataFrame/LazyFrame) and returns the same native type
    (after filtering), so callers never need to track the backend themselves.

    Returns:
        Tuple of (filtered_table_or_None, start_out_of_range, end_out_of_range)
    """
    # pa.RecordBatch is not supported by narwhals; promote to Table upfront.
    # The result will be a pa.Table, which is a lossless promotion.
    is_record_batch = False
    if pa is not None and isinstance(tbl, pa.RecordBatch):
        is_record_batch = True
        tbl = pa.Table.from_batches([tbl])

    # Wrap in narwhals, collecting lazy frames (e.g. polars.LazyFrame) eagerly.
    # narwhals.from_native preserves the backend so to_native() later returns
    # the original type (pyarrow Table → pyarrow Table, pandas → pandas, etc.).
    df = nw.from_native(tbl, allow_series=False)
    if isinstance(df, nw.LazyFrame):
        df = df.collect()

    # Resolve primary key (may be a callable that inspects the native table).
    primary_key = self._primary_key(df.to_native()) if callable(self._primary_key) else self._primary_key
    if primary_key:
        unique_columns: List[str] = [primary_key] if isinstance(primary_key, str) else list(primary_key)
        for pk in unique_columns:
            if pk not in df.columns:
                raise IncrementalPrimaryKeyMissing(self.resource_name, pk, df.to_native())
        # A single-column primary key doubles as the dedup row index.
        if isinstance(primary_key, str):
            self._dlt_index = primary_key
    elif primary_key is None:
        unique_columns = list(df.columns)
    else:
        unique_columns = []

    start_out_of_range = end_out_of_range = False
    if len(df) == 0:
        return tbl, start_out_of_range, end_out_of_range

    cursor_path = self.cursor_path
    if cursor_path not in df.columns:
        raise IncrementalCursorPathMissing(
            self.resource_name,
            cursor_path,
            df.to_native(),
            f"Column name `{cursor_path}` was not found in the table. Nested JSON paths"
            " are not supported for arrow tables and dataframes, the incremental"
            " `cursor_path` must be a column name.",
        )

    # ---------------------------------------------------------------------------
    # Build backend-agnostic comparison helpers from last_value_func.
    #
    # Series-level operators (series >= value) delegate directly to the native
    # backend, which can raise type-mismatch errors (e.g. pyarrow refuses to
    # compare timestamp[ns,UTC] with timestamp[us,+00:00]).
    #
    # Using narwhals *expressions* (nw.col / nw.lit) instead lets narwhals
    # resolve types before handing off to the backend.  nw.lit(value).cast(dtype)
    # ensures the scalar is cast to exactly the same dtype as the column, so
    # pyarrow (and every other backend) sees a compatible pair.
    # ---------------------------------------------------------------------------
    col_dtype = df[cursor_path].dtype

    if self.last_value_func is max:
        def _compute_agg(series: "nw.Series") -> Any:
            return series.max()

        def _end_compare_expr(col: str, dtype: "nw.DType", value: Any) -> "nw.Expr":
            lit = nw.lit(value).cast(dtype)
            return nw.col(col) < lit if self.range_end == "open" else nw.col(col) <= lit

        def _end_compare_scalar(a: Any, b: Any) -> bool:
            return a < b if self.range_end == "open" else a <= b

        def _last_value_compare_expr(col: str, dtype: "nw.DType", value: Any) -> "nw.Expr":
            lit = nw.lit(value).cast(dtype)
            return nw.col(col) >= lit if self.range_start == "closed" else nw.col(col) > lit

        def _new_value_compare(a: Any, b: Any) -> bool:
            return a > b
    else:  # min
        def _compute_agg(series: "nw.Series") -> Any:
            return series.min()

        def _end_compare_expr(col: str, dtype: "nw.DType", value: Any) -> "nw.Expr":
            lit = nw.lit(value).cast(dtype)
            return nw.col(col) > lit if self.range_end == "open" else nw.col(col) >= lit

        def _end_compare_scalar(a: Any, b: Any) -> bool:
            return a > b if self.range_end == "open" else a >= b

        def _last_value_compare_expr(col: str, dtype: "nw.DType", value: Any) -> "nw.Expr":
            lit = nw.lit(value).cast(dtype)
            return nw.col(col) <= lit if self.range_start == "closed" else nw.col(col) < lit

        def _new_value_compare(a: Any, b: Any) -> bool:
            return a < b

    # ---------------------------------------------------------------------------
    # Compute the max/min cursor value across the whole batch (Python scalar).
    #
    # Narwhals may return backend-specific scalars depending on the frame type:
    #   - pandas-backed frames: numpy.generic (e.g. np.int64) or pandas.Timestamp
    #   - pyarrow / polars: plain Python types
    #
    # We normalise to plain Python so the value is:
    #   (a) JSON-serialisable for state persistence, and
    #   (b) safely comparable with the pendulum.DateTime values restored from
    #       state — pendulum.__eq__(pandas.Timestamp) internally calls
    #       Timestamp.toordinal() which raises NotImplementedError for
    #       nanosecond-precision timestamps.
    # ---------------------------------------------------------------------------
    row_value = _compute_agg(df[cursor_path])
    if numpy is not None and isinstance(row_value, numpy.generic):
        # Covers np.int64, np.float64, np.datetime64, … from pandas int/float cols
        row_value = row_value.item()
    if pandas is not None and isinstance(row_value, pandas.Timestamp):
        # pandas datetime columns return Timestamp from .max()/.min()
        row_value = row_value.to_pydatetime()

    # Adapt tz-awareness of start/last value to match the incoming data on
    # the very first batch (only fires once per instance lifetime).
    if not self.seen_data and self.start_value_is_datetime and isinstance(row_value, datetime):
        assert self.last_value == self.start_value
        self.start_value = self.last_value = self._adapt_timezone(
            row_value, self.last_value, "last_value", self.resource_name
        )

    # ---------------------------------------------------------------------------
    # Separate rows where cursor is NULL.
    # We always check null_count (rather than relying on schema-level nullable
    # flag) so the logic is uniform across all backends.
    # ---------------------------------------------------------------------------
    df_with_null: Optional["nw.DataFrame"] = None
    has_nulls = df[cursor_path].null_count() > 0
    if has_nulls:
        df_with_null = df.filter(nw.col(cursor_path).is_null())
        df = df.filter(~nw.col(cursor_path).is_null())
        if self.on_cursor_value_missing == "raise":
            raise IncrementalCursorPathHasValueNone(self.resource_name, cursor_path)

    # ---------------------------------------------------------------------------
    # Filter: exclude rows beyond end_value.
    # ---------------------------------------------------------------------------
    if self.end_value is not None:
        if not self.seen_data and self.end_value_is_datetime and isinstance(row_value, datetime):
            self.end_value = self._adapt_timezone(
                row_value, self.end_value, "end_value", self.resource_name
            )
        try:
            # Scalar check: is the batch max/min already within range?
            end_out_of_range = not _end_compare_scalar(row_value, self.end_value)
            if end_out_of_range:
                # Expression evaluation is lazy in narwhals: the cast/comparison
                # is only executed here inside filter, so we catch type errors here.
                df = df.filter(_end_compare_expr(cursor_path, col_dtype, self.end_value))
                # NOTE: row_value is intentionally NOT recalculated here — when
                # end_value is set, self.last_value is never persisted to state.
        except IncrementalCursorInvalidCoercion:
            raise
        except Exception as ex:
            raise IncrementalCursorInvalidCoercion(
                self.resource_name,
                cursor_path,
                self.end_value,
                "end_value",
                "<column>",
                str(col_dtype),
                str(ex),
            ) from ex

    self.seen_data = True

    # ---------------------------------------------------------------------------
    # Filter: exclude rows below/at start_value (already-processed boundary).
    # ---------------------------------------------------------------------------
    need_temp_index = False  # tracks whether we injected a temporary row index
    if self.start_value is not None:
        try:
            keep_expr = _last_value_compare_expr(cursor_path, col_dtype, self.start_value)
            # Expression evaluation is lazy in narwhals: the cast/comparison
            # is only executed here inside filter, so we catch type errors here.
            n_before = len(df)
            df = df.filter(keep_expr)
        except IncrementalCursorInvalidCoercion:
            raise
        except Exception as ex:
            raise IncrementalCursorInvalidCoercion(
                self.resource_name,
                cursor_path,
                self.start_value,
                "start_value/initial_value",
                "<column>",
                str(col_dtype),
                str(ex),
            ) from ex
        # Detect whether any rows were filtered out (= start boundary was hit).
        start_out_of_range = len(df) < n_before

        if self.boundary_deduplication:
            # Add a temporary integer row index when no single-column PK is
            # acting as the index already.
            need_temp_index = ArrowIncremental._dlt_index not in df.columns
            if need_temp_index:
                df = df.with_row_index(ArrowIncremental._dlt_index)

            # Among the kept rows, find those sitting exactly on the boundary
            # (cursor == start_value) and remove any that we already processed
            # in the previous run (tracked via start_unique_hashes).
            eq_expr = nw.col(cursor_path) == nw.lit(self.start_value).cast(col_dtype)
            eq_df = df.filter(eq_expr)
            unique_values_index = self.compute_unique_values_with_index(eq_df, unique_columns)
            to_remove = [i for i, v in unique_values_index if v in self.start_unique_hashes]
            if to_remove:
                df = df.filter(~df[self._dlt_index].is_in(to_remove))

    # ---------------------------------------------------------------------------
    # Update last_value and unique_hashes for the next run.
    # ---------------------------------------------------------------------------
    eq_row_expr = nw.col(cursor_path) == nw.lit(row_value).cast(col_dtype)
    if self.last_value is None or _new_value_compare(row_value, self.last_value):
        # The batch contains a new extreme — reset tracked hashes.
        self.last_value = row_value
        if self.boundary_deduplication:
            self.unique_hashes = set(
                self.compute_unique_values(
                    df.filter(eq_row_expr),
                    unique_columns,
                )
            )
    elif self.last_value == row_value and self.boundary_deduplication:
        # Same extreme as before — accumulate hashes.
        self.unique_hashes.update(
            self.compute_unique_values(
                df.filter(eq_row_expr),
                unique_columns,
            )
        )

    # Drop the temporary row index before returning (never drop a real PK column).
    if need_temp_index and ArrowIncremental._dlt_index in df.columns:
        df = df.drop(ArrowIncremental._dlt_index)

    # ---------------------------------------------------------------------------
    # Re-attach rows with NULL cursor values when mode is "include".
    # ---------------------------------------------------------------------------
    if self.on_cursor_value_missing == "include" and has_nulls and df_with_null is not None:
        df = nw.concat([df, df_with_null])

    if len(df) == 0:
        return None, start_out_of_range, end_out_of_range

    native_tbl = df.to_native()
    if is_record_batch:
        # TODO test that this never drops data
        native_tbl = native_tbl.combine_chunks().to_batches()[0]

    return native_tbl, start_out_of_range, end_out_of_range
