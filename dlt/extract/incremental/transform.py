from datetime import datetime  # noqa: I251
from typing import Optional, Tuple, Protocol, Mapping, Union, List

try:
    import pandas as pd
except ModuleNotFoundError:
    pd = None

from dlt.common.exceptions import MissingDependencyException
from dlt.common.utils import digest128
from dlt.common.json import json
from dlt.common import pendulum
from dlt.common.typing import TDataItem, TDataItems
from dlt.common.jsonpath import TJsonPath, find_values
from dlt.extract.incremental.exceptions import IncrementalCursorPathMissing, IncrementalPrimaryKeyMissing
from dlt.extract.incremental.typing import IncrementalColumnState, TCursorValue, LastValueFunc
from dlt.extract.utils import resolve_column_value
from dlt.extract.typing import TTableHintTemplate
from dlt.common.schema.typing import TColumnNames
try:
    from dlt.common.libs.pyarrow import is_arrow_item, pyarrow as pa, TAnyArrowItem
except MissingDependencyException:
    is_arrow_item = lambda x: False



class IncrementalTransformer:
    def __init__(
        self,
        resource_name: str,
        cursor_path: TJsonPath,
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

    def __call__(
        self,
        row: TDataItem,
    ) -> Tuple[bool, bool, bool]:
        ...


class JsonIncremental(IncrementalTransformer):
    def unique_value(
        self,
        row: TDataItem,
        primary_key: Optional[TTableHintTemplate[TColumnNames]],
        resource_name: str
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

        row_values = find_values(self.cursor_path, row)
        if not row_values:
            raise IncrementalCursorPathMissing(self.resource_name, str(self.cursor_path), row)
        row_value = row_values[0]

        # For datetime cursor, ensure the value is a timezone aware datetime.
        # The object saved in state will always be a tz aware pendulum datetime so this ensures values are comparable
        if isinstance(row_value, datetime):
            row_value = pendulum.instance(row_value)

        last_value = self.incremental_state['last_value']

        # Check whether end_value has been reached
        # Filter end value ranges exclusively, so in case of "max" function we remove values >= end_value
        if self.end_value is not None and (
            self.last_value_func((row_value, self.end_value)) != self.end_value or self.last_value_func((row_value, )) == self.end_value
        ):
            end_out_of_range = True
            return None, start_out_of_range, end_out_of_range

        check_values = (row_value,) + ((last_value, ) if last_value is not None else ())
        new_value = self.last_value_func(check_values)
        if last_value == new_value:
            processed_row_value = self.last_value_func((row_value, ))
            # we store row id for all records with the current "last_value" in state and use it to deduplicate

            if processed_row_value == last_value:
                unique_value = self.unique_value(row, self.primary_key, self.resource_name)
                # if unique value exists then use it to deduplicate
                if unique_value:
                    if unique_value in self.incremental_state['unique_hashes']:
                        return None, start_out_of_range, end_out_of_range
                    # add new hash only if the record row id is same as current last value
                    self.incremental_state['unique_hashes'].append(unique_value)
                return row, start_out_of_range, end_out_of_range
            # skip the record that is not a last_value or new_value: that record was already processed
            check_values = (row_value,) + ((self.start_value,) if self.start_value is not None else ())
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



class ArrowIncremental(IncrementalTransformer):
    def unique_values(
        self,
        item: "TAnyArrowItem",
        primary_key: Optional[TTableHintTemplate[TColumnNames]],
        resource_name: str
    ) -> List[Tuple[int, str]]:
        item = item
        indices = item["_dlt_index"]
        item = item.drop(["_dlt_index"])  # Don't include the index in unique hash
        if primary_key:
            columns = primary_key(item) if callable(primary_key) else primary_key
            if isinstance(columns, str):
                item = item[columns]
            else:
                item = item.select(columns)
        rows = item.to_pylist()
        return [
            (index, digest128(json.dumps(row, sort_keys=True))) for index, row in zip(indices, rows)
        ]

    def __call__(
        self,
        tbl: "TAnyArrowItem",
    ) -> Tuple[TDataItem, bool, bool]:
        is_pandas = pd is not None and isinstance(tbl, pd.DataFrame)
        if is_pandas:
            tbl = pa.Table.from_pandas(tbl)

        start_out_of_range = end_out_of_range = False
        if not tbl:  # row is None or empty arrow table
            return tbl, start_out_of_range, end_out_of_range

        last_value = self.incremental_state['last_value']

        if self.last_value_func is max:
            compute = pa.compute.max
            end_compare = pa.compute.less
            start_compare = pa.compute.greater_equal
            last_value_compare = pa.compute.greater_equal
            new_value_compare = pa.compute.greater
        elif self.last_value_func is min:
            compute = pa.compute.min
            end_compare = pa.compute.greater
            start_compare = pa.compute.less_equal
            last_value_compare = pa.compute.less_equal
            new_value_compare = pa.compute.less
        else:
            raise NotImplementedError("Only min or max last_value_func is supported for arrow tables")


        # TODO: Json path support. For now assume the cursor_path is a column name
        cursor_path = str(self.cursor_path)
        # The new max/min value
        row_value = compute(tbl[cursor_path]).as_py()

        # If end_value is provided, filter to include table rows that are "less" than end_value
        if self.end_value is not None:
            tbl = tbl.filter(end_compare(tbl[cursor_path], self.end_value))
            # Is max row value higher than end value?
            end_out_of_range = not end_compare(row_value, self.end_value)
            if end_out_of_range:
                if is_pandas:
                    tbl = tbl.to_pandas()
                return tbl, start_out_of_range, end_out_of_range

        # Filter out all rows which have cursor value equal to last value
        # and unique id exists in state
        tbl = tbl.append_column("_dlt_index", pa.array(range(tbl.num_rows)))
        if last_value is not None:
            tbl = tbl.filter(last_value_compare(tbl[cursor_path], last_value))
            # Exclude rows from the table which have unique hashes already seen before

            # Rows with same cursor as stored last value
            eq_rows = tbl.filter(pa.compute.equal(tbl[cursor_path], last_value))
            # compute index, unique hash mapping
            unique_values = self.unique_values(eq_rows, self.primary_key, self.resource_name)
            unique_values = [(i, uq_val) for i, uq_val in unique_values if uq_val in self.incremental_state['unique_hashes']]
            remove_idx = pa.array(i for i, _ in unique_values)
            # Filter the table
            tbl = tbl.filter(pa.invert(pa.compute.is_in(tbl["_dlt_index"], remove_idx)))

            if new_value_compare(row_value, last_value):  # Last value has changed
                self.incremental_state['last_value'] = row_value
                # Compute unique hashes for all rows equal to row value
                self.incremental_state['unique_hashes'] = [uq_val for _, uq_val in self.unique_values(
                    tbl.filter(pa.compute.equal(tbl[cursor_path], row_value)), self.primary_key, self.resource_name
                )]
            else:
                # last value is unchanged, add the hashes
                self.incremental_state['unique_hashes'].extend(uq_val for _, uq_val in unique_values)
        else:
            self.incremental_state['last_value'] = row_value
            self.incremental_state['unique_hashes'] = [uq_val for _, uq_val in self.unique_values(
                tbl.filter(pa.compute.equal(tbl[cursor_path], row_value)), self.primary_key, self.resource_name
            )]

        if self.start_value is not None:
            # Is any value lower than start value
            start_out_of_range = pa.compute.any(end_compare(tbl[cursor_path], self.start_value)).as_py()
            # Include rows >= start_value
            tbl = tbl.filter(start_compare(tbl[cursor_path], self.start_value))

        if is_pandas:
            return tbl.drop(["_dlt_index"]).to_pandas(), start_out_of_range, end_out_of_range
        return tbl.drop(["_dlt_index"]), start_out_of_range, end_out_of_range
