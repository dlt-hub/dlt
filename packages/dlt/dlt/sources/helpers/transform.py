from collections.abc import Sequence as C_Sequence
from typing import Any, Dict, Sequence, Union

from dlt.common.typing import TDataItem
from dlt.extract.items_transform import ItemTransformFunctionNoMeta

import jsonpath_ng


def take_first(max_items: int) -> ItemTransformFunctionNoMeta[bool]:
    """A filter that takes only first `max_items` from a resource"""
    count: int = 0

    def _filter(_: TDataItem) -> bool:
        nonlocal count
        count += 1
        return count <= max_items

    return _filter


def skip_first(max_items: int) -> ItemTransformFunctionNoMeta[bool]:
    """A filter that skips first `max_items` from a resource"""
    count: int = 0

    def _filter(_: TDataItem) -> bool:
        nonlocal count
        count += 1
        return count > max_items

    return _filter


def pivot(
    paths: Union[str, Sequence[str]] = "$", prefix: str = "col"
) -> ItemTransformFunctionNoMeta[TDataItem]:
    """
    Pivot the given sequence of sequences into a sequence of dicts,
    generating column names from the given prefix and indexes, e.g.:
    {"field": [[1, 2]]} -> {"field": [{"prefix_0": 1, "prefix_1": 2}]}

    Args:
        paths (Union[str, Sequence[str]]): JSON paths of the fields to pivot.
        prefix (Optional[str]): Prefix to add to the column names.

    Returns:
        ItemTransformFunctionNoMeta[TDataItem]: The transformer function.
    """
    if isinstance(paths, str):
        paths = [paths]

    def _seq_to_dict(seq: Sequence[Any]) -> Dict[str, Any]:
        """
        Transform the given sequence into a dict, generating
        columns with the given prefix.

        Args:
            seq (List): The sequence to transform.

        Returns:
            Dict: a dictionary with the sequence values.
        """
        return {prefix + str(i): value for i, value in enumerate(seq)}

    def _raise_if_not_sequence(match: jsonpath_ng.jsonpath.DatumInContext) -> None:
        """Check if the given field is a sequence of sequences.

        Args:
            match (jsonpath_ng.jsonpath.DatumInContext): The field to check.
        """
        if not isinstance(match.value, C_Sequence):
            raise ValueError(
                "Pivot transformer is only applicable to sequences "
                f"fields, however, the value of `{str(match.full_path)}`"
                " is not a sequence."
            )

        for item in match.value:
            if not isinstance(item, C_Sequence):
                raise ValueError(
                    "Pivot transformer is only applicable to sequences, "
                    f"however, the value of `{str(match.full_path)}` "
                    "includes a non-sequence element."
                )

    def _transformer(item: TDataItem) -> TDataItem:
        """Pivot the given sequence item into a sequence of dicts.

        Args:
            item (TDataItem): The data item to transform.

        Returns:
            TDataItem: the data item with pivoted columns.
        """
        for path in paths:
            expr = jsonpath_ng.parse(path)

            for match in expr.find([item] if path in "$" else item):
                trans_value = []
                _raise_if_not_sequence(match)

                for value in match.value:
                    trans_value.append(_seq_to_dict(value))

                if path == "$":
                    item = trans_value
                else:
                    match.full_path.update(item, trans_value)

        return item

    return _transformer


def add_row_hash_to_table(row_hash_column_name: str) -> TDataItem:
    """Computes content hash for each row of panda frame, arrow table or batch and adds it as `row_hash_column_name` column.

    Internally arrow tables and batches are converted to pandas DataFrame and then `hash_pandas_object` is used to
    generate a series with row hashes. Hashes are converted to signed int64 and added to original table. Data may be modified.
    For SCD2 use with a resource configuration that assigns custom row version column to `row_hash_column_name`
    """
    from dlt.common.libs import pyarrow
    from dlt.common.libs.pyarrow import pyarrow as pa
    from dlt.common.libs.pandas import pandas as pd

    def _unwrap(table: TDataItem) -> TDataItem:
        if is_arrow := pyarrow.is_arrow_item(table):
            df = table.to_pandas(deduplicate_objects=False)
        else:
            df = table

        hash_ = pd.util.hash_pandas_object(df, index=False)

        if is_arrow:
            table = pyarrow.append_column(
                table,
                row_hash_column_name,
                pa.Array.from_pandas(hash_, type=pa.int64(), safe=False),
            )
        else:
            hash_np = hash_.values.astype("int64", copy=False, casting="unsafe")
            table[row_hash_column_name] = hash_np

        return table

    return _unwrap
