from collections.abc import Sequence
from typing import Any, Dict, List, Union

from dlt.common.typing import TDataItem
from dlt.extract.items import ItemTransformFunctionNoMeta

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
    paths: Union[str, Sequence[str]] = "$", prefix: str = ""
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
        if not isinstance(match.value, Sequence):
            raise ValueError(
                (
                    "Pivot transformer is only applicable to sequences "
                    f"fields, however, the value of {str(match.full_path)}"
                    " is not a sequence."
                )
            )

        for item in match.value:
            if not isinstance(item, Sequence):
                raise ValueError(
                    (
                        "Pivot transformer is only applicable to sequences, "
                        f"however, the value of {str(match.full_path)} "
                        "includes a non-sequence element."
                    )
                )

    def _transformer(item: TDataItem) -> TDataItem:
        """Pivot the given sequence item into a sequence of dicts.

        Args:
            item (TDataItem): The data item to transform.

        Returns:
            TDataItem: the data item with pivoted columns.
        """
        trans_item: Dict[str, List[Any]] = {}

        for path in paths:
            expr = jsonpath_ng.parse(path)
            for match in expr.find(item):
                _raise_if_not_sequence(match)

                f_path = str(match.full_path)
                trans_item[f_path] = []

                for value in match.value:
                    trans_item[f_path].append(_seq_to_dict(value))

        return trans_item

    return _transformer
