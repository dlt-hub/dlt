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
    paths: Union[str, List[str]] = "$", prefix: str = ""
) -> ItemTransformFunctionNoMeta[TDataItem]:
    """Pivot the given values into a dictionary.

    Walks through the given JSON paths and turns lists
    of lists into lists of dicts, generating
    column names from the given prefix and indexes, e.g.:
    {"field": [[1, 2]]} -> {"field": [{"prefix_0": 1, "prefix_1": 2}]}

    Args:
        paths (Union[str, List[str]]): JSON paths to pivot.
        prefix (Optional[str]): Prefix to add to the column names.

    Returns:
        ItemTransformFunctionNoMeta[TDataItem]:
            A function to pivot inner lists into a dict.
    """
    if isinstance(paths, str):
        paths = [paths]

    def _list_to_dict(list_: List[Any]) -> Dict[str, Any]:
        """
        Transform the given list into a dict, generating
        columns with the given prefix.

        Args:
            list_ (List): The list to transform.

        Returns:
            Dict: a dictionary with the list values.
        """
        return {prefix + str(i): value for i, value in enumerate(list_)}

    def _is_list_of_lists(value: Any) -> bool:
        """Check if the given value is a list of lists.

        Args:
            value (Any): a value to check.

        Returns:
            bool: True if the value is a list of lists.
        """
        return all(isinstance(item, list) for item in value)

    def _transformer(item: TDataItem) -> TDataItem:
        """Pivot the given item into a dictionary.

        Args:
            item (TDataItem): A data item.

        Returns:
            TDataItem: a data item with pivoted columns.
        """
        trans_item: Dict[str, List[Any]] = {}
        for path in paths:
            expr = jsonpath_ng.parse(path)
            matches = expr.find(item)

            for match in matches:
                if _is_list_of_lists(match.value):
                    f_path = str(match.full_path)
                    trans_item[f_path] = []
                    for value in match.value:
                        trans_item[f_path].append(_list_to_dict(value))

        return trans_item

    return _transformer
