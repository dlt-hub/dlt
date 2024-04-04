from typing import List, Union

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

    Args:
        columns (Union[str, List[str]]): JSON paths to pivot.
        prefix (Optional[str]): Prefix to add to the column names.

    Returns:
        ItemTransformFunctionNoMeta[TDataItem]:
            A function to pivot columns into a dict.
    """
    if isinstance(paths, str):
        paths = [paths]

    def _transformer(item: TDataItem) -> TDataItem:
        """Pivot columns into a dictionary.

        Args:
            item (TDataItem): a data item.

        Returns:
            TDataItem: a data item with pivoted columns.
        """
        trans_item = {}
        for path in paths:
            expr = jsonpath_ng.parse(path)
            matches = expr.find(item)

            for match in matches:
                trans_item[prefix + str(match.full_path)] = match.value

        return trans_item

    return _transformer
