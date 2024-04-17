import re
from typing import List, Dict, Any, Tuple, Union, Optional, Callable, Iterable

from requests import Response

from .paginators import (
    BasePaginator,
    HeaderLinkPaginator,
    JSONResponsePaginator,
    SinglePagePaginator,
)

RECORD_KEY_PATTERNS = frozenset(
    [
        "data",
        "items",
        "results",
        "entries",
        "records",
        "rows",
        "entities",
        "payload",
        "content",
        "objects",
    ]
)

NON_RECORD_KEY_PATTERNS = frozenset(
    [
        "meta",
        "metadata",
        "pagination",
        "links",
        "extras",
        "headers",
    ]
)

NEXT_PAGE_KEY_PATTERNS = frozenset(["next", "nextpage", "nexturl"])
NEXT_PAGE_DICT_KEY_PATTERNS = frozenset(["href", "url"])


def single_entity_path(path: str) -> bool:
    """Checks if path ends with path param indicating that single object is returned"""
    return re.search(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}$", path) is not None


def find_all_lists(
    dict_: Dict[str, Any],
    result: List[Tuple[int, str, List[Any]]] = None,
    level: int = 0,
) -> List[Tuple[int, str, List[Any]]]:
    """Recursively looks for lists in dict_ and returns tuples
    in format (nesting level, dictionary key, list)
    """
    if level > 2:
        return []

    for key, value in dict_.items():
        if isinstance(value, list):
            result.append((level, key, value))
        elif isinstance(value, dict):
            find_all_lists(value, result, level + 1)

    return result


def find_records(
    response: Union[Dict[str, Any], List[Any], Any],
) -> Union[Dict[str, Any], List[Any], Any]:
    # when a list was returned (or in rare case a simple type or null)
    if not isinstance(response, dict):
        return response
    lists = find_all_lists(response, result=[])
    if len(lists) == 0:
        # could not detect anything
        return response
    # we are ordered by nesting level, find the most suitable list
    try:
        return next(
            list_info[2]
            for list_info in lists
            if list_info[1] in RECORD_KEY_PATTERNS and list_info[1] not in NON_RECORD_KEY_PATTERNS
        )
    except StopIteration:
        # return the least nested element
        return lists[0][2]


def matches_any_pattern(key: str, patterns: Iterable[str]) -> bool:
    normalized_key = key.lower()
    return any(pattern in normalized_key for pattern in patterns)


def find_next_page_path(
    dictionary: Dict[str, Any], path: Optional[List[str]] = None
) -> Optional[List[str]]:
    if not isinstance(dictionary, dict):
        return None

    if path is None:
        path = []

    for key, value in dictionary.items():
        if matches_any_pattern(key, NEXT_PAGE_KEY_PATTERNS):
            if isinstance(value, dict):
                for dict_key in value:
                    if matches_any_pattern(dict_key, NEXT_PAGE_DICT_KEY_PATTERNS):
                        return [*path, key, dict_key]
            return [*path, key]

        if isinstance(value, dict):
            result = find_next_page_path(value, [*path, key])
            if result:
                return result

    return None


def header_links_detector(response: Response) -> Optional[HeaderLinkPaginator]:
    links_next_key = "next"

    if response.links.get(links_next_key):
        return HeaderLinkPaginator()
    return None


def json_links_detector(response: Response) -> Optional[JSONResponsePaginator]:
    dictionary = response.json()
    next_path_parts = find_next_page_path(dictionary)

    if not next_path_parts:
        return None

    return JSONResponsePaginator(next_url_path=".".join(next_path_parts))


def single_page_detector(response: Response) -> Optional[SinglePagePaginator]:
    """This is our fallback paginator, also for results that are single entities"""
    return SinglePagePaginator()


class PaginatorFactory:
    def __init__(self, detectors: List[Callable[[Response], Optional[BasePaginator]]] = None):
        if detectors is None:
            detectors = [
                header_links_detector,
                json_links_detector,
                single_page_detector,
            ]
        self.detectors = detectors

    def create_paginator(self, response: Response) -> Optional[BasePaginator]:
        for detector in self.detectors:
            paginator = detector(response)
            if paginator:
                return paginator
        return None
