import re
from pathlib import PurePosixPath
from typing import List, Dict, Any, Tuple, Union, Callable, Iterable
from urllib.parse import urlparse

from requests import Response

from .paginators import (
    BasePaginator,
    HeaderLinkPaginator,
    JSONResponsePaginator,
    JSONResponseCursorPaginator,
    SinglePagePaginator,
    PageNumberPaginator,
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
        "values",
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

NEXT_PAGE_KEY_PATTERNS = frozenset(["next"])
NEXT_PAGE_DICT_KEY_PATTERNS = frozenset(["href", "url"])
TOTAL_PAGES_KEYS = frozenset(["^total_pages$", "^pages$", "^totalpages$"])


def single_entity_path(path: str) -> bool:
    """Checks if path ends with path param indicating that single object is returned"""
    # get last path segment
    name = PurePosixPath(path).name
    # alphabet for a name taken from https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md#fixed-fields-6
    return re.search(r"\{([a-zA-Z0-9\.\-_]+)\}", name) is not None


def matches_any_pattern(key: str, patterns: Iterable[str]) -> bool:
    normalized_key = key.lower()
    return any(re.match(pattern, normalized_key) for pattern in patterns)


def find_all_lists(
    dict_: Dict[str, Any],
    path: Tuple[str, ...] = (),
    result: List[Tuple[Tuple[str, ...], List[Any]]] = None,
) -> List[Tuple[Tuple[str, ...], List[Any]]]:
    """Recursively looks for lists in dict_ and returns tuples
    in format (dictionary keys, list)
    """
    if len(path) > 2:
        return None

    for key, value in dict_.items():
        if isinstance(value, list):
            result.append(((*path, key), value))
        elif isinstance(value, dict):
            find_all_lists(value, path=(*path, key), result=result)

    return result


def find_response_page_data(
    response: Union[Dict[str, Any], List[Any], Any],
) -> Tuple[Tuple[str, ...], Any]:
    """Finds a path to response data, assuming that data is a list, returns a tuple(path, data)"""
    # when a list was returned (or in rare case a simple type or null)
    if not isinstance(response, dict):
        return (("$",), response)
    lists = find_all_lists(response, result=[])
    if len(lists) == 0:
        # could not detect anything
        return (("$",), response)
    # we are ordered by nesting level, find the most suitable list
    try:
        return next(
            list_info
            for list_info in lists
            if list_info[0][-1] in RECORD_KEY_PATTERNS
            and list_info[0][-1] not in NON_RECORD_KEY_PATTERNS
        )
    except StopIteration:
        # return the least nested element
        return lists[0]


def find_next_page_path(
    response: Dict[str, Any], path: Tuple[str, ...] = ()
) -> Tuple[Tuple[str, ...], Any]:
    if not isinstance(response, dict):
        return (None, None)

    for key, value in response.items():
        if matches_any_pattern(key, NEXT_PAGE_KEY_PATTERNS):
            if isinstance(value, dict):
                for dict_key, dict_value in value.items():
                    if matches_any_pattern(dict_key, NEXT_PAGE_DICT_KEY_PATTERNS) and isinstance(
                        dict_value, (str, int, float)
                    ):
                        return ((*path, key, dict_key), dict_value)
            else:
                if isinstance(value, (str, int, float)):
                    return ((*path, key), value)

        if isinstance(value, dict):
            result = find_next_page_path(value, (*path, key))
            if result != (None, None):
                return result

    return (None, None)


def find_total_pages_path(
    response: Dict[str, Any], path: Tuple[str, ...] = ()
) -> Tuple[Tuple[str, ...], Any]:
    if not isinstance(response, dict):
        return (None, None)

    for key, value in response.items():
        if matches_any_pattern(key, TOTAL_PAGES_KEYS) and isinstance(value, (str, int, float)):
            assert key != "pageSize"
            return ((*path, key), value)

        if isinstance(value, dict):
            result = find_total_pages_path(value, (*path, key))
            if result != (None, None):
                return result

    return (None, None)


def header_links_detector(response: Response) -> Tuple[HeaderLinkPaginator, float]:
    links_next_key = "next"

    if response.links.get(links_next_key):
        return HeaderLinkPaginator(), 1.0
    return None, None


def json_links_detector(response: Response) -> Tuple[JSONResponsePaginator, float]:
    dictionary = response.json()
    next_path_parts, next_href = find_next_page_path(dictionary)

    if not next_path_parts:
        return None, None

    try:
        urlparse(next_href)
        if next_href.startswith("http") or next_href.startswith("/"):
            return JSONResponsePaginator(next_url_path=".".join(next_path_parts)), 1.0
    except Exception:
        pass

    return None, None


def cursor_paginator_detector(response: Response) -> Tuple[JSONResponseCursorPaginator, float]:
    dictionary = response.json()
    cursor_path_parts, _ = find_next_page_path(dictionary)

    if not cursor_path_parts:
        return None, None

    return JSONResponseCursorPaginator(cursor_path=".".join(cursor_path_parts)), 0.5


def pages_number_paginator_detector(response: Response) -> Tuple[PageNumberPaginator, float]:
    total_pages_path, total_pages = find_total_pages_path(response.json())
    if not total_pages_path:
        return None, None

    try:
        int(total_pages)
        return PageNumberPaginator(total_path=".".join(total_pages_path)), 0.5
    except Exception:
        pass

    return None, None


def single_page_detector(response: Response) -> Tuple[SinglePagePaginator, float]:
    """This is our fallback paginator, also for results that are single entities"""
    return SinglePagePaginator(), 0.0


class PaginatorFactory:
    def __init__(self, detectors: List[Callable[[Response], Tuple[BasePaginator, float]]] = None):
        """`detectors` are functions taking Response as input and returning paginator instance and
        detection score. Score value:
        1.0 - perfect detection
        0.0 - fallback detection
        in between - partial detection, several paginator parameters are defaults
        """
        if detectors is None:
            detectors = [
                header_links_detector,
                json_links_detector,
                pages_number_paginator_detector,
                cursor_paginator_detector,
                single_page_detector,
            ]
        self.detectors = detectors

    def create_paginator(self, response: Response) -> Tuple[BasePaginator, float]:
        for detector in self.detectors:
            paginator, score = detector(response)
            if paginator:
                return paginator, score
        return None, None
