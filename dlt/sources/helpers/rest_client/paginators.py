from abc import ABC, abstractmethod
from typing import Optional

from dlt.sources.helpers.requests import Response, Request
from dlt.common import jsonpath


class BasePaginator(ABC):
    def __init__(self) -> None:
        self._has_next_page = True
        self._next_reference: Optional[str] = None

    @property
    def has_next_page(self) -> bool:
        """
        Check if there is a next page available.

        Returns:
            bool: True if there is a next page available, False otherwise.
        """
        return self._has_next_page

    @property
    def next_reference(self) -> Optional[str]:
        return self._next_reference

    @next_reference.setter
    def next_reference(self, value: Optional[str]) -> None:
        self._next_reference = value
        self._has_next_page = value is not None

    @abstractmethod
    def update_state(self, response: Response) -> None:
        """Update the paginator state based on the response.

        Args:
            response (Response): The response object from the API.
        """
        ...

    @abstractmethod
    def update_request(self, request: Request) -> None:
        """
        Update the request object with the next arguments for the API request.

        Args:
            request (Request): The request object to be updated.
        """
        ...


class SinglePagePaginator(BasePaginator):
    """A paginator for single-page API responses."""

    def update_state(self, response: Response) -> None:
        self._has_next_page = False

    def update_request(self, request: Request) -> None:
        return


class OffsetPaginator(BasePaginator):
    """A paginator that uses the 'offset' parameter for pagination."""

    def __init__(
        self,
        initial_limit: int,
        initial_offset: int = 0,
        offset_param: str = "offset",
        limit_param: str = "limit",
        total_path: jsonpath.TJsonPath = "total",
    ) -> None:
        super().__init__()
        self.offset_param = offset_param
        self.limit_param = limit_param
        self.total_path = jsonpath.compile_path(total_path)

        self.offset = initial_offset
        self.limit = initial_limit

    def update_state(self, response: Response) -> None:
        values = jsonpath.find_values(self.total_path, response.json())
        total = values[0] if values else None

        if total is None:
            raise ValueError(
                f"Total count not found in response for {self.__class__.__name__}"
            )

        self.offset += self.limit

        if self.offset >= total:
            self._has_next_page = False

    def update_request(self, request: Request) -> None:
        if request.params is None:
            request.params = {}

        request.params[self.offset_param] = self.offset
        request.params[self.limit_param] = self.limit


class BaseNextUrlPaginator(BasePaginator):
    def update_request(self, request: Request) -> None:
        request.url = self.next_reference


class HeaderLinkPaginator(BaseNextUrlPaginator):
    """A paginator that uses the 'Link' header in HTTP responses
    for pagination.

    A good example of this is the GitHub API:
        https://docs.github.com/en/rest/guides/traversing-with-pagination
    """

    def __init__(self, links_next_key: str = "next") -> None:
        """
        Args:
            links_next_key (str, optional): The key (rel ) in the 'Link' header
                that contains the next page URL. Defaults to 'next'.
        """
        super().__init__()
        self.links_next_key = links_next_key

    def update_state(self, response: Response) -> None:
        self.next_reference = response.links.get(self.links_next_key, {}).get("url")


class JSONResponsePaginator(BaseNextUrlPaginator):
    """A paginator that uses a specific key in the JSON response to find
    the next page URL.
    """

    def __init__(
        self,
        next_url_path: jsonpath.TJsonPath = "next",
    ):
        """
        Args:
            next_url_path: The JSON path to the key that contains the next page URL in the response.
                Defaults to 'next'.
        """
        super().__init__()
        self.next_url_path = jsonpath.compile_path(next_url_path)

    def update_state(self, response: Response) -> None:
        values = jsonpath.find_values(self.next_url_path, response.json())
        self.next_reference = values[0] if values else None


class JSONResponseCursorPaginator(BasePaginator):
    """A paginator that uses a cursor query param to paginate. The cursor for the
    next page is found in the JSON response.
    """

    def __init__(
        self,
        cursor_path: jsonpath.TJsonPath = "cursors.next",
        cursor_param: str = "after",
    ):
        """
        Args:
            cursor_path: The JSON path to the key that contains the cursor in the response.
            cursor_param: The name of the query parameter to be used in the request to get the next page.
        """
        super().__init__()
        self.cursor_path = jsonpath.compile_path(cursor_path)
        self.cursor_param = cursor_param

    def update_state(self, response: Response) -> None:
        values = jsonpath.find_values(self.cursor_path, response.json())
        self.next_reference = values[0] if values else None

    def update_request(self, request: Request) -> None:
        if request.params is None:
            request.params = {}

        request.params[self.cursor_param] = self._next_reference
