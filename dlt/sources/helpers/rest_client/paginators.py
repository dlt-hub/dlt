from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from urllib.parse import urlparse, urljoin

from requests import Response, Request
from dlt.common import jsonpath


class BasePaginator(ABC):
    """A base class for all paginator implementations. Paginators are used
    to handle paginated responses from RESTful APIs.

    See `RESTClient.paginate()` for example usage.
    """

    def __init__(self) -> None:
        self._has_next_page = True

    @property
    def has_next_page(self) -> bool:
        """Determines if there is a next page available.

        Returns:
            bool: True if a next page is available, otherwise False.
        """
        return self._has_next_page

    @abstractmethod
    def update_state(self, response: Response) -> None:
        """Updates the paginator's state based on the response from the API.

        This method should extract necessary pagination details (like next page
        references) from the response and update the paginator's state
        accordingly. It should also set the `_has_next_page` attribute to
        indicate if there is a next page available.

        Args:
            response (Response): The response object from the API request.
        """
        ...

    @abstractmethod
    def update_request(self, request: Request) -> None:
        """Updates the request object with arguments for fetching the next page.

        This method should modify the request object to include necessary
        details (like URLs or parameters) for requesting the next page based on
        the current state of the paginator.

        Args:
            request (Request): The request object to be updated for the next
                page fetch.
        """
        ...


class SinglePagePaginator(BasePaginator):
    """A paginator for single-page API responses."""

    def update_state(self, response: Response) -> None:
        self._has_next_page = False

    def update_request(self, request: Request) -> None:
        return


class BaseNumericPaginator(BasePaginator):
    def __init__(
        self,
        param_name: str,
        initial_value: int,
        total_path: jsonpath.TJsonPath,
        value_step: int,
        error_message_items: str = "items",
    ):
        super().__init__()
        self.param_name = param_name
        self.current_value = initial_value
        self.total_path = jsonpath.compile_path(total_path)
        self.value_step = value_step
        self.error_message_items = error_message_items

    def update_state(self, response: Response) -> None:
        response_json = response.json()
        values = jsonpath.find_values(self.total_path, response_json)
        total = values[0] if values else None
        if total is None:
            self._handle_missing_total(response_json)

        try:
            total = int(total)
        except ValueError:
            self._handle_invalid_total(total)

        self.current_value += self.value_step

        if self.current_value >= total:
            self._has_next_page = False

    def _handle_missing_total(self, response_json: Dict[str, Any]) -> None:
        raise ValueError(
            f"Total {self.error_message_items} is not found in the response in"
            f" {self.__class__.__name__}. Expected a response with a '{self.total_path}' key, got"
            f" {response_json}"
        )

    def _handle_invalid_total(self, total: Any) -> None:
        raise ValueError(
            f"'{self.total_path}' is not an integer in the response in {self.__class__.__name__}."
            f" Expected an integer, got {total}"
        )

    def update_request(self, request: Request) -> None:
        if request.params is None:
            request.params = {}
        request.params[self.param_name] = self.current_value


class PageNumberPaginator(BaseNumericPaginator):
    """A paginator that uses page number-based pagination strategy.

    For example, consider an API located at `https://api.example.com/items`
    that supports pagination through page number and page size query parameters,
    and provides the total number of pages in its responses, as shown below:

        {
            "items": [...],
            "total_pages": 10
        }

    To use `PageNumberPaginator` with such an API, you can instantiate `RESTClient`
    as follows:

        from dlt.sources.helpers.rest_client import RESTClient

        client = RESTClient(
            base_url="https://api.example.com",
            paginator=PageNumberPaginator(
                total_pages_path="total_pages"
            )
        )

        @dlt.resource
        def get_items():
            for page in client.paginate("/items", params={"size": 100}):
                yield page

    Note that we pass the `size` parameter in the initial request to the API.
    The `PageNumberPaginator` will automatically increment the page number for
    each subsequent request until all items are fetched.
    """

    def __init__(
        self,
        initial_page: int = 1,
        page_param: str = "page",
        total_pages_path: jsonpath.TJsonPath = "total",
    ):
        """
        Args:
            initial_page (int): The initial page number.
            page_param (str): The query parameter name for the page number.
                Defaults to 'page'.
            total_pages_path (jsonpath.TJsonPath): The JSONPath expression for
                the total number of pages. Defaults to 'total'.
        """
        super().__init__(
            param_name=page_param,
            initial_value=initial_page,
            total_path=total_pages_path,
            value_step=1,
            error_message_items="pages",
        )


class OffsetPaginator(BaseNumericPaginator):
    """A paginator that uses offset-based pagination strategy.

    This paginator is useful for APIs where pagination is controlled
    through offset and limit query parameters and the total count of items
    is returned in the response.

    For example, consider an API located at `https://api.example.com/items`
    that supports pagination through offset and limit, and provides the total
    item count in its responses, as shown below:

        {
            "items": [...],
            "total": 1000
        }

    To use `OffsetPaginator` with such an API, you can instantiate `RESTClient`
    as follows:

        from dlt.sources.helpers.rest_client import RESTClient

        client = RESTClient(
            base_url="https://api.example.com",
            paginator=OffsetPaginator(
                initial_limit=100,
                total_path="total"
            )
        )
        @dlt.resource
        def get_items():
            for page in client.paginate("/items", params={"limit": 100}):
                yield page

    Note that we pass the `limit` parameter in the initial request to the API.
    The `OffsetPaginator` will automatically increment the offset for each
    subsequent request until all items are fetched.
    """

    def __init__(
        self,
        initial_limit: int,
        initial_offset: int = 0,
        offset_param: str = "offset",
        limit_param: str = "limit",
        total_path: jsonpath.TJsonPath = "total",
    ) -> None:
        """
        Args:
            initial_limit (int): The maximum number of items to retrieve
                in each request.
            initial_offset (int): The offset for the first request.
                Defaults to 0.
            offset_param (str): The query parameter name for the offset.
                Defaults to 'offset'.
            limit_param (str): The query parameter name for the limit.
                Defaults to 'limit'.
            total_path (jsonpath.TJsonPath): The JSONPath expression for
                the total number of items.
        """
        super().__init__(
            param_name=offset_param,
            initial_value=initial_offset,
            total_path=total_path,
            value_step=initial_limit,
        )
        self.limit_param = limit_param
        self.limit = initial_limit

    def update_request(self, request: Request) -> None:
        super().update_request(request)
        request.params[self.limit_param] = self.limit


class BaseReferencePaginator(BasePaginator):
    """A base paginator class for paginators that use a reference to the next
    page, such as a URL or a cursor string.

    Subclasses should implement:
      1. `update_state` method to extract the next page reference and
        set the `_next_reference` attribute accordingly.
      2. `update_request` method to update the request object with the next
        page reference.
    """

    def __init__(self) -> None:
        super().__init__()
        self.__next_reference: Optional[str] = None

    @property
    def _next_reference(self) -> Optional[str]:
        """The reference to the next page, such as a URL or a cursor.

        Returns:
            Optional[str]: The reference to the next page if available,
                otherwise None.
        """
        return self.__next_reference

    @_next_reference.setter
    def _next_reference(self, value: Optional[str]) -> None:
        """Sets the reference to the next page and updates the availability
        of the next page.

        Args:
            value (Optional[str]): The reference to the next page.
        """
        self.__next_reference = value
        self._has_next_page = value is not None


class BaseNextUrlPaginator(BaseReferencePaginator):
    """
    A base paginator class for paginators that use a URL provided in the API
    response to fetch the next page. For example, the URL can be found in HTTP
    headers or in the JSON response.

    Subclasses should implement the `update_state` method to extract the next
    page URL and set the `_next_reference` attribute accordingly.

    See `HeaderLinkPaginator` and `JSONResponsePaginator` for examples.
    """

    def update_request(self, request: Request) -> None:
        # Handle relative URLs
        if self._next_reference:
            parsed_url = urlparse(self._next_reference)
            if not parsed_url.scheme:
                self._next_reference = urljoin(request.url, self._next_reference)

        request.url = self._next_reference


class HeaderLinkPaginator(BaseNextUrlPaginator):
    """A paginator that uses the 'Link' header in HTTP responses
    for pagination.

    A good example of this is the GitHub API:
        https://docs.github.com/en/rest/guides/traversing-with-pagination

    For example, consider an API response that includes 'Link' header:

        ...
        Content-Type: application/json
        Link: <https://api.example.com/items?page=2>; rel="next", <https://api.example.com/items?page=1>; rel="prev"

        [
            {"id": 1, "name": "item1"},
            {"id": 2, "name": "item2"},
            ...
        ]

    In this scenario, the URL for the next page (`https://api.example.com/items?page=2`)
    is identified by its relation type `rel="next"`. `HeaderLinkPaginator` extracts
    this URL from the 'Link' header and uses it to fetch the next page of results:

        from dlt.sources.helpers.rest_client import RESTClient
        client = RESTClient(
            base_url="https://api.example.com",
            paginator=HeaderLinkPaginator()
        )

        @dlt.resource
        def get_issues():
            for page in client.paginate("/items"):
                yield page
    """

    def __init__(self, links_next_key: str = "next") -> None:
        """
        Args:
            links_next_key (str, optional): The key (rel) in the 'Link' header
                that contains the next page URL. Defaults to 'next'.
        """
        super().__init__()
        self.links_next_key = links_next_key

    def update_state(self, response: Response) -> None:
        """Extracts the next page URL from the 'Link' header in the response."""
        self._next_reference = response.links.get(self.links_next_key, {}).get("url")


class JSONResponsePaginator(BaseNextUrlPaginator):
    """Locates the next page URL within the JSON response body. The key
    containing the URL can be specified using a JSON path.

    For example, suppose the JSON response from an API contains data items
    along with a 'pagination' object:

        {
            "items": [
                {"id": 1, "name": "item1"},
                {"id": 2, "name": "item2"},
                ...
            ],
            "pagination": {
                "next": "https://api.example.com/items?page=2"
            }
        }

    The link to the next page (`https://api.example.com/items?page=2`) is
    located in the 'next' key of the 'pagination' object. You can use
    `JSONResponsePaginator` to paginate through the API endpoint:

        from dlt.sources.helpers.rest_client import RESTClient
        client = RESTClient(
            base_url="https://api.example.com",
            paginator=JSONResponsePaginator(next_url_path="pagination.next")
        )

        @dlt.resource
        def get_data():
            for page in client.paginate("/posts"):
                yield page
    """

    def __init__(
        self,
        next_url_path: jsonpath.TJsonPath = "next",
    ):
        """
        Args:
            next_url_path (jsonpath.TJsonPath): The JSON path to the key
                containing the next page URL in the response body.
                Defaults to 'next'.
        """
        super().__init__()
        self.next_url_path = jsonpath.compile_path(next_url_path)

    def update_state(self, response: Response) -> None:
        """Extracts the next page URL from the JSON response."""
        values = jsonpath.find_values(self.next_url_path, response.json())
        self._next_reference = values[0] if values else None


class JSONResponseCursorPaginator(BaseReferencePaginator):
    """Uses a cursor parameter for pagination, with the cursor value found in
    the JSON response body.

    For example, suppose the JSON response from an API contains
    a 'cursors' object:

        {
            "items": [
                {"id": 1, "name": "item1"},
                {"id": 2, "name": "item2"},
                ...
            ],
            "cursors": {
                "next": "aW1wb3J0IGFudGlncmF2aXR5"
            }
        }

    And the API endpoint expects a 'cursor' query parameter to fetch
    the next page. So the URL for the next page would look
    like `https://api.example.com/items?cursor=aW1wb3J0IGFudGlncmF2aXR5`.

    You can paginate through this API endpoint using
    `JSONResponseCursorPaginator`:

        from dlt.sources.helpers.rest_client import RESTClient
        client = RESTClient(
            base_url="https://api.example.com",
            paginator=JSONResponseCursorPaginator(
                cursor_path="cursors.next",
                cursor_param="cursor"
            )
        )

        @dlt.resource
        def get_data():
            for page in client.paginate("/posts"):
                yield page
    """

    def __init__(
        self,
        cursor_path: jsonpath.TJsonPath = "cursors.next",
        cursor_param: str = "after",
    ):
        """
        Args:
            cursor_path: The JSON path to the key that contains the cursor in
                the response.
            cursor_param: The name of the query parameter to be used in
                the request to get the next page.
        """
        super().__init__()
        self.cursor_path = jsonpath.compile_path(cursor_path)
        self.cursor_param = cursor_param

    def update_state(self, response: Response) -> None:
        """Extracts the cursor value from the JSON response."""
        values = jsonpath.find_values(self.cursor_path, response.json())
        self._next_reference = values[0] if values else None

    def update_request(self, request: Request) -> None:
        """Updates the request with the cursor query parameter."""
        if request.params is None:
            request.params = {}

        request.params[self.cursor_param] = self._next_reference
