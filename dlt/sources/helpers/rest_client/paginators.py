import warnings
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
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

    def init_request(self, request: Request) -> None:  # noqa: B027, optional override
        """Initializes the request object with parameters for the first
        pagination request.

        This method can be overridden by subclasses to include specific
        initialization logic.

        Args:
            request (Request): The request object to be initialized.
        """
        pass

    @abstractmethod
    def update_state(self, response: Response, data: Optional[List[Any]] = None) -> None:
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

    def __str__(self) -> str:
        return f"{type(self).__name__} at {id(self):x}"


class SinglePagePaginator(BasePaginator):
    """A paginator for single-page API responses."""

    def update_state(self, response: Response, data: Optional[List[Any]] = None) -> None:
        self._has_next_page = False

    def update_request(self, request: Request) -> None:
        return


class RangePaginator(BasePaginator):
    """A base paginator class for paginators that use a numeric parameter
    for pagination, such as page number or offset.

    See `PageNumberPaginator` and `OffsetPaginator` for examples.
    """

    def __init__(
        self,
        param_name: str,
        initial_value: int,
        value_step: int,
        base_index: int = 0,
        maximum_value: Optional[int] = None,
        total_path: Optional[jsonpath.TJsonPath] = None,
        error_message_items: str = "items",
        stop_after_empty_page: Optional[bool] = True,
    ):
        """
        Args:
            param_name (str): The query parameter name for the numeric value.
                For example, 'page'.
            initial_value (int): The initial value of the numeric parameter.
            value_step (int): The step size to increment the numeric parameter.
            base_index (int, optional): The index of the initial element.
                Used to define 0-based or 1-based indexing. Defaults to 0.
            maximum_value (int, optional): The maximum value for the numeric parameter.
                If provided, pagination will stop once this value is reached
                or exceeded, even if more data is available. This allows you
                to limit the maximum range for pagination.
                If not provided, `total_path` must be specified. Defaults to None.
            total_path (jsonpath.TJsonPath, optional): The JSONPath expression
                for the total number of items. For example, if the JSON response is
                `{"items": [...], "total": 100}`, the `total_path` would be 'total'.
                If not provided, `maximum_value` must be specified.
            error_message_items (str): The name of the items in the error message.
                Defaults to 'items'.
            stop_after_empty_page (bool): Whether pagination should stop when
              a page contains no result items. Defaults to `True`.
        """
        super().__init__()
        if total_path is None and maximum_value is None and not stop_after_empty_page:
            raise ValueError(
                "Either `total_path` or `maximum_value` or `stop_after_empty_page` must be"
                " provided."
            )
        self.param_name = param_name
        self.current_value = initial_value
        self.value_step = value_step
        self.base_index = base_index
        self.maximum_value = maximum_value
        self.total_path = jsonpath.compile_path(total_path) if total_path else None
        self.error_message_items = error_message_items
        self.stop_after_empty_page = stop_after_empty_page

    def init_request(self, request: Request) -> None:
        if request.params is None:
            request.params = {}

        request.params[self.param_name] = self.current_value

    def update_state(self, response: Response, data: Optional[List[Any]] = None) -> None:
        if self._stop_after_this_page(data):
            self._has_next_page = False
        else:
            total = None
            if self.total_path:
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

            if (total is not None and self.current_value >= total + self.base_index) or (
                self.maximum_value is not None and self.current_value >= self.maximum_value
            ):
                self._has_next_page = False

    def _stop_after_this_page(self, data: Optional[List[Any]] = None) -> bool:
        return self.stop_after_empty_page and not data

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


class PageNumberPaginator(RangePaginator):
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
                total_path="total_pages"
            )
        )

        @dlt.resource
        def get_items():
            for page in client.paginate("/items", params={"size": 100}):
                yield page

    Note that we pass the `size` parameter in the initial request to the API.
    The `PageNumberPaginator` will automatically increment the page number for
    each subsequent request until all items are fetched.

    If the API does not provide the total number of pages, you can use the
    `maximum_page` parameter to limit the number of pages to fetch. For example:

        client = RESTClient(
            base_url="https://api.example.com",
            paginator=PageNumberPaginator(
                maximum_page=5,
                total_path=None
            )
        )
        ...

    In this case, pagination will stop after fetching 5 pages of data.
    """

    def __init__(
        self,
        base_page: int = 0,
        page: int = None,
        page_param: str = "page",
        total_path: jsonpath.TJsonPath = "total",
        maximum_page: Optional[int] = None,
        stop_after_empty_page: Optional[bool] = True,
    ):
        """
        Args:
            base_page (int): The index of the initial page from the API perspective.
                Determines the page number that the API server uses for the starting
                page. Normally, this is 0-based or 1-based (e.g., 1, 2, 3, ...)
                indexing for the pages. Defaults to 0.
            page (int): The page number for the first request. If not provided,
                the initial value will be set to `base_page`.
            page_param (str): The query parameter name for the page number.
                Defaults to 'page'.
            total_path (jsonpath.TJsonPath): The JSONPath expression for
                the total number of pages. Defaults to 'total'.
            maximum_page (int): The maximum page number. If provided, pagination
                will stop once this page is reached or exceeded, even if more
                data is available. This allows you to limit the maximum number
                of pages for pagination. Defaults to None.
            stop_after_empty_page (bool): Whether pagination should stop when
              a page contains no result items. Defaults to `True`.
        """
        if total_path is None and maximum_page is None and not stop_after_empty_page:
            raise ValueError(
                "Either `total_path` or `maximum_page` or `stop_after_empty_page` must be provided."
            )

        page = page if page is not None else base_page

        super().__init__(
            param_name=page_param,
            initial_value=page,
            base_index=base_page,
            total_path=total_path,
            value_step=1,
            maximum_value=maximum_page,
            error_message_items="pages",
            stop_after_empty_page=stop_after_empty_page,
        )

    def __str__(self) -> str:
        return (
            super().__str__()
            + f": current page: {self.current_value} page_param: {self.param_name} total_path:"
            f" {self.total_path} maximum_value: {self.maximum_value}"
        )


class OffsetPaginator(RangePaginator):
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
                limit=100,
                total_path="total"
            )
        )
        @dlt.resource
        def get_items():
            for page in client.paginate("/items"):
                yield page

    The `OffsetPaginator` will automatically increment the offset for each
    subsequent request until all items are fetched.

    If the API does not provide the total count of items, you can use the
    `maximum_offset` parameter to limit the number of items to fetch. For example:

        client = RESTClient(
            base_url="https://api.example.com",
            paginator=OffsetPaginator(
                limit=100,
                maximum_offset=1000,
                total_path=None
            )
        )
        ...

    In this case, pagination will stop after fetching 1000 items.
    """

    def __init__(
        self,
        limit: int,
        offset: int = 0,
        offset_param: str = "offset",
        limit_param: str = "limit",
        total_path: jsonpath.TJsonPath = "total",
        maximum_offset: Optional[int] = None,
        stop_after_empty_page: Optional[bool] = True,
    ) -> None:
        """
        Args:
            limit (int): The maximum number of items to retrieve
                in each request.
            offset (int): The offset for the first request.
                Defaults to 0.
            offset_param (str): The query parameter name for the offset.
                Defaults to 'offset'.
            limit_param (str): The query parameter name for the limit.
                Defaults to 'limit'.
            total_path (jsonpath.TJsonPath): The JSONPath expression for
                the total number of items.
            maximum_offset (int): The maximum offset value. If provided,
                pagination will stop once this offset is reached or exceeded,
                even if more data is available. This allows you to limit the
                maximum range for pagination. Defaults to None.
            stop_after_empty_page (bool): Whether pagination should stop when
              a page contains no result items. Defaults to `True`.
        """
        if total_path is None and maximum_offset is None and not stop_after_empty_page:
            raise ValueError(
                "Either `total_path` or `maximum_offset` or `stop_after_empty_page` must be"
                " provided."
            )
        super().__init__(
            param_name=offset_param,
            initial_value=offset,
            total_path=total_path,
            value_step=limit,
            maximum_value=maximum_offset,
            stop_after_empty_page=stop_after_empty_page,
        )
        self.limit_param = limit_param
        self.limit = limit

    def init_request(self, request: Request) -> None:
        super().init_request(request)
        request.params[self.limit_param] = self.limit

    def update_request(self, request: Request) -> None:
        super().update_request(request)
        request.params[self.limit_param] = self.limit

    def __str__(self) -> str:
        return (
            super().__str__()
            + f": current offset: {self.current_value} offset_param: {self.param_name} limit:"
            f" {self.value_step} total_path: {self.total_path} maximum_value:"
            f" {self.maximum_value}"
        )


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

    See `HeaderLinkPaginator` and `JSONLinkPaginator` for examples.
    """

    def update_request(self, request: Request) -> None:
        # Handle relative URLs
        if self._next_reference:
            parsed_url = urlparse(self._next_reference)
            if not parsed_url.scheme:
                self._next_reference = urljoin(request.url, self._next_reference)

        request.url = self._next_reference

        # Clear the query parameters from the previous request otherwise they
        # will be appended to the next URL in Session.prepare_request
        request.params = None


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

    def update_state(self, response: Response, data: Optional[List[Any]] = None) -> None:
        """Extracts the next page URL from the 'Link' header in the response."""
        self._next_reference = response.links.get(self.links_next_key, {}).get("url")

    def __str__(self) -> str:
        return super().__str__() + f": links_next_key: {self.links_next_key}"


class JSONLinkPaginator(BaseNextUrlPaginator):
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
    `JSONLinkPaginator` to paginate through the API endpoint:

        from dlt.sources.helpers.rest_client import RESTClient
        client = RESTClient(
            base_url="https://api.example.com",
            paginator=JSONLinkPaginator(next_url_path="pagination.next")
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

    def update_state(self, response: Response, data: Optional[List[Any]] = None) -> None:
        """Extracts the next page URL from the JSON response."""
        values = jsonpath.find_values(self.next_url_path, response.json())
        self._next_reference = values[0] if values else None

    def __str__(self) -> str:
        return super().__str__() + f": next_url_path: {self.next_url_path}"


class JSONResponsePaginator(JSONLinkPaginator):
    def __init__(
        self,
        next_url_path: jsonpath.TJsonPath = "next",
    ) -> None:
        warnings.warn(
            "JSONResponsePaginator is deprecated and will be removed in version 1.0.0. Use"
            " JSONLinkPaginator instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(next_url_path)


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
        cursor_param: str = "cursor",
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

    def update_state(self, response: Response, data: Optional[List[Any]] = None) -> None:
        """Extracts the cursor value from the JSON response."""
        values = jsonpath.find_values(self.cursor_path, response.json())
        self._next_reference = values[0] if values else None

    def update_request(self, request: Request) -> None:
        """Updates the request with the cursor query parameter."""
        if request.params is None:
            request.params = {}

        request.params[self.cursor_param] = self._next_reference

    def __str__(self) -> str:
        return (
            super().__str__()
            + f": cursor_path: {self.cursor_path} cursor_param: {self.cursor_param}"
        )
