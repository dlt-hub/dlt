import warnings
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

from requests import Request, Response

from dlt.common import jsonpath
from dlt.common.utils import str2bool


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
        param_name: Optional[str],
        initial_value: int,
        value_step: int,
        base_index: int = 0,
        maximum_value: Optional[int] = None,
        total_path: Optional[jsonpath.TJsonPath] = None,
        error_message_items: str = "items",
        stop_after_empty_page: Optional[bool] = True,
        has_more_path: Optional[jsonpath.TJsonPath] = None,
        param_body_path: Optional[jsonpath.TJsonPath] = None,
    ):
        """
        Args:
            param_name (str): The query parameter name for the numeric value.
                For example, 'page'.
            param_body_path (jsonpath.TJsonPath): The JSONPath expression specifying
                where to place the numeric parameter in the request JSON body.
                Defaults to `None`.
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
            has_more_path (jsonpath.TJsonPath): The JSONPath expression for
                the boolean value indicating whether there are more items to fetch.
                Defaults to None.
        """
        super().__init__()
        if (
            total_path is None
            and maximum_value is None
            and has_more_path is None
            and not stop_after_empty_page
        ):
            raise ValueError(
                "One of `total_path`, `maximum_value`, `has_more_path`, or `stop_after_empty_page`"
                " must be provided."
            )
        self.param_name = param_name
        self.param_body_path = param_body_path
        self.initial_value = initial_value
        self.current_value = initial_value
        self.value_step = value_step
        self.base_index = base_index
        self.maximum_value = maximum_value
        self.total_path = jsonpath.compile_path(total_path) if total_path else None
        self.error_message_items = error_message_items
        self.stop_after_empty_page = stop_after_empty_page
        self.has_more_path = jsonpath.compile_path(has_more_path) if has_more_path else None

    def init_request(self, request: Request) -> None:
        self._has_next_page = True
        self.current_value = self.initial_value
        self.update_request(request)

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

            has_more = None
            if self.has_more_path:
                values = jsonpath.find_values(self.has_more_path, response.json())
                has_more = values[0] if values else None
                if has_more is None:
                    self._handle_missing_has_more(response.json())
                elif isinstance(has_more, str):
                    try:
                        has_more = str2bool(has_more)
                    except ValueError:
                        self._handle_invalid_has_more(has_more)
                elif not isinstance(has_more, bool):
                    self._handle_invalid_has_more(has_more)

                self._has_next_page = has_more

    def _stop_after_this_page(self, data: Optional[List[Any]] = None) -> bool:
        return self.stop_after_empty_page and not data

    def _handle_missing_total(self, response_json: Dict[str, Any]) -> None:
        raise ValueError(
            f"Total `{self.error_message_items}` not found in the response in"
            f" `{self.__class__.__name__}` .Expected a response with a `{self.total_path}` key, got"
            f" `{response_json}`."
        )

    def _handle_invalid_total(self, total: Any) -> None:
        raise ValueError(
            f"'{self.total_path}' is not an `int` in the response in `{self.__class__.__name__}`."
            f" Expected an integer, got `{total}`"
        )

    def _handle_missing_has_more(self, response_json: Dict[str, Any]) -> None:
        raise ValueError(
            f"Has more value not found in the response in `{self.__class__.__name__}`."
            f"Expected a response with a `{self.has_more_path}` key, got"
            f" `{response_json}`."
        )

    def _handle_invalid_has_more(self, has_more: Any) -> None:
        raise ValueError(
            f"'{self.has_more_path}' is not a `bool` in the response in"
            f" `{self.__class__.__name__}`. Expected a boolean, got `{has_more}`"
        )

    def update_request(self, request: Request) -> None:
        """Updates the request with the current value either in query parameters
        or in the request JSON body."""
        if self.param_body_path:
            self._update_request_with_body_path(request, self.param_body_path, self.current_value)
        else:
            self._update_request_with_param_name(request, self.param_name, self.current_value)

    @staticmethod
    def _update_request_with_param_name(
        request: Request, param_name: str, current_value: int
    ) -> None:
        if request.params is None:
            request.params = {}
        request.params[param_name] = current_value

    @staticmethod
    def _update_request_with_body_path(
        request: Request, body_path: jsonpath.TJsonPath, current_value: int
    ) -> None:
        if request.json is None:
            request.json = {}
        jsonpath.set_value_at_path(request.json, body_path, current_value)


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

    If the API provides a boolean value indicating whether there are more items
    to fetch, you can use the `has_more_path` parameter to stop the pagination
    when there are no more items to fetch. For example:

        {
            "items": [...],
            "has_more": false
        }

        client = RESTClient(
            base_url="https://api.example.com",
            paginator=PageNumberPaginator(
                total_path=None,
                has_more_path="has_more"
            )
        )
        ...

    In this case, pagination will stop once the `has_more` value is `false`.
    """

    def __init__(
        self,
        base_page: int = 0,
        page: int = None,
        page_param: Optional[str] = None,
        total_path: Optional[jsonpath.TJsonPath] = "total",
        maximum_page: Optional[int] = None,
        stop_after_empty_page: Optional[bool] = True,
        has_more_path: Optional[jsonpath.TJsonPath] = None,
        page_body_path: Optional[jsonpath.TJsonPath] = None,
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
            page_body_path (jsonpath.TJsonPath): A JSONPath expression specifying where
                to place the page number in the request JSON body. Use this instead
                of `page_param` when sending the page number in the request body.
                Defaults to `None`.
            total_path (jsonpath.TJsonPath): The JSONPath expression for
                the total number of pages. Defaults to 'total'.
            maximum_page (int): The maximum page number. If provided, pagination
                will stop once this page is reached or exceeded, even if more
                data is available. This allows you to limit the maximum number
                of pages for pagination. Defaults to None.
            stop_after_empty_page (bool): Whether pagination should stop when
              a page contains no result items. Defaults to `True`.
            has_more_path (jsonpath.TJsonPath): The JSONPath expression for
                the boolean value indicating whether there are more items to fetch.
                Defaults to None.
        """
        # For backward compatibility: set default cursor_param if both are None
        if page_param is None and page_body_path is None:
            page_param = "page"
        # Check that only one of page_param or page_body_path is provided
        elif page_param is not None and page_body_path is not None:
            raise ValueError("Either 'page_param' or 'page_body_path' must be provided, not both.")
        if (
            total_path is None
            and maximum_page is None
            and has_more_path is None
            and not stop_after_empty_page
        ):
            raise ValueError(
                "One of `total_path`, `maximum_page`, `has_more_path`, or `stop_after_empty_page`"
                " must be provided."
            )

        page = page if page is not None else base_page

        super().__init__(
            param_name=page_param,
            param_body_path=page_body_path,
            initial_value=page,
            base_index=base_page,
            total_path=total_path,
            value_step=1,
            maximum_value=maximum_page,
            error_message_items="pages",
            stop_after_empty_page=stop_after_empty_page,
            has_more_path=has_more_path,
        )

    def __str__(self) -> str:
        return (
            super().__str__()
            + f": current page: {self.current_value} "
            + (f"page_param: {self.param_name} " if self.param_name else "")
            + (f"page_body_path: {self.param_body_path} " if self.param_body_path else "")
            + f"total_path: {self.total_path} maximum_value: {self.maximum_value} "
            f"has_more_path: {self.has_more_path}"
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

    If the API provides a boolean value indicating whether there are more items
    to fetch, you can use the `has_more_path` parameter to stop the pagination
    when there are no more items to fetch. For example:

        {
            "items": [...],
            "has_more": false
        }

        client = RESTClient(
            base_url="https://api.example.com",
            paginator=OffsetPaginator(
                limit=100,
                total_path=None,
                has_more_path="has_more"
            )
        )
        ...

    In this case, pagination will stop once the `has_more` value is `false`.
    """

    def __init__(
        self,
        limit: int,
        offset: int = 0,
        offset_param: Optional[str] = None,
        limit_param: Optional[str] = None,
        total_path: Optional[jsonpath.TJsonPath] = "total",
        maximum_offset: Optional[int] = None,
        stop_after_empty_page: Optional[bool] = True,
        has_more_path: Optional[jsonpath.TJsonPath] = None,
        offset_body_path: Optional[jsonpath.TJsonPath] = None,
        limit_body_path: Optional[jsonpath.TJsonPath] = None,
    ) -> None:
        """
        Args:
            limit (int): The maximum number of items to retrieve
                in each request.
            offset (int): The offset for the first request.
                Defaults to 0.
            offset_param (str): The query parameter name for the offset.
                Defaults to 'offset'.
            offset_body_path (jsonpath.TJsonPath): A JSONPath expression specifying
                where to place the offset in the request JSON body.
                If provided, the paginator will use this instead of `offset_param`
                to send the offset in the request body. Defaults to `None`.
            limit_param (str): The query parameter name for the limit.
                Defaults to 'limit'.
            limit_body_path (jsonpath.TJsonPath): A JSONPath expression specifying
                where to place the limit in the request JSON body.
                If provided, the paginator will use this instead of `limit_param`
                to send the limit in the request body. Defaults to `None`.
            total_path (jsonpath.TJsonPath): The JSONPath expression for
                the total number of items.
            maximum_offset (int): The maximum offset value. If provided,
                pagination will stop once this offset is reached or exceeded,
                even if more data is available. This allows you to limit the
                maximum range for pagination. Defaults to None.
            stop_after_empty_page (bool): Whether pagination should stop when
              a page contains no result items. Defaults to `True`.
            has_more_path (jsonpath.TJsonPath): The JSONPath expression for
                the boolean value indicating whether there are more items to fetch.
                Defaults to None.
        """
        # For backward compatibility: set default offset_param if both are None
        if offset_param is None and offset_body_path is None:
            offset_param = "offset"
        # Check that only one of offset_param or offset_body_path is provided
        if offset_param is not None and offset_body_path is not None:
            raise ValueError(
                "Either 'offset_param' or 'offset_body_path' must be provided, not both."
            )

        if limit_param is None and limit_body_path is None:
            limit_param = "limit"
        # Check that only one of limit_param or limit_body_path is provided
        if limit_param is not None and limit_body_path is not None:
            raise ValueError(
                "Either 'limit_param' or 'limit_body_path' must be provided, not both."
            )

        if (
            total_path is None
            and maximum_offset is None
            and has_more_path is None
            and not stop_after_empty_page
        ):
            raise ValueError(
                "One of `total_path`, `maximum_offset`, `has_more_path`, or `stop_after_empty_page`"
                " must be provided."
            )
        super().__init__(
            param_name=offset_param,
            initial_value=offset,
            total_path=total_path,
            value_step=limit,
            maximum_value=maximum_offset,
            stop_after_empty_page=stop_after_empty_page,
            has_more_path=has_more_path,
            param_body_path=offset_body_path,
        )
        self.limit_param = limit_param
        self.limit_body_path = limit_body_path
        self.limit = limit

    def init_request(self, request: Request) -> None:
        super().init_request(request)
        self._update_request_limit(request)

    def update_request(self, request: Request) -> None:
        super().update_request(request)
        self._update_request_limit(request)

    def _update_request_limit(self, request: Request) -> None:
        if self.limit_body_path:
            self._update_request_with_body_path(request, self.limit_body_path, self.limit)
        else:
            self._update_request_with_param_name(request, self.limit_param, self.limit)

    def __str__(self) -> str:
        return (
            super().__str__()
            + f": current offset: {self.current_value} "
            + (f"offset_param: {self.param_name} " if self.param_name else "")
            + (f"offset_body_path: {self.param_body_path} " if self.param_body_path else "")
            + f"limit: {self.value_step} "
            + (f"limit_param: {self.limit_param} " if self.limit_param else "")
            + (f"limit_body_path: {self.limit_body_path} " if self.limit_body_path else "")
            + f"total_path: {self.total_path} "
            + f"maximum_value: {self.maximum_value} has_more_path: {self.has_more_path}"
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

    For requests with a JSON body, you can specify where to place the cursor in the request body
    using the `cursor_body_path` parameter. For example:

        from dlt.sources.helpers.rest_client import RESTClient
        client = RESTClient(
            base_url="https://api.example.com",
            paginator=JSONResponseCursorPaginator(
                cursor_path="nextPageToken",
                cursor_body_path="nextPageToken"
            )
        )

        @dlt.resource
        def get_data():
            for page in client.paginate(path="/search", method="POST", json={"query": "example"}):
                yield page
    """

    def __init__(
        self,
        cursor_path: jsonpath.TJsonPath = "cursors.next",
        cursor_param: Optional[str] = None,
        cursor_body_path: Optional[jsonpath.TJsonPath] = None,
    ):
        """
        Args:
            cursor_path: The JSON path to the key that contains the cursor in
                the response.
            cursor_param: The name of the query parameter to be used in
                the request to get the next page.
            cursor_body_path: The JSON path where to place the cursor in the request body.
        """
        super().__init__()
        self.cursor_path = jsonpath.compile_path(cursor_path)

        # For backward compatibility: set default cursor_param if both are None
        if cursor_param is None and cursor_body_path is None:
            cursor_param = "cursor"
        # Check that only one of cursor_param or cursor_body_path is provided
        elif cursor_param is not None and cursor_body_path is not None:
            raise ValueError(
                "Either 'cursor_param' or 'cursor_body_path' must be provided, not both."
            )

        self.cursor_param = cursor_param
        self.cursor_body_path = cursor_body_path

    def update_state(self, response: Response, data: Optional[List[Any]] = None) -> None:
        """Extracts the cursor value from the JSON response."""
        values = jsonpath.find_values(self.cursor_path, response.json())
        self._next_reference = values[0] if values and values[0] else None

    def update_request(self, request: Request) -> None:
        """Updates the request with the cursor value either in query parameters
        or in the request JSON body."""
        if self.cursor_body_path:
            if request.json is None:
                request.json = {}
            jsonpath.set_value_at_path(request.json, self.cursor_body_path, self._next_reference)
        else:
            if request.params is None:
                request.params = {}
            request.params[self.cursor_param] = self._next_reference

    def __str__(self) -> str:
        return (
            super().__str__()
            + f": cursor_path: {self.cursor_path}"
            + (f" cursor_param: {self.cursor_param}" if self.cursor_param else "")
            + (f" cursor_body_path: {self.cursor_body_path}" if self.cursor_body_path else "")
        )


class HeaderCursorPaginator(BaseReferencePaginator):
    """A paginator that uses a cursor in the HTTP responses header
    for pagination.

    For example, consider an API response that includes 'NextPageToken' header:

        ...
        Content-Type: application/json
        NextPageToken: 123456"

        [
            {"id": 1, "name": "item1"},
            {"id": 2, "name": "item2"},
            ...
        ]

    In this scenario, the parameter to construct the URL for the next page (`https://api.example.com/items?page=123456`)
    is identified by the `NextPageToken` header. `HeaderCursorPaginator` extracts
    this parameter from the header and uses it to fetch the next page of results:

        from dlt.sources.helpers.rest_client import RESTClient
        client = RESTClient(
            base_url="https://api.example.com",
            paginator=HeaderCursorPaginator()
        )

        @dlt.resource
        def get_issues():
            for page in client.paginate("/items"):
                yield page
    """

    def __init__(self, cursor_key: str = "next", cursor_param: str = "cursor") -> None:
        """
        Args:
            cursor_key (str, optional): The key in the header
                that contains the next page cursor value. Defaults to 'next'.
            cursor_param (str, optional): The param name to pass the token to
                for the next request. Defaults to 'cursor'.
        """
        super().__init__()
        self.cursor_key = cursor_key
        self.cursor_param = cursor_param

    def update_state(self, response: Response, data: Optional[List[Any]] = None) -> None:
        """Extracts the next page cursor from the header in the response."""
        self._next_reference = response.headers.get(self.cursor_key)

    def update_request(self, request: Request) -> None:
        """Updates the request with the cursor query parameter."""
        if request.params is None:
            request.params = {}

        request.params[self.cursor_param] = self._next_reference

    def __str__(self) -> str:
        return (
            super().__str__()
            + f": cursor_value: {self._next_reference} cursor_param: {self.cursor_param}"
        )
