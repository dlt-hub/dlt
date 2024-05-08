from typing import (
    Iterator,
    Optional,
    List,
    Dict,
    Any,
    TypeVar,
    Iterable,
    cast,
)
import copy
from urllib.parse import urlparse
from requests import Session as BaseSession  # noqa: I251
from requests import Response, Request

from dlt.common import jsonpath
from dlt.common import logger

from dlt.sources.helpers.requests.retry import Client

from .typing import HTTPMethodBasic, HTTPMethod, Hooks
from .paginators import BasePaginator
from .auth import AuthConfigBase
from .detector import PaginatorFactory, find_records
from .exceptions import IgnoreResponseException

from .utils import join_url


_T = TypeVar("_T")


class PageData(List[_T]):
    """A list of elements in a single page of results with attached request context.

    The context allows to inspect the response, paginator and authenticator, modify the request
    """

    def __init__(
        self,
        __iterable: Iterable[_T],
        request: Request,
        response: Response,
        paginator: BasePaginator,
        auth: AuthConfigBase,
    ):
        super().__init__(__iterable)
        self.request = request
        self.response = response
        self.paginator = paginator
        self.auth = auth


class RESTClient:
    """A generic REST client for making requests to an API with support for
    pagination and authentication.

    Args:
        base_url (str): The base URL of the API to make requests to.
        headers (Optional[Dict[str, str]]): Default headers to include in all requests.
        auth (Optional[AuthConfigBase]): Authentication configuration for all requests.
        paginator (Optional[BasePaginator]): Default paginator for handling paginated responses.
        data_selector (Optional[jsonpath.TJsonPath]): JSONPath selector for extracting data from responses.
        session (BaseSession): HTTP session for making requests.
        paginator_factory (Optional[PaginatorFactory]): Factory for creating paginator instances,
            used for detecting paginators.
    """

    def __init__(
        self,
        base_url: str,
        headers: Optional[Dict[str, str]] = None,
        auth: Optional[AuthConfigBase] = None,
        paginator: Optional[BasePaginator] = None,
        data_selector: Optional[jsonpath.TJsonPath] = None,
        session: BaseSession = None,
        paginator_factory: Optional[PaginatorFactory] = None,
    ) -> None:
        self.base_url = base_url
        self.headers = headers
        self.auth = auth

        if session:
            self._validate_session_raise_for_status(session)
            self.session = session
        else:
            self.session = Client(raise_for_status=False).session

        self.paginator = paginator
        self.pagination_factory = paginator_factory or PaginatorFactory()

        self.data_selector = data_selector

    def _validate_session_raise_for_status(self, session: BaseSession) -> None:
        # dlt.sources.helpers.requests.session.Session
        # has raise_for_status=True by default
        if getattr(self.session, "raise_for_status", False):
            logger.warning(
                "The session provided has raise_for_status enabled. "
                "This may cause unexpected behavior."
            )

    def _create_request(
        self,
        path: str,
        method: HTTPMethod,
        params: Dict[str, Any],
        json: Optional[Dict[str, Any]] = None,
        auth: Optional[AuthConfigBase] = None,
        hooks: Optional[Hooks] = None,
    ) -> Request:
        parsed_url = urlparse(path)
        if parsed_url.scheme in ("http", "https"):
            url = path
        else:
            url = join_url(self.base_url, path)

        return Request(
            method=method,
            url=url,
            headers=self.headers,
            params=params,
            json=json,
            auth=auth or self.auth,
            hooks=hooks,
        )

    def _send_request(self, request: Request) -> Response:
        logger.info(
            f"Making {request.method.upper()} request to {request.url}"
            f" with params={request.params}, json={request.json}"
        )

        prepared_request = self.session.prepare_request(request)

        return self.session.send(prepared_request)

    def request(self, path: str = "", method: HTTPMethod = "GET", **kwargs: Any) -> Response:
        prepared_request = self._create_request(
            path=path,
            method=method,
            **kwargs,
        )
        return self._send_request(prepared_request)

    def get(self, path: str, params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Response:
        return self.request(path, method="GET", params=params, **kwargs)

    def post(self, path: str, json: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Response:
        return self.request(path, method="POST", json=json, **kwargs)

    def paginate(
        self,
        path: str = "",
        method: HTTPMethodBasic = "GET",
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        auth: Optional[AuthConfigBase] = None,
        paginator: Optional[BasePaginator] = None,
        data_selector: Optional[jsonpath.TJsonPath] = None,
        hooks: Optional[Hooks] = None,
    ) -> Iterator[PageData[Any]]:
        """Iterates over paginated API responses, yielding pages of data.

        Args:
            path (str): Endpoint path for the request, relative to `base_url`.
            method (HTTPMethodBasic): HTTP method for the request, defaults to 'get'.
            params (Optional[Dict[str, Any]]): URL parameters for the request.
            json (Optional[Dict[str, Any]]): JSON payload for the request.
            auth (Optional[AuthConfigBase]): Authentication configuration for the request.
            paginator (Optional[BasePaginator]): Paginator instance for handling
                pagination logic.
            data_selector (Optional[jsonpath.TJsonPath]): JSONPath selector for
                extracting data from the response.
            hooks (Optional[Hooks]): Hooks to modify request/response objects. Note that
                when hooks are not provided, the default behavior is to raise an exception
                on error status codes.

        Yields:
            PageData[Any]: A page of data from the paginated API response, along with request and response context.

        Raises:
            HTTPError: If the response status code is not a success code. This is raised
                by default when hooks are not provided.

        Example:
            >>> client = RESTClient(base_url="https://api.example.com")
            >>> for page in client.paginate("/search", method="post", json={"query": "foo"}):
            >>>     print(page)
        """

        paginator = paginator if paginator else copy.deepcopy(self.paginator)
        auth = auth or self.auth
        data_selector = data_selector or self.data_selector
        hooks = hooks or {}

        def raise_for_status(response: Response, *args: Any, **kwargs: Any) -> None:
            response.raise_for_status()

        if "response" not in hooks:
            hooks["response"] = [raise_for_status]

        request = self._create_request(
            path=path, method=method, params=params, json=json, auth=auth, hooks=hooks
        )

        if paginator:
            paginator.init_request(request)

        while True:
            try:
                response = self._send_request(request)
            except IgnoreResponseException:
                break

            if paginator is None:
                paginator = self.detect_paginator(response)

            data = self.extract_response(response, data_selector)
            paginator.update_state(response)
            paginator.update_request(request)

            # yield data with context
            yield PageData(data, request=request, response=response, paginator=paginator, auth=auth)

            if not paginator.has_next_page:
                break

    def extract_response(self, response: Response, data_selector: jsonpath.TJsonPath) -> List[Any]:
        if data_selector:
            # we should compile data_selector
            data: Any = jsonpath.find_values(data_selector, response.json())
            # extract if single item selected
            data = data[0] if isinstance(data, list) and len(data) == 1 else data
        else:
            data = find_records(response.json())
        # wrap single pages into lists
        if not isinstance(data, list):
            data = [data]
        return cast(List[Any], data)

    def detect_paginator(self, response: Response) -> BasePaginator:
        """Detects a paginator for the response and returns it.

        Args:
            response (Response): The response to detect the paginator for.

        Returns:
            BasePaginator: The paginator instance that was detected.
        """
        paginator = self.pagination_factory.create_paginator(response)
        if paginator is None:
            raise ValueError(f"No suitable paginator found for the response at {response.url}")
        logger.info(f"Detected paginator: {paginator.__class__.__name__}")
        return paginator
