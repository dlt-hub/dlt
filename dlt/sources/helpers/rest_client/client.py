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
from requests.auth import AuthBase

from dlt.common import jsonpath, logger

from dlt.sources.helpers.requests.retry import Client

from .typing import HTTPMethodBasic, HTTPMethod, Hooks
from .paginators import BasePaginator
from .detector import PaginatorFactory, find_response_page_data
from .exceptions import IgnoreResponseException, PaginatorNotFound

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
        auth: AuthBase,
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
        auth (Optional[AuthBase]): Authentication configuration for all requests.
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
        auth: Optional[AuthBase] = None,
        paginator: Optional[BasePaginator] = None,
        data_selector: Optional[jsonpath.TJsonPath] = None,
        session: BaseSession = None,
        paginator_factory: Optional[PaginatorFactory] = None,
    ) -> None:
        self.base_url = base_url
        self.headers = headers
        self.auth = auth

        if session:
            # dlt.sources.helpers.requests.session.Session
            # has raise_for_status=True by default
            self.session = _warn_if_raise_for_status_and_return(session)
        else:
            self.session = Client(raise_for_status=False).session

        self.paginator = paginator
        self.pagination_factory = paginator_factory or PaginatorFactory()

        self.data_selector = data_selector

    def _create_request(
        self,
        path: str,
        method: HTTPMethod,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        auth: Optional[AuthBase] = None,
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

    def _send_request(self, request: Request, **kwargs: Any) -> Response:
        logger.info(
            f"Making {request.method.upper()} request to {request.url}"
            f" with params={request.params}, json={request.json}"
        )

        prepared_request = self.session.prepare_request(request)

        send_kwargs = self.session.merge_environment_settings(
            prepared_request.url,
            kwargs.pop("proxies", {}),
            kwargs.pop("stream", None),
            kwargs.pop("verify", None),
            kwargs.pop("cert", None),
        )

        send_kwargs.update(**kwargs)  #  type: ignore[call-arg]
        return self.session.send(prepared_request, **send_kwargs)

    def request(self, path: str = "", method: HTTPMethod = "GET", **kwargs: Any) -> Response:
        prepared_request = self._create_request(
            path=path,
            method=method,
            params=kwargs.pop("params", None),
            json=kwargs.pop("json", None),
            auth=kwargs.pop("auth", None),
            hooks=kwargs.pop("hooks", None),
        )
        return self._send_request(prepared_request, **kwargs)

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
        auth: Optional[AuthBase] = None,
        paginator: Optional[BasePaginator] = None,
        data_selector: Optional[jsonpath.TJsonPath] = None,
        hooks: Optional[Hooks] = None,
        **kwargs: Any,
    ) -> Iterator[PageData[Any]]:
        """Iterates over paginated API responses, yielding pages of data.

        Args:
            path (str): Endpoint path for the request, relative to `base_url`.
            method (HTTPMethodBasic): HTTP method for the request, defaults to 'get'.
            params (Optional[Dict[str, Any]]): URL parameters for the request.
            json (Optional[Dict[str, Any]]): JSON payload for the request.
            auth (Optional[AuthBase): Authentication configuration for the request.
            paginator (Optional[BasePaginator]): Paginator instance for handling
                pagination logic.
            data_selector (Optional[jsonpath.TJsonPath]): JSONPath selector for
                extracting data from the response.
            hooks (Optional[Hooks]): Hooks to modify request/response objects. Note that
                when hooks are not provided, the default behavior is to raise an exception
                on error status codes.
            **kwargs (Any): Optional arguments to that the Request library accepts, such as
                `stream`, `verify`, `proxies`, `cert`, `timeout`, and `allow_redirects`.


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
                response = self._send_request(request, **kwargs)
            except IgnoreResponseException:
                break

            if not data_selector:
                data_selector = self.detect_data_selector(response)
            data = self.extract_response(response, data_selector)

            if paginator is None:
                paginator = self.detect_paginator(response, data)
            paginator.update_state(response, data)
            paginator.update_request(request)

            # yield data with context
            yield PageData(data, request=request, response=response, paginator=paginator, auth=auth)

            if not paginator.has_next_page:
                logger.info(f"Paginator {str(paginator)} does not have more pages")
                break

    def extract_response(self, response: Response, data_selector: jsonpath.TJsonPath) -> List[Any]:
        # we should compile data_selector
        data: Any = jsonpath.find_values(data_selector, response.json())
        # extract if single item selected
        data = data[0] if isinstance(data, list) and len(data) == 1 else data
        if isinstance(data, list):
            length_info = f" with length {len(data)}"
        else:
            length_info = ""
        logger.info(
            f"Extracted data of type {type(data).__name__} from path {data_selector}{length_info}"
        )
        # wrap single pages into lists
        if not isinstance(data, list):
            data = [data]
        return cast(List[Any], data)

    def detect_data_selector(self, response: Response) -> str:
        """Detects a path to page data in `response`. If there's no
           paging detected, returns "$" which will select full response

        Returns:
            str: a json path to the page data.
        """
        path, data = find_response_page_data(response.json())
        data_selector = ".".join(path)
        # if list is detected, it is probably a paged data
        if isinstance(data, list):
            logger.info(
                f"Detected page data at path: '{data_selector}' type: list length: {len(data)}"
            )
        else:
            logger.info(f"Detected single page data at path: '{path}' type: {type(data).__name__}")
        return data_selector

    def detect_paginator(self, response: Response, data: Any) -> BasePaginator:
        """Detects a paginator for the response and returns it.

        Args:
            response (Response): The response to detect the paginator for.
            data_selector (data_selector): Path to paginated data or $ if paginated data not detected

        Returns:
            BasePaginator: The paginator instance that was detected.
        """
        paginator, score = self.pagination_factory.create_paginator(response)
        if paginator is None:
            raise PaginatorNotFound(
                f"No suitable paginator found for the response at {response.url}"
            )
        if score == 1.0:
            logger.info(f"Detected paginator: {paginator}")
        elif score == 0.0:
            if isinstance(data, list):
                logger.warning(
                    f"Fallback paginator used: {paginator}. Please provide right paginator"
                    " manually."
                )
            else:
                logger.info(
                    f"Fallback paginator used: {paginator} to handle not paginated response."
                )
        else:
            logger.warning(
                "Please verify the paginator settings. We strongly suggest to use explicit"
                " instance of the paginator as some settings may not be guessed correctly."
            )
        return paginator


def _warn_if_raise_for_status_and_return(session: BaseSession) -> BaseSession:
    """A generic function to warn if the session has raise_for_status enabled."""
    if getattr(session, "raise_for_status", False):
        logger.warning(
            "The session provided has raise_for_status enabled. This may cause unexpected behavior."
        )
    return session
