from typing import Optional, Dict, Iterator, Any

from dlt.common import jsonpath

from .client import RESTClient  # noqa: F401
from .client import PageData
from .auth import AuthConfigBase
from .paginators import BasePaginator
from .typing import HTTPMethodBasic, Hooks


def paginate(
    url: str,
    method: HTTPMethodBasic = "GET",
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json: Optional[Dict[str, Any]] = None,
    auth: AuthConfigBase = None,
    paginator: Optional[BasePaginator] = None,
    data_selector: Optional[jsonpath.TJsonPath] = None,
    hooks: Optional[Hooks] = None,
) -> Iterator[PageData[Any]]:
    """
    Paginate over a REST API endpoint.

    Args:
        url: URL to paginate over.
        **kwargs: Keyword arguments to pass to `RESTClient.paginate`.

    Returns:
        Iterator[Page]: Iterator over pages.
    """
    client = RESTClient(
        base_url=url,
        headers=headers,
    )
    return client.paginate(
        path="",
        method=method,
        params=params,
        json=json,
        auth=auth,
        paginator=paginator,
        data_selector=data_selector,
        hooks=hooks,
    )
