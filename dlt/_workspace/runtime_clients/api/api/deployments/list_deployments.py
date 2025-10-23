# Python internals
from http import HTTPStatus
from typing import Any, Optional, Union
from uuid import UUID

# Other libraries
import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.list_deployments_response_200 import ListDeploymentsResponse200
from ...models.list_deployments_response_400 import ListDeploymentsResponse400
from ...models.list_deployments_response_401 import ListDeploymentsResponse401
from ...models.list_deployments_response_403 import ListDeploymentsResponse403
from ...models.list_deployments_response_404 import ListDeploymentsResponse404
from ...types import UNSET, Response, Unset


def _get_kwargs(
    workspace_id: UUID,
    *,
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["limit"] = limit

    params["offset"] = offset

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/v1/workspaces/{workspace_id}/deployments".format(
            workspace_id=workspace_id,
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[
    Union[
        ListDeploymentsResponse200,
        ListDeploymentsResponse400,
        ListDeploymentsResponse401,
        ListDeploymentsResponse403,
        ListDeploymentsResponse404,
    ]
]:
    if response.status_code == 200:
        response_200 = ListDeploymentsResponse200.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = ListDeploymentsResponse400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = ListDeploymentsResponse401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = ListDeploymentsResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = ListDeploymentsResponse404.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[
    Union[
        ListDeploymentsResponse200,
        ListDeploymentsResponse400,
        ListDeploymentsResponse401,
        ListDeploymentsResponse403,
        ListDeploymentsResponse404,
    ]
]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    workspace_id: UUID,
    *,
    client: Union[AuthenticatedClient, Client],
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Response[
    Union[
        ListDeploymentsResponse200,
        ListDeploymentsResponse400,
        ListDeploymentsResponse401,
        ListDeploymentsResponse403,
        ListDeploymentsResponse404,
    ]
]:
    """ListDeployments


    Gets all history of deployments for a workspace. Returns a paginated list of deployments ordered by
    version number descending.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        limit (Union[Unset, int]):  Default: 100.
        offset (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ListDeploymentsResponse200, ListDeploymentsResponse400, ListDeploymentsResponse401, ListDeploymentsResponse403, ListDeploymentsResponse404]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        limit=limit,
        offset=offset,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    workspace_id: UUID,
    *,
    client: Union[AuthenticatedClient, Client],
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Optional[
    Union[
        ListDeploymentsResponse200,
        ListDeploymentsResponse400,
        ListDeploymentsResponse401,
        ListDeploymentsResponse403,
        ListDeploymentsResponse404,
    ]
]:
    """ListDeployments


    Gets all history of deployments for a workspace. Returns a paginated list of deployments ordered by
    version number descending.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        limit (Union[Unset, int]):  Default: 100.
        offset (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ListDeploymentsResponse200, ListDeploymentsResponse400, ListDeploymentsResponse401, ListDeploymentsResponse403, ListDeploymentsResponse404]
    """

    return sync_detailed(
        workspace_id=workspace_id,
        client=client,
        limit=limit,
        offset=offset,
    ).parsed


async def asyncio_detailed(
    workspace_id: UUID,
    *,
    client: Union[AuthenticatedClient, Client],
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Response[
    Union[
        ListDeploymentsResponse200,
        ListDeploymentsResponse400,
        ListDeploymentsResponse401,
        ListDeploymentsResponse403,
        ListDeploymentsResponse404,
    ]
]:
    """ListDeployments


    Gets all history of deployments for a workspace. Returns a paginated list of deployments ordered by
    version number descending.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        limit (Union[Unset, int]):  Default: 100.
        offset (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ListDeploymentsResponse200, ListDeploymentsResponse400, ListDeploymentsResponse401, ListDeploymentsResponse403, ListDeploymentsResponse404]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        limit=limit,
        offset=offset,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    workspace_id: UUID,
    *,
    client: Union[AuthenticatedClient, Client],
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Optional[
    Union[
        ListDeploymentsResponse200,
        ListDeploymentsResponse400,
        ListDeploymentsResponse401,
        ListDeploymentsResponse403,
        ListDeploymentsResponse404,
    ]
]:
    """ListDeployments


    Gets all history of deployments for a workspace. Returns a paginated list of deployments ordered by
    version number descending.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        limit (Union[Unset, int]):  Default: 100.
        offset (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ListDeploymentsResponse200, ListDeploymentsResponse400, ListDeploymentsResponse401, ListDeploymentsResponse403, ListDeploymentsResponse404]
    """

    return (
        await asyncio_detailed(
            workspace_id=workspace_id,
            client=client,
            limit=limit,
            offset=offset,
        )
    ).parsed
