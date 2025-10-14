from http import HTTPStatus
from typing import Any, Optional, Union, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.list_profiles_response_200 import ListProfilesResponse200
from ...models.list_profiles_response_400 import ListProfilesResponse400
from ...models.list_profiles_response_401 import ListProfilesResponse401
from ...models.list_profiles_response_403 import ListProfilesResponse403
from ...models.list_profiles_response_404 import ListProfilesResponse404
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
        "url": "/v1/workspaces/{workspace_id}/profiles".format(
            workspace_id=workspace_id,
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[
    Union[
        ListProfilesResponse200,
        ListProfilesResponse400,
        ListProfilesResponse401,
        ListProfilesResponse403,
        ListProfilesResponse404,
    ]
]:
    if response.status_code == 200:
        response_200 = ListProfilesResponse200.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = ListProfilesResponse400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = ListProfilesResponse401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = ListProfilesResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = ListProfilesResponse404.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[
    Union[
        ListProfilesResponse200,
        ListProfilesResponse400,
        ListProfilesResponse401,
        ListProfilesResponse403,
        ListProfilesResponse404,
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
        ListProfilesResponse200,
        ListProfilesResponse400,
        ListProfilesResponse401,
        ListProfilesResponse403,
        ListProfilesResponse404,
    ]
]:
    """ListProfiles


    Gets all profiles for a workspace. Returns a paginated list of profiles ordered by name ascending.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        limit (Union[Unset, int]):  Default: 100.
        offset (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ListProfilesResponse200, ListProfilesResponse400, ListProfilesResponse401, ListProfilesResponse403, ListProfilesResponse404]]
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
        ListProfilesResponse200,
        ListProfilesResponse400,
        ListProfilesResponse401,
        ListProfilesResponse403,
        ListProfilesResponse404,
    ]
]:
    """ListProfiles


    Gets all profiles for a workspace. Returns a paginated list of profiles ordered by name ascending.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        limit (Union[Unset, int]):  Default: 100.
        offset (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ListProfilesResponse200, ListProfilesResponse400, ListProfilesResponse401, ListProfilesResponse403, ListProfilesResponse404]
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
        ListProfilesResponse200,
        ListProfilesResponse400,
        ListProfilesResponse401,
        ListProfilesResponse403,
        ListProfilesResponse404,
    ]
]:
    """ListProfiles


    Gets all profiles for a workspace. Returns a paginated list of profiles ordered by name ascending.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        limit (Union[Unset, int]):  Default: 100.
        offset (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ListProfilesResponse200, ListProfilesResponse400, ListProfilesResponse401, ListProfilesResponse403, ListProfilesResponse404]]
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
        ListProfilesResponse200,
        ListProfilesResponse400,
        ListProfilesResponse401,
        ListProfilesResponse403,
        ListProfilesResponse404,
    ]
]:
    """ListProfiles


    Gets all profiles for a workspace. Returns a paginated list of profiles ordered by name ascending.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        limit (Union[Unset, int]):  Default: 100.
        offset (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ListProfilesResponse200, ListProfilesResponse400, ListProfilesResponse401, ListProfilesResponse403, ListProfilesResponse404]
    """

    return (
        await asyncio_detailed(
            workspace_id=workspace_id,
            client=client,
            limit=limit,
            offset=offset,
        )
    ).parsed
