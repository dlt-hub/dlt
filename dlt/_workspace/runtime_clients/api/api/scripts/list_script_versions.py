from http import HTTPStatus
from typing import Any, Optional, Union, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.list_script_versions_response_200 import ListScriptVersionsResponse200
from ...models.list_script_versions_response_400 import ListScriptVersionsResponse400
from ...models.list_script_versions_response_401 import ListScriptVersionsResponse401
from ...models.list_script_versions_response_403 import ListScriptVersionsResponse403
from ...models.list_script_versions_response_404 import ListScriptVersionsResponse404
from ...types import UNSET, Response, Unset


def _get_kwargs(
    workspace_id: UUID,
    script_id_or_name: str,
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
        "url": "/v1/workspaces/{workspace_id}/scripts/{script_id_or_name}/versions".format(
            workspace_id=workspace_id,
            script_id_or_name=script_id_or_name,
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[
    Union[
        ListScriptVersionsResponse200,
        ListScriptVersionsResponse400,
        ListScriptVersionsResponse401,
        ListScriptVersionsResponse403,
        ListScriptVersionsResponse404,
    ]
]:
    if response.status_code == 200:
        response_200 = ListScriptVersionsResponse200.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = ListScriptVersionsResponse400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = ListScriptVersionsResponse401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = ListScriptVersionsResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = ListScriptVersionsResponse404.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[
    Union[
        ListScriptVersionsResponse200,
        ListScriptVersionsResponse400,
        ListScriptVersionsResponse401,
        ListScriptVersionsResponse403,
        ListScriptVersionsResponse404,
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
    script_id_or_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Response[
    Union[
        ListScriptVersionsResponse200,
        ListScriptVersionsResponse400,
        ListScriptVersionsResponse401,
        ListScriptVersionsResponse403,
        ListScriptVersionsResponse404,
    ]
]:
    """ListScriptVersions


    Gets the script version history for a script. Returns a paginated list of script versions ordered by
    version descending, latest first.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        script_id_or_name (str):
        limit (Union[Unset, int]):  Default: 100.
        offset (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ListScriptVersionsResponse200, ListScriptVersionsResponse400, ListScriptVersionsResponse401, ListScriptVersionsResponse403, ListScriptVersionsResponse404]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        script_id_or_name=script_id_or_name,
        limit=limit,
        offset=offset,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    workspace_id: UUID,
    script_id_or_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Optional[
    Union[
        ListScriptVersionsResponse200,
        ListScriptVersionsResponse400,
        ListScriptVersionsResponse401,
        ListScriptVersionsResponse403,
        ListScriptVersionsResponse404,
    ]
]:
    """ListScriptVersions


    Gets the script version history for a script. Returns a paginated list of script versions ordered by
    version descending, latest first.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        script_id_or_name (str):
        limit (Union[Unset, int]):  Default: 100.
        offset (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ListScriptVersionsResponse200, ListScriptVersionsResponse400, ListScriptVersionsResponse401, ListScriptVersionsResponse403, ListScriptVersionsResponse404]
    """

    return sync_detailed(
        workspace_id=workspace_id,
        script_id_or_name=script_id_or_name,
        client=client,
        limit=limit,
        offset=offset,
    ).parsed


async def asyncio_detailed(
    workspace_id: UUID,
    script_id_or_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Response[
    Union[
        ListScriptVersionsResponse200,
        ListScriptVersionsResponse400,
        ListScriptVersionsResponse401,
        ListScriptVersionsResponse403,
        ListScriptVersionsResponse404,
    ]
]:
    """ListScriptVersions


    Gets the script version history for a script. Returns a paginated list of script versions ordered by
    version descending, latest first.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        script_id_or_name (str):
        limit (Union[Unset, int]):  Default: 100.
        offset (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ListScriptVersionsResponse200, ListScriptVersionsResponse400, ListScriptVersionsResponse401, ListScriptVersionsResponse403, ListScriptVersionsResponse404]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        script_id_or_name=script_id_or_name,
        limit=limit,
        offset=offset,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    workspace_id: UUID,
    script_id_or_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Optional[
    Union[
        ListScriptVersionsResponse200,
        ListScriptVersionsResponse400,
        ListScriptVersionsResponse401,
        ListScriptVersionsResponse403,
        ListScriptVersionsResponse404,
    ]
]:
    """ListScriptVersions


    Gets the script version history for a script. Returns a paginated list of script versions ordered by
    version descending, latest first.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        script_id_or_name (str):
        limit (Union[Unset, int]):  Default: 100.
        offset (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ListScriptVersionsResponse200, ListScriptVersionsResponse400, ListScriptVersionsResponse401, ListScriptVersionsResponse403, ListScriptVersionsResponse404]
    """

    return (
        await asyncio_detailed(
            workspace_id=workspace_id,
            script_id_or_name=script_id_or_name,
            client=client,
            limit=limit,
            offset=offset,
        )
    ).parsed
