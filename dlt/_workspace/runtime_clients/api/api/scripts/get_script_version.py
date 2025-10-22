from http import HTTPStatus
from typing import Any, Optional, Union, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.get_script_version_response_400 import GetScriptVersionResponse400
from ...models.get_script_version_response_401 import GetScriptVersionResponse401
from ...models.get_script_version_response_403 import GetScriptVersionResponse403
from ...models.get_script_version_response_404 import GetScriptVersionResponse404
from ...models.script_version_response import ScriptVersionResponse
from ...types import UNSET, Response


def _get_kwargs(
    workspace_id: UUID,
    script_id_or_name: str,
    script_version_id_or_version: Union[UUID, int],
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/v1/workspaces/{workspace_id}/scripts/{script_id_or_name}/versions/{script_version_id_or_version}".format(
            workspace_id=workspace_id,
            script_id_or_name=script_id_or_name,
            script_version_id_or_version=script_version_id_or_version,
        ),
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[
    Union[
        GetScriptVersionResponse400,
        GetScriptVersionResponse401,
        GetScriptVersionResponse403,
        GetScriptVersionResponse404,
        ScriptVersionResponse,
    ]
]:
    if response.status_code == 200:
        response_200 = ScriptVersionResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = GetScriptVersionResponse400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = GetScriptVersionResponse401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = GetScriptVersionResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = GetScriptVersionResponse404.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[
    Union[
        GetScriptVersionResponse400,
        GetScriptVersionResponse401,
        GetScriptVersionResponse403,
        GetScriptVersionResponse404,
        ScriptVersionResponse,
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
    script_version_id_or_version: Union[UUID, int],
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[
    Union[
        GetScriptVersionResponse400,
        GetScriptVersionResponse401,
        GetScriptVersionResponse403,
        GetScriptVersionResponse404,
        ScriptVersionResponse,
    ]
]:
    """GetScriptVersion


    Gets a historic version for a script, either by ID or by version number.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        script_id_or_name (str):
        script_version_id_or_version (Union[UUID, int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[GetScriptVersionResponse400, GetScriptVersionResponse401, GetScriptVersionResponse403, GetScriptVersionResponse404, ScriptVersionResponse]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        script_id_or_name=script_id_or_name,
        script_version_id_or_version=script_version_id_or_version,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    workspace_id: UUID,
    script_id_or_name: str,
    script_version_id_or_version: Union[UUID, int],
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[
    Union[
        GetScriptVersionResponse400,
        GetScriptVersionResponse401,
        GetScriptVersionResponse403,
        GetScriptVersionResponse404,
        ScriptVersionResponse,
    ]
]:
    """GetScriptVersion


    Gets a historic version for a script, either by ID or by version number.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        script_id_or_name (str):
        script_version_id_or_version (Union[UUID, int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[GetScriptVersionResponse400, GetScriptVersionResponse401, GetScriptVersionResponse403, GetScriptVersionResponse404, ScriptVersionResponse]
    """

    return sync_detailed(
        workspace_id=workspace_id,
        script_id_or_name=script_id_or_name,
        script_version_id_or_version=script_version_id_or_version,
        client=client,
    ).parsed


async def asyncio_detailed(
    workspace_id: UUID,
    script_id_or_name: str,
    script_version_id_or_version: Union[UUID, int],
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[
    Union[
        GetScriptVersionResponse400,
        GetScriptVersionResponse401,
        GetScriptVersionResponse403,
        GetScriptVersionResponse404,
        ScriptVersionResponse,
    ]
]:
    """GetScriptVersion


    Gets a historic version for a script, either by ID or by version number.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        script_id_or_name (str):
        script_version_id_or_version (Union[UUID, int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[GetScriptVersionResponse400, GetScriptVersionResponse401, GetScriptVersionResponse403, GetScriptVersionResponse404, ScriptVersionResponse]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        script_id_or_name=script_id_or_name,
        script_version_id_or_version=script_version_id_or_version,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    workspace_id: UUID,
    script_id_or_name: str,
    script_version_id_or_version: Union[UUID, int],
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[
    Union[
        GetScriptVersionResponse400,
        GetScriptVersionResponse401,
        GetScriptVersionResponse403,
        GetScriptVersionResponse404,
        ScriptVersionResponse,
    ]
]:
    """GetScriptVersion


    Gets a historic version for a script, either by ID or by version number.

    Requires READ permission on the organization level.

    Args:
        workspace_id (UUID):
        script_id_or_name (str):
        script_version_id_or_version (Union[UUID, int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[GetScriptVersionResponse400, GetScriptVersionResponse401, GetScriptVersionResponse403, GetScriptVersionResponse404, ScriptVersionResponse]
    """

    return (
        await asyncio_detailed(
            workspace_id=workspace_id,
            script_id_or_name=script_id_or_name,
            script_version_id_or_version=script_version_id_or_version,
            client=client,
        )
    ).parsed
