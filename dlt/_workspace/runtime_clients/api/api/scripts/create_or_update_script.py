# Python internals
from http import HTTPStatus
from typing import Any, Optional, Union
from uuid import UUID

# Other libraries
import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.create_or_update_script_response_400 import CreateOrUpdateScriptResponse400
from ...models.create_or_update_script_response_401 import CreateOrUpdateScriptResponse401
from ...models.create_or_update_script_response_403 import CreateOrUpdateScriptResponse403
from ...models.create_or_update_script_response_404 import CreateOrUpdateScriptResponse404
from ...models.create_script_request import CreateScriptRequest
from ...models.script_response import ScriptResponse
from ...types import Response


def _get_kwargs(
    workspace_id: UUID,
    *,
    body: CreateScriptRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/v1/workspaces/{workspace_id}/scripts".format(
            workspace_id=workspace_id,
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[
    Union[
        CreateOrUpdateScriptResponse400,
        CreateOrUpdateScriptResponse401,
        CreateOrUpdateScriptResponse403,
        CreateOrUpdateScriptResponse404,
        ScriptResponse,
    ]
]:
    if response.status_code == 201:
        response_201 = ScriptResponse.from_dict(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = CreateOrUpdateScriptResponse400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = CreateOrUpdateScriptResponse401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = CreateOrUpdateScriptResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = CreateOrUpdateScriptResponse404.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[
    Union[
        CreateOrUpdateScriptResponse400,
        CreateOrUpdateScriptResponse401,
        CreateOrUpdateScriptResponse403,
        CreateOrUpdateScriptResponse404,
        ScriptResponse,
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
    body: CreateScriptRequest,
) -> Response[
    Union[
        CreateOrUpdateScriptResponse400,
        CreateOrUpdateScriptResponse401,
        CreateOrUpdateScriptResponse403,
        CreateOrUpdateScriptResponse404,
        ScriptResponse,
    ]
]:
    """CreateOrUpdateScript


    Creates a new script for a workspace. A script name in a workspace must be unique. A script has
    versions the
    highest version is the active version. Scripts define entry points of a workspace, or more
    precisely, entry points for the
    currently active deployment. Scripts can also be deactivated, so that they are not run on schedule
    or accessible via public
    endpoints in the cases where they are notebooks.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        body (CreateScriptRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[CreateOrUpdateScriptResponse400, CreateOrUpdateScriptResponse401, CreateOrUpdateScriptResponse403, CreateOrUpdateScriptResponse404, ScriptResponse]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    workspace_id: UUID,
    *,
    client: Union[AuthenticatedClient, Client],
    body: CreateScriptRequest,
) -> Optional[
    Union[
        CreateOrUpdateScriptResponse400,
        CreateOrUpdateScriptResponse401,
        CreateOrUpdateScriptResponse403,
        CreateOrUpdateScriptResponse404,
        ScriptResponse,
    ]
]:
    """CreateOrUpdateScript


    Creates a new script for a workspace. A script name in a workspace must be unique. A script has
    versions the
    highest version is the active version. Scripts define entry points of a workspace, or more
    precisely, entry points for the
    currently active deployment. Scripts can also be deactivated, so that they are not run on schedule
    or accessible via public
    endpoints in the cases where they are notebooks.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        body (CreateScriptRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[CreateOrUpdateScriptResponse400, CreateOrUpdateScriptResponse401, CreateOrUpdateScriptResponse403, CreateOrUpdateScriptResponse404, ScriptResponse]
    """

    return sync_detailed(
        workspace_id=workspace_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    workspace_id: UUID,
    *,
    client: Union[AuthenticatedClient, Client],
    body: CreateScriptRequest,
) -> Response[
    Union[
        CreateOrUpdateScriptResponse400,
        CreateOrUpdateScriptResponse401,
        CreateOrUpdateScriptResponse403,
        CreateOrUpdateScriptResponse404,
        ScriptResponse,
    ]
]:
    """CreateOrUpdateScript


    Creates a new script for a workspace. A script name in a workspace must be unique. A script has
    versions the
    highest version is the active version. Scripts define entry points of a workspace, or more
    precisely, entry points for the
    currently active deployment. Scripts can also be deactivated, so that they are not run on schedule
    or accessible via public
    endpoints in the cases where they are notebooks.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        body (CreateScriptRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[CreateOrUpdateScriptResponse400, CreateOrUpdateScriptResponse401, CreateOrUpdateScriptResponse403, CreateOrUpdateScriptResponse404, ScriptResponse]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    workspace_id: UUID,
    *,
    client: Union[AuthenticatedClient, Client],
    body: CreateScriptRequest,
) -> Optional[
    Union[
        CreateOrUpdateScriptResponse400,
        CreateOrUpdateScriptResponse401,
        CreateOrUpdateScriptResponse403,
        CreateOrUpdateScriptResponse404,
        ScriptResponse,
    ]
]:
    """CreateOrUpdateScript


    Creates a new script for a workspace. A script name in a workspace must be unique. A script has
    versions the
    highest version is the active version. Scripts define entry points of a workspace, or more
    precisely, entry points for the
    currently active deployment. Scripts can also be deactivated, so that they are not run on schedule
    or accessible via public
    endpoints in the cases where they are notebooks.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        body (CreateScriptRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[CreateOrUpdateScriptResponse400, CreateOrUpdateScriptResponse401, CreateOrUpdateScriptResponse403, CreateOrUpdateScriptResponse404, ScriptResponse]
    """

    return (
        await asyncio_detailed(
            workspace_id=workspace_id,
            client=client,
            body=body,
        )
    ).parsed
