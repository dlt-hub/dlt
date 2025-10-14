from http import HTTPStatus
from typing import Any, Optional, Union, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.create_script_version_request import CreateScriptVersionRequest
from ...models.create_script_version_response_400 import CreateScriptVersionResponse400
from ...models.create_script_version_response_401 import CreateScriptVersionResponse401
from ...models.create_script_version_response_403 import CreateScriptVersionResponse403
from ...models.create_script_version_response_404 import CreateScriptVersionResponse404
from ...models.script_version_response import ScriptVersionResponse
from ...types import UNSET, Response


def _get_kwargs(
    workspace_id: UUID,
    script_id_or_name: str,
    *,
    body: CreateScriptVersionRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/v1/workspaces/{workspace_id}/scripts/{script_id_or_name}/versions".format(
            workspace_id=workspace_id,
            script_id_or_name=script_id_or_name,
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
        CreateScriptVersionResponse400,
        CreateScriptVersionResponse401,
        CreateScriptVersionResponse403,
        CreateScriptVersionResponse404,
        ScriptVersionResponse,
    ]
]:
    if response.status_code == 201:
        response_201 = ScriptVersionResponse.from_dict(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = CreateScriptVersionResponse400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = CreateScriptVersionResponse401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = CreateScriptVersionResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = CreateScriptVersionResponse404.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[
    Union[
        CreateScriptVersionResponse400,
        CreateScriptVersionResponse401,
        CreateScriptVersionResponse403,
        CreateScriptVersionResponse404,
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
    *,
    client: Union[AuthenticatedClient, Client],
    body: CreateScriptVersionRequest,
) -> Response[
    Union[
        CreateScriptVersionResponse400,
        CreateScriptVersionResponse401,
        CreateScriptVersionResponse403,
        CreateScriptVersionResponse404,
        ScriptVersionResponse,
    ]
]:
    """CreateScriptVersion


    Creates a new script version for a script. A script version has a version number, an entry point, a
    script type and a schedule.
    The profile attribute is optional but should be a valid profile name or ID if provided, if not
    provided, the default profile will be attached.
    The profile governs which configuration will be used if the script is executed.
    The schedule attribute is optional but should be a valid cron expression if provided.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        script_id_or_name (str):
        body (CreateScriptVersionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[CreateScriptVersionResponse400, CreateScriptVersionResponse401, CreateScriptVersionResponse403, CreateScriptVersionResponse404, ScriptVersionResponse]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        script_id_or_name=script_id_or_name,
        body=body,
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
    body: CreateScriptVersionRequest,
) -> Optional[
    Union[
        CreateScriptVersionResponse400,
        CreateScriptVersionResponse401,
        CreateScriptVersionResponse403,
        CreateScriptVersionResponse404,
        ScriptVersionResponse,
    ]
]:
    """CreateScriptVersion


    Creates a new script version for a script. A script version has a version number, an entry point, a
    script type and a schedule.
    The profile attribute is optional but should be a valid profile name or ID if provided, if not
    provided, the default profile will be attached.
    The profile governs which configuration will be used if the script is executed.
    The schedule attribute is optional but should be a valid cron expression if provided.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        script_id_or_name (str):
        body (CreateScriptVersionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[CreateScriptVersionResponse400, CreateScriptVersionResponse401, CreateScriptVersionResponse403, CreateScriptVersionResponse404, ScriptVersionResponse]
    """

    return sync_detailed(
        workspace_id=workspace_id,
        script_id_or_name=script_id_or_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    workspace_id: UUID,
    script_id_or_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    body: CreateScriptVersionRequest,
) -> Response[
    Union[
        CreateScriptVersionResponse400,
        CreateScriptVersionResponse401,
        CreateScriptVersionResponse403,
        CreateScriptVersionResponse404,
        ScriptVersionResponse,
    ]
]:
    """CreateScriptVersion


    Creates a new script version for a script. A script version has a version number, an entry point, a
    script type and a schedule.
    The profile attribute is optional but should be a valid profile name or ID if provided, if not
    provided, the default profile will be attached.
    The profile governs which configuration will be used if the script is executed.
    The schedule attribute is optional but should be a valid cron expression if provided.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        script_id_or_name (str):
        body (CreateScriptVersionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[CreateScriptVersionResponse400, CreateScriptVersionResponse401, CreateScriptVersionResponse403, CreateScriptVersionResponse404, ScriptVersionResponse]]
    """

    kwargs = _get_kwargs(
        workspace_id=workspace_id,
        script_id_or_name=script_id_or_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    workspace_id: UUID,
    script_id_or_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    body: CreateScriptVersionRequest,
) -> Optional[
    Union[
        CreateScriptVersionResponse400,
        CreateScriptVersionResponse401,
        CreateScriptVersionResponse403,
        CreateScriptVersionResponse404,
        ScriptVersionResponse,
    ]
]:
    """CreateScriptVersion


    Creates a new script version for a script. A script version has a version number, an entry point, a
    script type and a schedule.
    The profile attribute is optional but should be a valid profile name or ID if provided, if not
    provided, the default profile will be attached.
    The profile governs which configuration will be used if the script is executed.
    The schedule attribute is optional but should be a valid cron expression if provided.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        script_id_or_name (str):
        body (CreateScriptVersionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[CreateScriptVersionResponse400, CreateScriptVersionResponse401, CreateScriptVersionResponse403, CreateScriptVersionResponse404, ScriptVersionResponse]
    """

    return (
        await asyncio_detailed(
            workspace_id=workspace_id,
            script_id_or_name=script_id_or_name,
            client=client,
            body=body,
        )
    ).parsed
