# Python internals
from http import HTTPStatus
from typing import Any, Optional, Union
from uuid import UUID

# Other libraries
import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.create_run_request import CreateRunRequest
from ...models.create_run_response_400 import CreateRunResponse400
from ...models.create_run_response_401 import CreateRunResponse401
from ...models.create_run_response_403 import CreateRunResponse403
from ...models.create_run_response_404 import CreateRunResponse404
from ...models.run_response import RunResponse
from ...types import Response


def _get_kwargs(
    workspace_id: UUID,
    *,
    body: CreateRunRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/v1/workspaces/{workspace_id}/runs/runs".format(
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
        CreateRunResponse400,
        CreateRunResponse401,
        CreateRunResponse403,
        CreateRunResponse404,
        RunResponse,
    ]
]:
    if response.status_code == 201:
        response_201 = RunResponse.from_dict(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = CreateRunResponse400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = CreateRunResponse401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = CreateRunResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = CreateRunResponse404.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[
    Union[
        CreateRunResponse400,
        CreateRunResponse401,
        CreateRunResponse403,
        CreateRunResponse404,
        RunResponse,
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
    body: CreateRunRequest,
) -> Response[
    Union[
        CreateRunResponse400,
        CreateRunResponse401,
        CreateRunResponse403,
        CreateRunResponse404,
        RunResponse,
    ]
]:
    """CreateRun


    Triggers a new run for a script in a workspace. The latest script version will be used. The profile
    associated with the script version will be used, of which
    the latest profile_version will be used. You may specify a specific profile to use.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        body (CreateRunRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[CreateRunResponse400, CreateRunResponse401, CreateRunResponse403, CreateRunResponse404, RunResponse]]
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
    body: CreateRunRequest,
) -> Optional[
    Union[
        CreateRunResponse400,
        CreateRunResponse401,
        CreateRunResponse403,
        CreateRunResponse404,
        RunResponse,
    ]
]:
    """CreateRun


    Triggers a new run for a script in a workspace. The latest script version will be used. The profile
    associated with the script version will be used, of which
    the latest profile_version will be used. You may specify a specific profile to use.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        body (CreateRunRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[CreateRunResponse400, CreateRunResponse401, CreateRunResponse403, CreateRunResponse404, RunResponse]
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
    body: CreateRunRequest,
) -> Response[
    Union[
        CreateRunResponse400,
        CreateRunResponse401,
        CreateRunResponse403,
        CreateRunResponse404,
        RunResponse,
    ]
]:
    """CreateRun


    Triggers a new run for a script in a workspace. The latest script version will be used. The profile
    associated with the script version will be used, of which
    the latest profile_version will be used. You may specify a specific profile to use.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        body (CreateRunRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[CreateRunResponse400, CreateRunResponse401, CreateRunResponse403, CreateRunResponse404, RunResponse]]
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
    body: CreateRunRequest,
) -> Optional[
    Union[
        CreateRunResponse400,
        CreateRunResponse401,
        CreateRunResponse403,
        CreateRunResponse404,
        RunResponse,
    ]
]:
    """CreateRun


    Triggers a new run for a script in a workspace. The latest script version will be used. The profile
    associated with the script version will be used, of which
    the latest profile_version will be used. You may specify a specific profile to use.

    Requires WRITE permission on the organization level.

    Args:
        workspace_id (UUID):
        body (CreateRunRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[CreateRunResponse400, CreateRunResponse401, CreateRunResponse403, CreateRunResponse404, RunResponse]
    """

    return (
        await asyncio_detailed(
            workspace_id=workspace_id,
            client=client,
            body=body,
        )
    ).parsed
