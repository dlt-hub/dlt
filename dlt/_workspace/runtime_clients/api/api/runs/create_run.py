from http import HTTPStatus
from typing import Any, cast
from uuid import UUID

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.create_run_request import CreateRunRequest
from ...models.error_response_400 import ErrorResponse400
from ...models.error_response_401 import ErrorResponse401
from ...models.error_response_403 import ErrorResponse403
from ...models.error_response_404 import ErrorResponse404
from ...models.run_response import RunResponse
from ...types import UNSET, Response


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
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | RunResponse | None:
    if response.status_code == 201:
        response_201 = RunResponse.from_dict(response.json())

        return response_201

    if response.status_code == 400:
        response_400 = ErrorResponse400.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = ErrorResponse401.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = ErrorResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = ErrorResponse404.from_dict(response.json())

        return response_404

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[
    ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | RunResponse
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
    client: AuthenticatedClient | Client,
    body: CreateRunRequest,
) -> Response[
    ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | RunResponse
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
        Response[ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | RunResponse]
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
    client: AuthenticatedClient | Client,
    body: CreateRunRequest,
) -> ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | RunResponse | None:
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
        ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | RunResponse
    """

    return sync_detailed(
        workspace_id=workspace_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    workspace_id: UUID,
    *,
    client: AuthenticatedClient | Client,
    body: CreateRunRequest,
) -> Response[
    ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | RunResponse
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
        Response[ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | RunResponse]
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
    client: AuthenticatedClient | Client,
    body: CreateRunRequest,
) -> ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | RunResponse | None:
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
        ErrorResponse400 | ErrorResponse401 | ErrorResponse403 | ErrorResponse404 | RunResponse
    """

    return (
        await asyncio_detailed(
            workspace_id=workspace_id,
            client=client,
            body=body,
        )
    ).parsed
